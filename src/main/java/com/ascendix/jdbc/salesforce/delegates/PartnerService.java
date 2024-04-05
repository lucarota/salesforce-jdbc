package com.ascendix.jdbc.salesforce.delegates;

import com.ascendix.jdbc.salesforce.metadata.Column;
import com.ascendix.jdbc.salesforce.metadata.Table;
import com.ascendix.jdbc.salesforce.utils.FieldDefTree;
import com.ascendix.jdbc.salesforce.utils.IteratorUtils;
import com.ascendix.jdbc.salesforce.utils.TreeNode;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.DescribeGlobalResult;
import com.sforce.soap.partner.DescribeGlobalSObjectResult;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.bind.XmlObject;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PartnerService {

    private final PartnerConnection partnerConnection;

    private static DescribeGlobalResult schemaCache;
    private static final Map<String, DescribeSObjectResult> sObjectsCache = new ConcurrentSkipListMap<>(
            java.lang.String.CASE_INSENSITIVE_ORDER);

    public PartnerService(PartnerConnection partnerConnection) {
        this.partnerConnection = partnerConnection;
    }

    public List<Table> getTables() throws ConnectionException {
        log.trace("[PartnerService] getTables IMPLEMENTED");
        Map<String, DescribeSObjectResult> sObjects = getSObjectsDescription();
        List<Table> tables = sObjects.values().stream()
            .map(this::convertToTable)
            .toList();
        log.info("[PartnerService] getTables tables count={}", tables.size());
        return tables;
    }

    public List<Table> getTables(Pattern pattern) throws ConnectionException {
        log.trace("[PartnerService] getTables IMPLEMENTED");
        Map<String, DescribeSObjectResult> sObjects = getSObjectsDescription();
        List<Table> tables = sObjects.values().stream()
            .filter(o -> pattern.matcher(o.getName()).find())
            .map(this::convertToTable)
            .toList();
        log.info("[PartnerService] getTables tables count={}", tables.size());
        return tables;
    }

    public DescribeSObjectResult describeSObject(String sObjectType) {
        try {
            if (!sObjectsCache.containsKey(sObjectType)) {
                DescribeSObjectResult description = partnerConnection.describeSObject(sObjectType);
                sObjectsCache.put(sObjectType, description);
                return description;
            } else {
                return sObjectsCache.get(sObjectType);
            }
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanupGlobalCache() throws ConnectionException {
        resetSchemaCache();
        sObjectsCache.clear();
    }

    private Table convertToTable(DescribeSObjectResult so) {
        log.trace("[PartnerService] convertToTable {}", so.getName());
        List<Field> fields = Arrays.asList(so.getFields());
        List<Column> columns = fields.stream()
            .map(this::convertToColumn)
            .toList();
        return new Table(so.getName(), null, columns, so.isQueryable());
    }

    private Column convertToColumn(Field field) {
        try {
            Column column = new Column(field.getName(), getType(field));
            column.setNillable(field.isNillable());
            column.setCalculated(field.isCalculated() || field.isAutoNumber());
            column.setLength(field.getLength());
            String[] referenceTos = field.getReferenceTo();
            List<String> sObjectTypes = getSObjectTypes();
            if (referenceTos != null) {
                for (String referenceTo : referenceTos) {
                    if (sObjectTypes.contains(referenceTo)) {
                        column.setReferencedTable(referenceTo);
                        column.setReferencedColumn("Id");
                    }
                }
            }
            return column;
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    private String getType(Field field) {
        String s = field.getType().toString();
        if (s.startsWith("_")) {
            s = s.substring("_".length());
        }
        return s.equalsIgnoreCase("double") ? "decimal" : s;
    }

    private synchronized void resetSchemaCache() throws ConnectionException {
       schemaCache = partnerConnection.describeGlobal();
    }

    private synchronized DescribeGlobalResult getDescribeGlobal() throws ConnectionException {
        if (schemaCache == null) {
            schemaCache = partnerConnection.describeGlobal();
        }

        return schemaCache;
    }

    private List<String> getSObjectTypes() throws ConnectionException {
        final Map<String, DescribeSObjectResult> sObjects = getSObjectsDescription();
        return sObjects.values().stream()
                .map(DescribeSObjectResult::getName)
                .toList();
    }

    private Map<String, DescribeSObjectResult> getSObjectsDescription() throws ConnectionException {
        if (sObjectsCache.isEmpty() || sObjectsCache.size() < getDescribeGlobal().getSobjects().length) {
            log.trace("Load all SObjects");
            Map<String, DescribeSObjectResult> cache;
            DescribeGlobalResult describeGlobals = getDescribeGlobal();
            List<String> tableNames = Arrays.stream(describeGlobals.getSobjects())
                .map(DescribeGlobalSObjectResult::getName)
                .toList();
            List<List<String>> tableNamesBatched = toBatches(tableNames, 100);
            cache = tableNamesBatched.stream()
                .flatMap(batch -> describeSObjects(batch).stream())
                .collect(Collectors.toMap(DescribeSObjectResult::getName, Function.identity()));
            sObjectsCache.putAll(cache);
        }
        return sObjectsCache;
    }

    private List<DescribeSObjectResult> describeSObjects(List<String> batch) {
        DescribeSObjectResult[] result;
        try {
            result = partnerConnection.describeSObjects(batch.toArray(new String[0]));
            return Arrays.asList(result);
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> List<List<T>> toBatches(List<T> objects, int batchSize) {
        List<List<T>> result = new ArrayList<>();
        for (int fromIndex = 0; fromIndex < objects.size(); fromIndex += batchSize) {
            int toIndex = Math.min(fromIndex + batchSize, objects.size());
            result.add(objects.subList(fromIndex, toIndex));
        }
        return result;
    }

    private List<TreeNode<ForceResultField>> extractQueryResultData(QueryResult qr) {
        List<XmlObject> rows = Arrays.asList(qr.getRecords());
        // extract the root entity name
        Object rootEntityName = rows.stream()
            .filter(xmlo -> "type".equals(xmlo.getName().getLocalPart()))
            .findFirst()
            .map(XmlObject::getValue)
            .orElse(null);
        return removeServiceInfo(rows, null, rootEntityName == null ? null : (String) rootEntityName);
    }

    public List<List<ForceResultField>> query(String soql, FieldDefTree expectedSchema) throws ConnectionException {
        log.trace("[PartnerService] query {}", soql);
        List<TreeNode<ForceResultField>> resultRows = Collections.synchronizedList(new LinkedList<>());
        QueryResult queryResult = null;
        do {
            queryResult = queryResult == null ? partnerConnection.query(soql)
                : partnerConnection.queryMore(queryResult.getQueryLocator());

            partnerConnection.setSessionHeader(partnerConnection.getConfig().getSessionId());
            resultRows.addAll(extractQueryResultData(queryResult));
        } while (!queryResult.isDone());

        return FieldDefTree.expand(resultRows, expectedSchema);
    }

    public Map.Entry<List<List<ForceResultField>>, String> queryStart(String soql, FieldDefTree expectedSchema)
        throws ConnectionException {
        log.trace("[PartnerService] queryStart {}", soql);
        QueryResult queryResult = partnerConnection.query(soql);
        String queryLocator = queryResult.isDone() ? null : queryResult.getQueryLocator();
        List<TreeNode<ForceResultField>> resultRows = extractQueryResultData(queryResult);
        return new AbstractMap.SimpleEntry<>(FieldDefTree.expand(resultRows, expectedSchema),
            queryLocator
        );
    }

    public Map.Entry<List<List<ForceResultField>>, String> queryMore(String queryLocator, FieldDefTree expectedSchema)
        throws ConnectionException {
        log.trace("[PartnerService] queryMore {}", queryLocator);
        QueryResult queryResult = partnerConnection.queryMore(queryLocator);
        queryLocator = queryResult.isDone() ? null : queryResult.getQueryLocator();
        List<TreeNode<ForceResultField>> resultRows = extractQueryResultData(queryResult);
        return new AbstractMap.SimpleEntry<>(FieldDefTree.expand(resultRows, expectedSchema),
            queryLocator
        );
    }

    private List<TreeNode<ForceResultField>> removeServiceInfo(List<XmlObject> rows, String parentName, String rootEntityName) {
        return rows.stream()
            .filter(this::isDataObjectType)
            .map(row -> removeServiceInfo(row, parentName, rootEntityName))
            .collect(Collectors.toList());
    }

    private TreeNode<ForceResultField> removeServiceInfo(XmlObject row, String parentName, String rootEntityName) {
        Iterator<XmlObject> children = row.getChildren();
        TreeNode<ForceResultField> root = new TreeNode<>();
        boolean skipFirst = true;
        while (children.hasNext()) {
            XmlObject field = children.next();
            if (!isDataObjectType(field)) {
                continue;
            }
            if (skipFirst) {
                // Removes duplicate Id from SF Partner API response
                // (https://developer.salesforce.com/forums/?id=906F00000008kciIAA)
                skipFirst = false;
                continue;
            }
            root.addTreeNode(translateField(field, parentName, rootEntityName));
        }
        return root;
    }

    private TreeNode<ForceResultField> translateField(XmlObject field, String parentName, String rootEntityName) {
        String fieldType = field.getXmlType() != null ? field.getXmlType().getLocalPart() : null;
        if ("sObject".equalsIgnoreCase(fieldType)) {
            TreeNode<ForceResultField> node = new TreeNode<>();
            TreeNode<ForceResultField> childFields = removeServiceInfo(field,
                field.getName().getLocalPart(),
                rootEntityName);
            childFields.getChildren().forEach(node::addTreeNode);
            return node;
        } else if (isNestedResultset(field)) {
            List<TreeNode<ForceResultField>> subNodes = removeServiceInfo(
                    IteratorUtils.toList(field.getChildren()), field.getName().getLocalPart(), rootEntityName);
            return new TreeNode<>(subNodes);
        } else {
            return new TreeNode<>(toForceResultField(field, parentName, rootEntityName));
        }
    }

    private ForceResultField toForceResultField(XmlObject field, String parentName, String rootEntityName) {
        String fieldType = field.getXmlType() != null ? field.getXmlType().getLocalPart() : null;
        if ("sObject".equalsIgnoreCase(fieldType)) {
            List<XmlObject> children = new ArrayList<>();
            field.getChildren().forEachRemaining(children::add);
            field = children.get(2);
        }
        String name = field.getName().getLocalPart();
        if (parentName != null && (rootEntityName == null || !rootEntityName.equals(parentName))) {
            name = parentName + "." + name;
        }
        Object value = field.getValue();
        return new ForceResultField(null, fieldType, name, value);
    }

    private boolean isNestedResultset(XmlObject object) {
        return object.getXmlType() != null && "QueryResult".equals(object.getXmlType().getLocalPart());
    }

    private static final List<String> SOAP_RESPONSE_SERVICE_OBJECT_TYPES = Arrays.asList("type", "done", "queryLocator",
        "size");

    private boolean isDataObjectType(XmlObject obj) {
        return !SOAP_RESPONSE_SERVICE_OBJECT_TYPES.contains(obj.getName().getLocalPart());
    }

    public SaveResult[] createRecords(String entityName, List<Map<String, Object>> recordsDefinitions)
        throws ConnectionException {
        // Create a new sObject of type Contact
        // and fill out its fields.

        SObject[] records = new SObject[recordsDefinitions.size()];

        for (int i = 0; i < recordsDefinitions.size(); i++) {
            Map<String, Object> recordDef = recordsDefinitions.get(i);
            SObject rec = records[i] = new SObject();
            rec.setType(entityName);
            for (Map.Entry<String, Object> field : recordDef.entrySet()) {
                rec.setField(field.getKey(), field.getValue());
            }
        }
        // Make a create call and pass it the array of sObjects
        return partnerConnection.create(records);
    }

    public SaveResult[] saveRecords(String entityName, List<Map<String, Object>> recordsDefinitions)
        throws ConnectionException {
        // Create a new sObject of type Contact
        // and fill out its fields.

        SObject[] records = new SObject[recordsDefinitions.size()];

        for (int i = 0; i < recordsDefinitions.size(); i++) {
            Map<String, Object> recordDef = recordsDefinitions.get(i);
            SObject rec = records[i] = new SObject();
            rec.setType(entityName);
            for (Map.Entry<String, Object> field : recordDef.entrySet()) {
                rec.setField(field.getKey(), field.getValue());
            }
        }
        // Make a create call and pass it the array of sObjects
        return partnerConnection.update(records);
    }

    public DeleteResult[] deleteRecords(Collection<String> recordsIds) throws ConnectionException {
        return partnerConnection.delete(recordsIds.toArray(new String[]{}));
    }
}
