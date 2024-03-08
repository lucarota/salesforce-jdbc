package com.ascendix.jdbc.salesforce.delegates;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.metadata.Column;
import com.ascendix.jdbc.salesforce.metadata.Table;
import com.ascendix.jdbc.salesforce.statement.FieldDef;
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

public class PartnerService {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

    private final PartnerConnection partnerConnection;

    private DescribeGlobalResult schemaCache;
    private final Map<String, DescribeSObjectResult> sObjectsCache = new ConcurrentHashMap<>();

    public PartnerService(PartnerConnection partnerConnection) {
        this.partnerConnection = partnerConnection;
        try {
            this.schemaCache = getDescribeGlobal();
        } catch (ConnectionException e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    public List<Table> getTables() throws ConnectionException {
        logger.finest("[PartnerService] getTables IMPLEMENTED ");
        Map<String, DescribeSObjectResult> sObjects = getSObjectsDescription();
        List<Table> tables = sObjects.values().stream()
            .map(this::convertToTable)
            .collect(Collectors.toList());
        logger.info("[PartnerService] getTables tables count=" + tables.size());
        return tables;
    }

    public DescribeSObjectResult describeSObject(String sObjectType) {
        try {
            final String obj = StringUtils.toRootLowerCase(sObjectType);
            if (!sObjectsCache.containsKey(obj)) {
                DescribeSObjectResult description = partnerConnection.describeSObject(obj);
                sObjectsCache.put(obj, description);
                return description;
            } else {
                return sObjectsCache.get(obj);
            }
        } catch (ConnectionException e) {
            throw new RuntimeException(e);
        }
    }

    public void cleanupGlobalCache() throws ConnectionException {
        this.schemaCache = getDescribeGlobal();
        this.sObjectsCache.clear();
    }

    private Table convertToTable(DescribeSObjectResult so) {
        logger.finest("[PartnerService] convertToTable " + so.getName());
        List<Field> fields = Arrays.asList(so.getFields());
        List<Column> columns = fields.stream()
            .map(this::convertToColumn)
            .collect(Collectors.toList());
        return new Table(so.getName(), null, columns, so.isQueryable());
    }

    private Column convertToColumn(Field field) {
        try {
            Column column = new Column(field.getName(), getType(field));
            column.setNillable(field.isNillable());
            column.setCalculated(field.isCalculated() || field.isAutoNumber());
            column.setLength(field.getLength());
            String[] referenceTos = field.getReferenceTo();
            if (referenceTos != null) {
                for (String referenceTo : referenceTos) {
                    if (getSObjectTypes().contains(referenceTo)) {
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

    private synchronized DescribeGlobalResult getDescribeGlobal() throws ConnectionException {
        if (this.schemaCache == null) {
            this.schemaCache = partnerConnection.describeGlobal();
        }

        return this.schemaCache;
    }

    private List<String> getSObjectTypes() throws ConnectionException {
        final Map<String, DescribeSObjectResult> sObjects = getSObjectsDescription();
        List<String> sObjectTypes = sObjects.values().stream()
                .map(DescribeSObjectResult::getName)
                .collect(Collectors.toList());
        logger.info("[PartnerService] getSObjectTypes count=" + sObjectTypes.size());
        return sObjectTypes;
    }

    private Map<String, DescribeSObjectResult> getSObjectsDescription() throws ConnectionException {
        if (this.sObjectsCache.isEmpty()) {
            logger.finest("Load all SObjects");
            Map<String, DescribeSObjectResult> cache;
            DescribeGlobalResult describeGlobals = partnerConnection.describeGlobal();
            List<String> tableNames = Arrays.stream(describeGlobals.getSobjects())
                .map(DescribeGlobalSObjectResult::getName)
                .collect(Collectors.toList());
            List<List<String>> tableNamesBatched = toBatches(tableNames, 100);
            cache = tableNamesBatched.stream()
                .flatMap(batch -> describeSObjects(batch).stream())
                .collect(Collectors.toMap(DescribeSObjectResult::getName, Function.identity()));
            this.sObjectsCache.putAll(cache);
        }
        return this.sObjectsCache;
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

    private List<List> extractQueryResultData(QueryResult qr) {
        List<XmlObject> rows = Arrays.asList(qr.getRecords());
        // extract the root entity name
        Object rootEntityName = rows.stream()
            .filter(xmlo -> "type".equals(xmlo.getName().getLocalPart()))
            .findFirst()
            .map(XmlObject::getValue)
            .orElse(null);
        String parentName = null;
        return removeServiceInfo(rows, parentName, rootEntityName == null ? null : (String) rootEntityName);
    }

    public List<List> query(String soql, List<FieldDef> expectedSchema) throws ConnectionException {
        logger.finest("[PartnerService] query " + soql);
        List<List> resultRows = Collections.synchronizedList(new LinkedList<>());
        QueryResult queryResult = null;
        do {
            queryResult = queryResult == null ? partnerConnection.query(soql)
                : partnerConnection.queryMore(queryResult.getQueryLocator());

            resultRows.addAll(extractQueryResultData(queryResult));
        } while (!queryResult.isDone());

        return PartnerResultToCartesianTable.expand(resultRows, expectedSchema);
    }

    public Map.Entry<List<List>, String> queryStart(String soql, List<FieldDef> expectedSchema)
        throws ConnectionException {
        logger.finest("[PartnerService] queryStart " + soql);
        QueryResult queryResult = partnerConnection.query(soql);
        String queryLocator = queryResult.isDone() ? null : queryResult.getQueryLocator();
        return new AbstractMap.SimpleEntry<>(Collections.unmodifiableList(extractQueryResultData(queryResult)),
            queryLocator
        );
    }

    public Map.Entry<List<List>, String> queryMore(String queryLocator, List<FieldDef> expectedSchema)
        throws ConnectionException {
        logger.finest("[PartnerService] queryMore " + queryLocator);
        QueryResult queryResult = partnerConnection.queryMore(queryLocator);
        queryLocator = queryResult.isDone() ? null : queryResult.getQueryLocator();
        return new AbstractMap.SimpleEntry<>(Collections.unmodifiableList(extractQueryResultData(queryResult)),
            queryLocator
        );
    }

    private List<List> removeServiceInfo(List<XmlObject> rows, String parentName, String rootEntityName) {
        return rows.stream()
            .filter(this::isDataObjectType)
            .map(row -> removeServiceInfo(row, parentName, rootEntityName))
            .collect(Collectors.toList());
    }

    private List<ForceResultField> removeServiceInfo(XmlObject row, String parentName, String rootEntityName) {
        return IteratorUtils.toList(row.getChildren()).stream()
            .filter(this::isDataObjectType)
            .skip(1) // Removes duplicate Id from SF Partner API response
            // (https://developer.salesforce.com/forums/?id=906F00000008kciIAA)
            .flatMap(field -> translateField(field, parentName, rootEntityName))
            .collect(Collectors.toList());
    }

    private Stream<ForceResultField> translateField(XmlObject field, String parentName, String rootEntityName) {
        Stream.Builder outStream = Stream.builder();

        String fieldType = field.getXmlType() != null ? field.getXmlType().getLocalPart() : null;
        if ("sObject".equalsIgnoreCase(fieldType)) {
            List<ForceResultField> childFields = removeServiceInfo(field,
                field.getName().getLocalPart(),
                rootEntityName);
            childFields.forEach(outStream::add);
        } else {
            if (isNestedResultset(field)) {
                outStream.add(removeServiceInfo(IteratorUtils.toList(field.getChildren()),
                    field.getName().getLocalPart(),
                    rootEntityName));
            } else {
                outStream.add(toForceResultField(field, parentName, rootEntityName));
            }
        }
        return outStream.build();
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

    private final static List<String> SOAP_RESPONSE_SERVICE_OBJECT_TYPES = Arrays.asList("type", "done", "queryLocator",
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
            SObject record = records[i] = new SObject();
            record.setType(entityName);
            for (Map.Entry<String, Object> field : recordDef.entrySet()) {
                record.setField(field.getKey(), field.getValue());
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
            SObject record = records[i] = new SObject();
            record.setType(entityName);
            for (Map.Entry<String, Object> field : recordDef.entrySet()) {
                record.setField(field.getKey(), field.getValue());
            }
        }
        // Make a create call and pass it the array of sObjects
        return partnerConnection.update(records);
    }

    public DeleteResult[] deleteRecords(Collection<String> recordsIds) throws ConnectionException {
        return partnerConnection.delete(recordsIds.toArray(new String[]{}));
    }
}
