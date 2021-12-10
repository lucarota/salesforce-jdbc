package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import org.mule.tools.soql.SOQLDataBaseVisitor;
import org.mule.tools.soql.SOQLParserHelper;
import org.mule.tools.soql.exception.SOQLParsingException;
import org.mule.tools.soql.query.SOQLQuery;
import org.mule.tools.soql.query.SOQLSubQuery;
import org.mule.tools.soql.query.clause.FromClause;
import org.mule.tools.soql.query.from.ObjectSpec;
import org.mule.tools.soql.query.select.FieldSpec;
import org.mule.tools.soql.query.select.FunctionCallSpec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.ascendix.jdbc.salesforce.statement.processor.InsertQueryProcessor.SF_JDBC_DRIVER_NAME;

public class SoqlQueryAnalyzer {

    private static final Logger logger = Logger.getLogger(SF_JDBC_DRIVER_NAME);
    private String soql;
    private Function<String, DescribeSObjectResult> objectDescriptor;
    private Map<String, DescribeSObjectResult> describedObjectsCache;
    private SOQLQuery queryData;
    private boolean expandedStarSyntaxForFields = false;
    private List fieldDefinitions;


    public SoqlQueryAnalyzer(String soql, Function<String, DescribeSObjectResult> objectDescriptor) {
        this(soql, objectDescriptor, new HashMap<>());
    }

    public SoqlQueryAnalyzer(String soql, Function<String, DescribeSObjectResult> objectDescriptor, Map<String, DescribeSObjectResult> describedObjectsCache) {
        this.soql = soql;
        this.objectDescriptor = objectDescriptor;
        this.describedObjectsCache = describedObjectsCache;
        // to parse the query and process the expansion if needed
        getQueryData();
    }

    public String getSoqlQuery() {
        return this.soql;
    }

    private class SelectSpecVisitor extends SOQLDataBaseVisitor<Void> {

        private final String rootEntityName;

        public SelectSpecVisitor(String rootEntityName) {
            this.rootEntityName = rootEntityName;
        }

        @Override
        public Void visitFieldSpec(FieldSpec fieldSpec) {
            String name = fieldSpec.getFieldName();

            String alias = fieldSpec.getAlias() != null ? fieldSpec.getAlias() : name;
            // If Object Name specified - verify it is not the same as SOQL root entity
            String objectPrefix = fieldSpec.getObjectPrefixNames().size() > 0 ? fieldSpec.getObjectPrefixNames().get(0) : null;
            if (fieldSpec.getAlias() == null && objectPrefix != null && !objectPrefix.equals(rootEntityName)) {
                alias = objectPrefix + "." + name;
            }
            List<String> prefixNames = new ArrayList<>(fieldSpec.getObjectPrefixNames());
            FieldDef result = createFieldDef(name, alias, prefixNames);
            fieldDefinitions.add(result);
            return null;
        }

        private FieldDef createFieldDef(String name, String alias, List<String> prefixNames) {
            List<String> fieldPrefixes = new ArrayList<>(prefixNames);
            String fromObject = getFromObjectName();
            if (!fieldPrefixes.isEmpty() && fieldPrefixes.get(0).equalsIgnoreCase(fromObject)) {
                fieldPrefixes.remove(0);
            }
            while (!fieldPrefixes.isEmpty()) {
                String referenceName = fieldPrefixes.get(0);
                Field reference = findField(referenceName, describeObject(fromObject), fld -> fld.getRelationshipName());
                fromObject = reference.getReferenceTo()[0];
                fieldPrefixes.remove(0);
            }
            String type = findField(name, describeObject(fromObject), fld -> fld.getName()).getType().name();
            FieldDef result = new FieldDef(name, alias, type);
            return result;
        }

        private final List<String> FUNCTIONS_HAS_INT_RESULT = Arrays.asList("COUNT", "COUNT_DISTINCT", "CALENDAR_MONTH",
                "CALENDAR_QUARTER", "CALENDAR_YEAR", "DAY_IN_MONTH", "DAY_IN_WEEK", "DAY_IN_YEAR", "DAY_ONLY", "FISCAL_MONTH",
                "FISCAL_QUARTER", "FISCAL_YEAR", "HOUR_IN_DAY", "WEEK_IN_MONTH", "WEEK_IN_YEAR");

        @Override
        public Void visitFunctionCallSpec(FunctionCallSpec functionCallSpec) {
            String alias = functionCallSpec.getAlias() != null ? functionCallSpec.getAlias() : functionCallSpec.getFunctionName();
            if (FUNCTIONS_HAS_INT_RESULT.contains(functionCallSpec.getFunctionName().toUpperCase())) {
                fieldDefinitions.add(new FieldDef(alias, alias, "int"));
            } else {
                org.mule.tools.soql.query.data.Field param = (org.mule.tools.soql.query.data.Field) functionCallSpec.getFunctionParameters().get(0);
                FieldDef result = createFieldDef(param.getFieldName(), alias, param.getObjectPrefixNames());
                fieldDefinitions.add(result);
            }
            return null;
        }

        @Override
        public Void visitSOQLSubQuery(SOQLSubQuery soqlSubQuery) {
            String subquerySoql = soqlSubQuery.toSOQLText().replaceAll("\\A\\s*\\(|\\)\\s*$", "");
            SOQLQuery subquery = SOQLParserHelper.createSOQLData(subquerySoql);
            String relationshipName = subquery.getFromClause().getMainObjectSpec().getObjectName();
            ChildRelationship relatedFrom = Arrays.stream(describeObject(getFromObjectName()).getChildRelationships())
                    .filter(rel -> relationshipName.equalsIgnoreCase(rel.getRelationshipName()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unresolved relationship in subquery \"" + subquerySoql + "\""));
            String fromObject = relatedFrom.getChildSObject();
            subquery.setFromClause(new FromClause(new ObjectSpec(fromObject, null)));

            SoqlQueryAnalyzer subqueryAnalyzer = new SoqlQueryAnalyzer(subquery.toSOQLText(), objectDescriptor, describedObjectsCache);
            fieldDefinitions.add(new ArrayList(subqueryAnalyzer.getFieldDefinitions()));
            return null;
        }

    }

    public boolean isExpandedStarSyntaxForFields() {
        return expandedStarSyntaxForFields;
    }

    public List getFieldDefinitions() {
        if (fieldDefinitions == null) {
            fieldDefinitions = new ArrayList<>();
            String rootEntityName = getQueryData().getFromClause().getMainObjectSpec().getObjectName();
            SelectSpecVisitor visitor = new SelectSpecVisitor(rootEntityName);
            getQueryData().getSelectSpecs()
                    .forEach(spec -> spec.accept(visitor));
        }
        return fieldDefinitions;
    }

    private Field findField(String name, DescribeSObjectResult objectDesc, Function<Field, String> nameFetcher) {
        return Arrays.stream(objectDesc.getFields())
                .filter(field -> name.equalsIgnoreCase(nameFetcher.apply(field)))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown field name \"" + name + "\" in object \"" + objectDesc.getName() + "\""));
    }

    private DescribeSObjectResult describeObject(String fromObjectName) {
        if (!describedObjectsCache.containsKey(fromObjectName)) {
            DescribeSObjectResult description = objectDescriptor.apply(fromObjectName);
            describedObjectsCache.put(fromObjectName, description);
            return description;
        } else {
            return describedObjectsCache.get(fromObjectName);
        }
    }

    public String getFromObjectName() {
        return getQueryData().getFromClause().getMainObjectSpec().getObjectName();
    }

    private SOQLQuery getQueryData() {
        if (queryData == null) {
            try {
                queryData = SOQLParserHelper.createSOQLData(soql);
            } catch (SOQLParsingException e) {
                if (e.getMessage().startsWith("There was a SOQL parsing error close to '*'")) {
                    String soqlExpandedToId = soql.replace("*", " Id ");
                    try {
                        queryData = SOQLParserHelper.createSOQLData(soqlExpandedToId);
                        this.expandedStarSyntaxForFields = true;
                        DescribeSObjectResult describeSObjectResult = describeObject(queryData.getMainObjectSpec().getObjectName());
                        String fields = Arrays.stream(describeSObjectResult.getFields())
                                .limit(100) // Limit to first 100 fields
                                .map(Field::getName).collect(Collectors.joining(", "));
                        logger.log(Level.INFO,"Warning in SOQL query parsing. Expansion of * fields to first 100 of "+describeSObjectResult.getFields().length+". Please fix the query as SOQL does not support * to fetch all the fields");
                        String soqlExpanded = soql.replace("*", fields);
                        queryData = SOQLParserHelper.createSOQLData(soqlExpanded);
                        this.soql = soqlExpanded;
                    } catch (SOQLParsingException e2) {
                        logger.log(Level.WARNING,"Error in SOQL query parsing. Expansion of * failed. Please fix the query as SOQL does not support * to fetch all the fields", e2);
                        throw new SOQLParsingException("Error in SOQL query parsing. Expansion of * failed. Please fix the query as SOQL does not support * to fetch all the fields", e);
                    }
                } else {
                    throw e;
                }
            }
        }
        return queryData;
    }

}
