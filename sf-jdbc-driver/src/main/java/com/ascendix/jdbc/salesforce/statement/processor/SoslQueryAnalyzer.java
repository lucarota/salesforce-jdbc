package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import org.mule.tools.soql.SOQLDataBaseVisitor;
import org.mule.tools.soql.SOQLParserHelper;
import org.mule.tools.soql.query.SOQLQuery;
import org.mule.tools.soql.query.SOQLSubQuery;
import org.mule.tools.soql.query.clause.FromClause;
import org.mule.tools.soql.query.from.ObjectSpec;
import org.mule.tools.soql.query.select.FieldSpec;
import org.mule.tools.soql.query.select.FunctionCallSpec;

public class SoslQueryAnalyzer {

    private String soql;
    private final Function<String, DescribeSObjectResult> objectDescriptor;
    private final Map<String, DescribeSObjectResult> describedObjectsCache;
    private SOQLQuery queryData;

    public SoslQueryAnalyzer(String soql, Function<String, DescribeSObjectResult> objectDescriptor) {
        this(soql, objectDescriptor, new HashMap<>());
    }

    public SoslQueryAnalyzer(String soql, Function<String, DescribeSObjectResult> objectDescriptor,
        Map<String, DescribeSObjectResult> describedObjectsCache) {
        this.soql = soql;
        this.objectDescriptor = objectDescriptor;
        this.describedObjectsCache = describedObjectsCache;
    }

    private List fieldDefinitions;

    public boolean analyse(String soql) {
        if (soql == null || soql.trim().isEmpty()) {
            return false;
        }
        this.soql = soql.trim();
        return this.soql.toLowerCase(Locale.ROOT).startsWith("find ");
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
            String objectPrefix =
                !fieldSpec.getObjectPrefixNames().isEmpty() ? fieldSpec.getObjectPrefixNames().get(0) : null;
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
                Field reference = findField(referenceName,
                    describeObject(fromObject),
                    Field::getRelationshipName);
                fromObject = reference.getReferenceTo()[0];
                fieldPrefixes.remove(0);
            }
            String type = findField(name, describeObject(fromObject), Field::getName).getType().name();
            FieldDef result = new FieldDef(name, alias, type);
            return result;
        }

        private final List<String> FUNCTIONS_HAS_INT_RESULT = Arrays.asList("COUNT",
            "COUNT_DISTINCT",
            "CALENDAR_MONTH",
            "CALENDAR_QUARTER",
            "CALENDAR_YEAR",
            "DAY_IN_MONTH",
            "DAY_IN_WEEK",
            "DAY_IN_YEAR",
            "DAY_ONLY",
            "FISCAL_MONTH",
            "FISCAL_QUARTER",
            "FISCAL_YEAR",
            "HOUR_IN_DAY",
            "WEEK_IN_MONTH",
            "WEEK_IN_YEAR");

        @Override
        public Void visitFunctionCallSpec(FunctionCallSpec functionCallSpec) {
            String alias =
                functionCallSpec.getAlias() != null ? functionCallSpec.getAlias() : functionCallSpec.getFunctionName();
            if (FUNCTIONS_HAS_INT_RESULT.contains(functionCallSpec.getFunctionName().toUpperCase())) {
                fieldDefinitions.add(new FieldDef(alias, alias, "int"));
            } else {
                org.mule.tools.soql.query.data.Field param = (org.mule.tools.soql.query.data.Field) functionCallSpec.getFunctionParameters()
                    .get(0);
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
                .orElseThrow(() -> new IllegalArgumentException(
                    "Unresolved relationship in subquery \"" + subquerySoql + "\""));
            String fromObject = relatedFrom.getChildSObject();
            subquery.setFromClause(new FromClause(new ObjectSpec(fromObject, null)));

            SoslQueryAnalyzer subqueryAnalyzer = new SoslQueryAnalyzer(subquery.toSOQLText(),
                objectDescriptor,
                describedObjectsCache);
            fieldDefinitions.add(new ArrayList(subqueryAnalyzer.getFieldDefinitions()));
            return null;
        }
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
            .orElseThrow(() -> new IllegalArgumentException(
                "Unknown field name \"" + name + "\" in object \"" + objectDesc.getName() + "\""));
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

    protected String getFromObjectName() {
        return getQueryData().getFromClause().getMainObjectSpec().getObjectName();
    }

    private SOQLQuery getQueryData() {
        if (queryData == null) {
            queryData = SOQLParserHelper.createSOQLData(soql);
        }
        return queryData;
    }
}
