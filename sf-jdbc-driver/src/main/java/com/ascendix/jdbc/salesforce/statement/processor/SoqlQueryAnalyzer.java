package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class SoqlQueryAnalyzer {

    private String soql;
    private final PartnerService partnerService;

    private PlainSelect queryData;
    @Getter
    private boolean expandedStarSyntaxForFields = false;
    private List<FieldDef> fieldDefinitions;

    public SoqlQueryAnalyzer(String soql, PartnerService partnerService) {
        this.soql = soql;
        this.partnerService = partnerService;
        // to parse the query and process the expansion if needed
        getQueryData();
    }

    public String getOriginalSoqlQuery() {
        return this.soql;
    }

    public String getSoqlQuery() {
        return this.queryData.toString();
    }

    private class SelectSpecVisitor implements SelectItemVisitor {

        private final String rootEntityName;

        public SelectSpecVisitor(String rootEntityName) {
            this.rootEntityName = rootEntityName;
        }

        @Override
        public void visit(SelectItem fieldSpec) {
            if (fieldSpec.getExpression() instanceof Column column) {
                String name = column.getColumnName();
                String alias = fieldSpec.getAlias() != null ? fieldSpec.getAlias().getName() : name;

                String objectPrefix = null;
                List<String> prefixNames = List.of();
                if (column.getTable() != null) {
                    objectPrefix = column.getTable().getName();
                    String[] prefix = StringUtils.split(column.getTable().getFullyQualifiedName(), '.');
                    prefixNames = List.of(prefix);
                }
                // If Object Name specified - verify it is not the same as SOQL root entity
                if (fieldSpec.getAlias() == null && objectPrefix != null && !objectPrefix.equals(rootEntityName)) {
                    alias = objectPrefix + "." + name;
                }
                FieldDef result = createFieldDef(name, alias, prefixNames);
                fieldDefinitions.add(result);
                /* Remove alias from query */
                fieldSpec.setAlias(null);
            } else if (fieldSpec.getExpression() instanceof net.sf.jsqlparser.expression.Function func) {
                String alias = fieldSpec.getAlias() != null ? fieldSpec.getAlias().getName() : func.getName();
                visitFunctionCallSpec(func, alias);
            } else if (fieldSpec.getExpression() instanceof ParenthesedSelect subQuery) {
                visitSubQuery(subQuery);
            }
        }

        private FieldDef createFieldDef(String name, String alias, List<String> prefixNames) {
            List<String> fieldPrefixes = new ArrayList<>(prefixNames);
            String fromObject = getFromObjectName();
            if (!fieldPrefixes.isEmpty() && fieldPrefixes.get(0).equalsIgnoreCase(fromObject)) {
                fieldPrefixes.remove(0);
            }
            while (!fieldPrefixes.isEmpty()) {
                String referenceName = fieldPrefixes.get(0);
                Field reference = findField(referenceName, describeObject(fromObject), Field::getRelationshipName);
                fromObject = reference.getReferenceTo()[0];
                fieldPrefixes.remove(0);
            }
            String type = findField(name, describeObject(fromObject), Field::getName).getType().name();
            return new FieldDef(name, alias, type);
        }

        private static final List<String> FUNCTIONS_HAS_INT_RESULT = Arrays.asList("COUNT",
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

        private void visitFunctionCallSpec(net.sf.jsqlparser.expression.Function functionCallSpec, String alias) {
            if (FUNCTIONS_HAS_INT_RESULT.contains(functionCallSpec.getName().toUpperCase())) {
                fieldDefinitions.add(new FieldDef(alias, alias, "int"));
            } else {
                Expression param = functionCallSpec.getParameters().get(0);
                String[] prefix = StringUtils.split(param.toString(), '.');
                List<String> prefixNames = List.of(ArrayUtils.remove(prefix, prefix.length - 1));
                FieldDef result = createFieldDef(param.toString(), alias, prefixNames);
                fieldDefinitions.add(result);
            }
        }

        private void visitSubQuery(ParenthesedSelect subQuery) {
            try {
                String subQuerySoql = subQuery.getPlainSelect().toString();
                Statement statement = CCJSqlParserUtil.parse(subQuerySoql);
                if (statement instanceof PlainSelect select) {
                    String relationshipName = select.getFromItem().toString();
                    ChildRelationship relatedFrom = Arrays.stream(
                                    describeObject(getFromObjectName()).getChildRelationships())
                            .filter(rel -> relationshipName.equalsIgnoreCase(rel.getRelationshipName())).findFirst()
                            .orElseThrow(() -> new IllegalArgumentException(
                                    "Unresolved relationship in subquery \"" + subQuerySoql + "\""));

                    String fromObject = relatedFrom.getChildSObject();
                    select.setFromItem(new Table(fromObject));

                    SoqlQueryAnalyzer subQueryAnalyzer = new SoqlQueryAnalyzer(select.toString(), partnerService);
                    fieldDefinitions.addAll(subQueryAnalyzer.getFieldDefinitions());
                }
            } catch (JSQLParserException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public List<FieldDef> getFieldDefinitions() {
        if (fieldDefinitions == null) {
            fieldDefinitions = new ArrayList<>();
            String rootEntityName = getFromObjectName();
            SelectSpecVisitor visitor = new SelectSpecVisitor(rootEntityName);
            getQueryData().getSelectItems().forEach(spec -> spec.accept(visitor));
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
        return partnerService.describeSObject(fromObjectName);
    }

    public String getFromObjectName() {
        return getQueryData().getFromItem().toString();
    }

    private PlainSelect getQueryData() {
        if (queryData == null) {
            try {
                Statement statement = CCJSqlParserUtil.parse(soql);
                if (statement instanceof PlainSelect select) {
                    if ("*".equals(select.getSelectItem(0).toString())) {
                        select.getSelectItems().clear();
                        this.expandedStarSyntaxForFields = true;
                        DescribeSObjectResult describeSObjectResult = describeObject(select.getFromItem().toString());
                        Arrays.stream(describeSObjectResult.getFields())
                                .forEach(f -> select.addSelectItem(new Column(null, f.getName())));
                    }
                    this.soql = select.toString();
                    this.queryData = select;
                }
            } catch (JSQLParserException e) {
                log.error("Failed request to create query with error: {}", e.getMessage(), e);
            }
        }
        return queryData;
    }
}
