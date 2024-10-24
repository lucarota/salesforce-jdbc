package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.ascendix.jdbc.salesforce.statement.processor.QueryAnalyzer;
import com.ascendix.jdbc.salesforce.statement.processor.SoqlQueryAnalyzer;
import com.ascendix.jdbc.salesforce.utils.FieldDefTree;
import com.sforce.soap.partner.ChildRelationship;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Function;

public class SelectSpecVisitor implements SelectItemVisitor<Expression> {

    private final String rootEntityName;
    private final FieldDefTree fieldDefinitions;
    private final PartnerService partnerService;

    public SelectSpecVisitor(String rootEntityName, FieldDefTree fieldDefinitions,
        final PartnerService partnerService) {
        this.rootEntityName = rootEntityName;
        this.fieldDefinitions = fieldDefinitions;
        this.partnerService = partnerService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S> Expression visit(SelectItem<? extends Expression> fieldSpec, S context) {
        visitInternal((SelectItem<Expression>) fieldSpec);
        return null;
    }

    private void visitInternal(SelectItem<Expression> fieldSpec) {
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
                if (prefixNames.size() > 1 && prefixNames.get(0).equals(rootEntityName)) {
                    alias = String.join(".", prefixNames.subList(1, prefixNames.size())) + name;
                } else {
                    alias = column.getTable().getFullyQualifiedName() + "." + name;
                }
            }
            FieldDef result = createFieldDef(name, alias, prefixNames);
            fieldDefinitions.addChild(result);
            /* Remove alias from query */
            fieldSpec.setAlias(null);
        } else if (fieldSpec.getExpression() instanceof net.sf.jsqlparser.expression.Function func) {
            String alias = fieldSpec.getAlias() != null ? fieldSpec.getAlias().getName() : func.getName();
            visitFunctionCallSpec(func, alias);
        } else if (fieldSpec.getExpression() instanceof ParenthesedSelect subQuery) {
            fieldSpec.setExpression(visitSubQuery(subQuery));
        }
    }

    private FieldDef createFieldDef(String name, String alias, List<String> prefixNames) {
        List<String> fieldPrefixes = new ArrayList<>(prefixNames);
        String fromObject = rootEntityName;
        if (!fieldPrefixes.isEmpty() && fieldPrefixes.get(0).equalsIgnoreCase(fromObject)) {
            fieldPrefixes.remove(0);
        }
        StringBuilder prefix = new StringBuilder();
        while (!fieldPrefixes.isEmpty()) {
            String referenceName = fieldPrefixes.get(0);
            prefix.append(referenceName).append(".");
            Field reference = findField(referenceName, describeObject(fromObject), Field::getRelationshipName);
            fromObject = reference.getReferenceTo()[0];
            fieldPrefixes.remove(0);
        }
        Field field = findField(name, describeObject(fromObject), Field::getName);
        String type = field.getType().name();

        return new FieldDef(name, prefix + name, alias, type);
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
            fieldDefinitions.addChild(new FieldDef(alias, alias, alias, "int"));
        } else {
            Expression param = functionCallSpec.getParameters().get(0);
            String[] prefix = StringUtils.split(param.toString(), '.');
            List<String> prefixNames = List.of(ArrayUtils.remove(prefix, prefix.length - 1));
            FieldDef result = createFieldDef(param.toString(), alias, prefixNames);
            fieldDefinitions.addChild(result);
        }
    }

    private Expression visitSubQuery(ParenthesedSelect subQuery) {
        PlainSelect select = subQuery.getPlainSelect();
        final FromItem from = select.getFromItem();
        String relationshipName;
        String[] prefixNames = StringUtils.split(from.toString(), '.');
        if (prefixNames.length > 1 && rootEntityName.equalsIgnoreCase(prefixNames[0])) {
            relationshipName = prefixNames[1];
        } else {
            relationshipName = from.toString();
        }

        ChildRelationship relatedFrom = Arrays.stream(describeObject(rootEntityName).getChildRelationships())
            .filter(rel -> relationshipName.equalsIgnoreCase(rel.getRelationshipName())).findFirst()
            .orElseThrow(() -> new IllegalArgumentException(
                "Unresolved relationship in subquery \"" + subQuery.getPlainSelect().toString() + "\""));

        String fromObject = relatedFrom.getChildSObject();
        select.setFromItem(new Table(fromObject));

        SoqlQueryAnalyzer subQueryAnalyzer = new SoqlQueryAnalyzer(new QueryAnalyzer(select.toString(),null, partnerService));
        fieldDefinitions.addTreeNode(subQueryAnalyzer.getFieldDefinitions());

        final PlainSelect soqlQuery = (PlainSelect)subQueryAnalyzer.getSoqlQuery();
        soqlQuery.setFromItem(from);
        subQuery.withSelect(soqlQuery);
        return subQuery;
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
}
