package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.statement.FieldDef;
import com.ascendix.jdbc.salesforce.statement.processor.utils.SelectSpecVisitor;
import com.ascendix.jdbc.salesforce.utils.FieldDefTree;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

@Slf4j
public class SoqlQueryAnalyzer {

    private final QueryAnalyzer queryAnalyzer;
    private FieldDefTree fieldDefinitions;

    public SoqlQueryAnalyzer(QueryAnalyzer queryAnalyzer) {
        this.queryAnalyzer = queryAnalyzer;
    }

    public String getSoqlQueryString() {
        return queryAnalyzer.getQueryData().toString();
    }

    public Statement getSoqlQuery() {
        return queryAnalyzer.getQueryData();
    }

    public FieldDefTree getFieldDefinitions() {
        if (fieldDefinitions == null) {
            fieldDefinitions = new FieldDefTree();
            String rootEntityName = queryAnalyzer.getFromObjectName();
            if (isExpandedStarSyntaxForFields()) {
                populateFieldDefinitionsStar(rootEntityName);
            } else {
                populateFieldDefinitions(rootEntityName);
            }
        }
        return fieldDefinitions;
    }

    private void populateFieldDefinitions(final String rootEntityName) {
        PlainSelect query = (PlainSelect) queryAnalyzer.getQueryData();
        SelectSpecVisitor visitor = new SelectSpecVisitor(rootEntityName, fieldDefinitions, queryAnalyzer.getPartnerService());
        final List<SelectItem<?>> selectItems = query.getSelectItems();
        if (query.getOrderByElements() != null) {
            query.getOrderByElements()
                .forEach(orderByElement -> orderByElement.accept(new OrderByVisitorAdapter<>() {
                    @Override
                    public <S> Expression visit(OrderByElement orderBy, S context) {
                        if (orderBy.getExpression() instanceof Column orderCol) {
                            selectItems.forEach(item -> {
                                if (item.getAlias() != null && item.getAlias().getName() .equals(orderCol.getColumnName())
                                    && item.getExpression() instanceof Column c) {
                                    orderCol.setColumnName(c.getColumnName());
                                }
                            });
                        }
                        return null;
                    }
                }, null));
        }
        selectItems.forEach(spec -> spec.accept(visitor, null));
    }

    private void populateFieldDefinitionsStar(final String rootEntityName) {
        Arrays.stream(queryAnalyzer.getExpandedFields())
            .forEach(field -> {
                String type = field.getType().name();
                String name = field.getName();
                fieldDefinitions.addChild(new FieldDef(name, rootEntityName + "." + name, name, type));
            });
    }

    public boolean isExpandedStarSyntaxForFields() {
        return queryAnalyzer.isExpandedStarSyntaxForFields();
    }

    public String getFromObjectName() {
        return queryAnalyzer.getFromObjectName();
    }
}
