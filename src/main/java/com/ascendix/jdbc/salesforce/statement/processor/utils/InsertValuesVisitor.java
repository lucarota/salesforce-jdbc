package com.ascendix.jdbc.salesforce.statement.processor.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.Values;

@Slf4j
public class InsertValuesVisitor extends SelectVisitorAdapter {

    private final Function<String, List<Map<String, Object>>> subSelectResolver;
    private final List<Column> columns;
    private final List<Map<String, Object>> records;
    private final List<Object> parameters;

    public InsertValuesVisitor(List<Column> columns, List<Map<String, Object>> records, List<Object> parameters,
        Function<String, List<Map<String, Object>>> subSelectResolver) {
        this.columns = columns;
        this.records = records;
        this.parameters = parameters;
        this.subSelectResolver = subSelectResolver;
    }

    @Override
    public void visit(Values values) {
        log.trace("Expression Visitor");

        for (Expression e : values.getExpressions()) {
            if (e instanceof ExpressionList expressionList) {
                new Values(expressionList).accept(new InsertValuesVisitor(columns, records, parameters, subSelectResolver));
            } else {
                HashMap<String, Object> fieldValues = new HashMap<>();
                records.add(fieldValues);
                for (int i = 0; i < columns.size(); i++) {
                    Expression expression = values.getExpressions().get(i);
                    expression.accept(
                        new ValueToStringVisitor(fieldValues, columns.get(i).getColumnName(), parameters, subSelectResolver));
                }
                break;
            }
        }
    }
}
