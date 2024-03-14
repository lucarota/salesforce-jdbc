package com.ascendix.jdbc.salesforce.statement.processor.utils;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SubSelect;

@Slf4j
public class ValueToStringVisitor extends ExpressionVisitorAdapter {

    private final Map<String, Object> fieldValues;
    private final String columnName;
    private final java.util.function.Function<String, List<Map<String, Object>>> subSelectResolver;

    public ValueToStringVisitor(Map<String, Object> fieldValues, String columnName,
        java.util.function.Function<String, List<Map<String, Object>>> subSelectResolver) {
        this.fieldValues = fieldValues;
        this.columnName = columnName;
        this.subSelectResolver = subSelectResolver;
    }

    @Override
    public void visit(NullValue nullValue) {
        log.trace("[VtoSVisitor] NullValue");
        fieldValues.put(columnName, null);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        log.warn("[VtoSVisitor] DoubleValue {}={}", columnName, doubleValue.getValue());
        fieldValues.put(columnName, doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        log.warn("[VtoSVisitor] LongValue {}={}", columnName, longValue.getValue());
        fieldValues.put(columnName, longValue.getValue());
    }

    @Override
    public void visit(HexValue hexValue) {
        log.warn("[VtoSVisitor] HexValue {}={}", columnName, hexValue.getValue());
        fieldValues.put(columnName, hexValue.getValue());
    }

    @Override
    public void visit(StringValue stringValue) {
        log.trace("[VtoSVisitor] StringValue {}={}", columnName, stringValue.getValue());
        fieldValues.put(columnName, stringValue.getValue());
    }

    @Override
    public void visit(SubSelect subSelect) {
        log.trace("[VtoxSVisitor] SubSelect {}={}", columnName, subSelect.toString());
        Object value = null;
        if (subSelectResolver != null) {
            subSelect.setUseBrackets(false);
            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
            if (records.size() == 1 && records.get(0).size() == 1) {
                // return the value as plain value
                value = records.get(0).entrySet().iterator().next().getValue();
                log.trace("[VtoSVisitor] resolved to {}", value);
            }
        } else {
            log.trace("[VtoSVisitor] subSelectResolver is undefined");
        }
        fieldValues.put(columnName, value);
    }
}
