package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.utils.ParamUtils;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

@Slf4j
public class ValueToStringVisitor extends ExpressionVisitorAdapter {

    private final Map<String, Object> fieldValues;
    private final String columnName;
    private final BiFunction<String, List<Object>, List<Map<String, Object>>> subSelectResolver;
    private final List<Object> parameters;

    public ValueToStringVisitor(Map<String, Object> fieldValues, String columnName, List<Object> parameters,
                                BiFunction<String, List<Object>, List<Map<String, Object>>> subSelectResolver) {
        this.fieldValues = fieldValues;
        this.columnName = columnName;
        this.parameters = parameters;
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
    public void visit(Select subSelect) {
        Object value = null;
        PlainSelect plainSelect = subSelect.getPlainSelect();
        if (plainSelect != null) {
            log.trace("[VtoxSVisitor] SubSelect {}={}", columnName, plainSelect);
            if (subSelectResolver != null) {
                List<Map<String, Object>> records = subSelectResolver.apply(plainSelect.toString(), parameters);
                if (records.size() == 1 && records.get(0).size() == 1) {
                    // return the value as plain value
                    value = records.get(0).entrySet().iterator().next().getValue();
                    log.trace("[VtoSVisitor] resolved to {}", value);
                }
            } else {
                log.trace("[VtoSVisitor] subSelectResolver is undefined");
            }
        }
        fieldValues.put(columnName, value);
    }

    @Override
    public void visit(JdbcParameter parameter) {
        int idx = parameter.getIndex() - 1;
        Object o = parameters.get(idx);
        fieldValues.put(columnName, getParameterStr(o));
    }

    private static String getParameterStr(Object o) {
        if (o == null) {
            return "NULL";
        } else if (o instanceof Date date) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            if (cal.get(Calendar.MILLISECOND) == 0 && cal.get(Calendar.SECOND) == 0 && cal.get(
                    Calendar.MINUTE) == 0 && cal.get(Calendar.HOUR_OF_DAY) == 0) {
                LocalDate localDate = Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();

                return ParamUtils.SQL_DATE_FORMAT.format(localDate);
            } else {
                LocalDateTime localDateTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                return ParamUtils.SQL_TIMESTAMP_FORMAT.format(localDateTime);
            }
        } else if (o instanceof Calendar cal) {
            if (cal.get(Calendar.MILLISECOND) == 0 && cal.get(Calendar.SECOND) == 0 && cal.get(
                    Calendar.MINUTE) == 0 && cal.get(Calendar.HOUR_OF_DAY) == 0) {
                LocalDate localDate = Instant.ofEpochMilli(cal.getTimeInMillis()).atZone(ZoneId.systemDefault())
                        .toLocalDate();

                return ParamUtils.SQL_DATE_FORMAT.format(localDate);
            } else {
                LocalDateTime localDateTime = cal.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                return ParamUtils.SQL_TIMESTAMP_FORMAT.format(localDateTime);
            }
        } else if (o instanceof BigDecimal d) {
            return d.toPlainString();
        } else {
            return o.toString();
        }
    }
}
