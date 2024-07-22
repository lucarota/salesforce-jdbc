package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.utils.ParamUtils;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

@Slf4j
public class ValueToStringVisitor extends ExpressionVisitorAdapter<Expression> {

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
    public <S> Expression visit(NullValue nullValue, S context) {
        log.trace("[VtoSVisitor] NullValue");
        fieldValues.put(columnName, null);
        return null;
    }

    @Override
    public <S> Expression visit(DoubleValue doubleValue, S context) {
        log.warn("[VtoSVisitor] DoubleValue {}={}", columnName, doubleValue.getValue());
        fieldValues.put(columnName, doubleValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(LongValue longValue, S context) {
        log.warn("[VtoSVisitor] LongValue {}={}", columnName, longValue.getValue());
        fieldValues.put(columnName, longValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(HexValue hexValue, S context) {
        log.warn("[VtoSVisitor] HexValue {}={}", columnName, hexValue.getValue());
        fieldValues.put(columnName, hexValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(StringValue stringValue, S context) {
        log.trace("[VtoSVisitor] StringValue {}={}", columnName, stringValue.getValue());
        fieldValues.put(columnName, stringValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(Select subSelect, S context) {
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
        return null;
    }

    @Override
    public <S> Expression visit(JdbcParameter parameter, S context) {
        int idx = parameter.getIndex() - 1;
        Object o = parameters.get(idx);
        fieldValues.put(columnName, o);
        return null;
    }

    private static Object getParameter(Object o) {
        if (o == null) {
            return "NULL";
        } else if (o instanceof java.sql.Date date) {
            LocalDate localDate = Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault()).toLocalDate();
            return ParamUtils.SQL_DATE_FORMAT.format(localDate);
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
        } else {
            return o;
        }
    }
}
