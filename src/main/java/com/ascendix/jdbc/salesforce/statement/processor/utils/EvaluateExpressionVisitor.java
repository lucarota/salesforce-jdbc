package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.exceptions.UnsupportedArgumentTypeException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.schema.Column;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Time;
import java.time.*;
import java.util.Date;
import java.util.Map;

@Slf4j
public class EvaluateExpressionVisitor extends ExpressionVisitorAdapter<Expression> {

    protected Map<String, Object> recordFieldsFromDB;
    @Getter
    protected Object result;

    public EvaluateExpressionVisitor(Map<String, Object> recordFieldsFromDB) {
        this.recordFieldsFromDB = recordFieldsFromDB;
    }

    public EvaluateExpressionVisitor subVisitor() {
        return new EvaluateExpressionVisitor(recordFieldsFromDB);
    }

    public String getResultString() {
        return (String) result;
    }

    public String getResultString(String defaultValue) {
        if (result == null) {
            return defaultValue;
        }
        if (result instanceof String value) {
            return value;
        }
        return result.toString();
    }

    public long getResultFixedNumber() {
        if (result == null) {
            return 0;
        }
        if (result instanceof Double d) {
            return d.longValue();
        }
        if (result instanceof Float f) {
            return f.longValue();
        }
        if (result instanceof Long l) {
            return l;
        }
        if (result instanceof Integer i) {
            return i.longValue();
        }
        if (result instanceof String s) {
            return Long.parseLong(s);
        }
        String message = String.format("Cannot convert to Fixed type %s value %s", result.getClass().getName(), result);
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    public double getResultFloatNumber() {
        if (result == null) {
            return 0d;
        }
        if (result instanceof Double d) {
            return d;
        }
        if (result instanceof Float f) {
            return f.doubleValue();
        }
        if (result instanceof Long l) {
            return l.doubleValue();
        }
        if (result instanceof Integer i) {
            return i.doubleValue();
        }
        if (result instanceof String s) {
            return Double.parseDouble(s);
        }
        String message = String.format("Cannot convert to Float type %s value %s", result.getClass().getName(), result);
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    public boolean isResultString() {
        return result instanceof String;
    }

    public boolean isResultFixedNumber() {
        return result != null && (result instanceof Long || result instanceof Integer);
    }

    public boolean isResultFloatNumber() {
        return result != null && (result instanceof Double || result instanceof Float);
    }

    public boolean isResultDateTime() {
        return result != null && (result instanceof Instant || result instanceof java.sql.Timestamp);
    }

    public boolean isResultTime() {
        return result instanceof java.sql.Time;
    }

    public boolean isResultDate() {
        return result instanceof java.sql.Date;
    }

    public boolean isResultNull() {
        return result == null;
    }

    @Override
    public <S> Expression visit(BitwiseRightShift aThis, S context) {
        log.warn("[EvaluateExpressionVisitor] BitwiseRightShift");
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseLeftShift aThis, S context) {
        log.warn("[EvaluateExpressionVisitor] BitwiseLeftShift");
        return null;
    }

    @Override
    public <S> Expression visit(NullValue nullValue, S context) {
        log.trace("[EvaluateExpressionVisitor] NullValue");
        result = null;
        return null;
    }

    @Override
    public <S> Expression visit(Function function, S context) {
        log.trace("[EvaluateExpressionVisitor] Function function={}", function.getName());
        if ("now".equalsIgnoreCase(function.getName())) {
            result = new Date();
            return null;
        }
        if ("getdate".equalsIgnoreCase(function.getName())) {
            result = LocalDate.now();
            return null;
        }
        throw new RuntimeException("Function '" + function.getName() + "' is not implemented.");
    }

    @Override
    public <S> Expression visit(DoubleValue doubleValue, S context) {
        log.trace("[EvaluateExpressionVisitor] DoubleValue={}", doubleValue.getValue());
        result = doubleValue.getValue();
        return null;
    }

    @Override
    public <S> Expression visit(LongValue longValue, S context) {
        log.trace("[EvaluateExpressionVisitor] LongValue={}", longValue.getValue());
        result = longValue.getValue();
        return null;
    }

    @Override
    public <S> Expression visit(HexValue hexValue, S context) {
        log.trace("[EvaluateExpressionVisitor] HexValue={}", hexValue.getValue());
        result = hexValue.getValue();
        return null;
    }

    @Override
    public <S> Expression visit(DateValue dateValue, S context) {
        log.trace("[EvaluateExpressionVisitor] DateValue={}", dateValue.getValue());
        result = dateValue.getValue();
        return null;
    }

    @Override
    public <S> Expression visit(TimeValue timeValue, S context) {
        log.trace("[EvaluateExpressionVisitor] BitwiseRightShift={}", timeValue.getValue());
        result = timeValue.getValue();
        return null;
    }

    @Override
    public <S> Expression visit(TimestampValue timestampValue, S context) {
        log.trace("[EvaluateExpressionVisitor] TimestampValue={}", timestampValue.getValue());
        result = timestampValue.getValue();
        return null;
    }

    @Override
    public <S> Expression visit(StringValue stringValue, S context) {
        log.trace("[EvaluateExpressionVisitor] StringValue={}", stringValue.getValue());
        result = stringValue.getValue();
        return null;
    }

    private Object processDateNumberOperation(EvaluateExpressionVisitor left, EvaluateExpressionVisitor right,
                                              boolean isAdding) {
        BigDecimal changeValue = BigDecimal.valueOf(right.getResultFloatNumber());
        // https://oracle-base.com/articles/misc/oracle-dates-timestamps-and-intervals
        // Also rounding - because otherwise 1 second will be 0.999993600
        long secondsValue = changeValue.subtract(BigDecimal.valueOf(changeValue.intValue()))
                .multiply(BigDecimal.valueOf(86400), new MathContext(4))
                .longValue();

        Period changeDays = Period.ofDays(isAdding ? changeValue.intValue() : -changeValue.intValue());
        Duration changeSec = Duration.ofSeconds(isAdding ? secondsValue : -secondsValue);
        if (left.result instanceof java.sql.Date date) {
            LocalDate instant = date.toLocalDate();
            instant = instant.plus(changeDays);
            result = java.sql.Date.valueOf(instant);
        }
        if (left.result instanceof Instant instant) {
            instant = instant.plus(changeDays);
            instant = instant.plus(changeSec);
            result = instant;
        }
        if (left.result instanceof java.sql.Timestamp timestamp) {
            Instant instant = timestamp.toInstant();
            instant = instant.plus(changeDays);
            instant = instant.plus(changeSec);
            result = new java.sql.Timestamp(instant.toEpochMilli());
        }
        if (left.result instanceof Time time) {
            LocalTime instant = time.toLocalTime();
            instant = instant.plus(changeSec);
            result = java.sql.Time.valueOf(instant);
        }

        return result;
    }

    @Override
    public <S> Expression visit(Addition addition, S context) {
        log.trace("[EvaluateExpressionVisitor] Addition");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        addition.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        addition.getRightExpression().accept(subEvaluatorRight);

        if ((!subEvaluatorLeft.isResultNull() && !subEvaluatorRight.isResultNull())
                && ((subEvaluatorLeft.isResultDateTime() || subEvaluatorLeft.isResultDate()
                || subEvaluatorLeft.isResultTime()) &&
                (subEvaluatorRight.isResultFloatNumber() || subEvaluatorRight.isResultFixedNumber()))) {
            result = processDateNumberOperation(subEvaluatorLeft, subEvaluatorRight, true);
            return null;
        }

        boolean isString = subEvaluatorLeft.isResultString() || subEvaluatorRight.isResultString();
        try {
            if (subEvaluatorLeft.isResultFloatNumber() || subEvaluatorRight.isResultFloatNumber()) {
                result = subEvaluatorLeft.getResultFloatNumber() + subEvaluatorRight.getResultFloatNumber();
                return null;
            }
            if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
                result = subEvaluatorLeft.getResultFixedNumber() + subEvaluatorRight.getResultFixedNumber();
                return null;
            }
        } catch (NumberFormatException e) {
            isString = true;
        }

        if (isString) {
            // if string - convert to string "null"
            result = subEvaluatorLeft.getResultString("null") + subEvaluatorRight.getResultString("null");
            return null;
        }

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return null;
        }
        result = null;
        String message = String.format("Addition not implemented for types %s and %s",
                subEvaluatorLeft.result.getClass().getName(),
                subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public <S> Expression visit(Division division, S context) {
        log.trace("[EvaluateExpressionVisitor] Division");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        division.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        division.getRightExpression().accept(subEvaluatorRight);

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return null;
        }

        boolean isString = subEvaluatorLeft.isResultString() || subEvaluatorRight.isResultString();
        if (isString) {
            String message = String.format("Division not implemented for types %s and %s",
                    subEvaluatorLeft.result.getClass().getName(),
                    subEvaluatorRight.result.getClass().getName());
            log.error(message);
            throw new UnsupportedArgumentTypeException(message);
        }

        if (subEvaluatorLeft.isResultFloatNumber() || subEvaluatorRight.isResultFloatNumber()) {
            result = subEvaluatorLeft.getResultFloatNumber() / subEvaluatorRight.getResultFloatNumber();
            return null;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() / subEvaluatorRight.getResultFixedNumber();
            return null;
        }
        result = null;
        String message = String.format("Division not implemented for types %s and %s",
                subEvaluatorLeft.result.getClass().getName(),
                subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public <S> Expression visit(IntegerDivision division, S context) {
        log.trace("[EvaluateExpressionVisitor] IntegerDivision");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        division.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        division.getRightExpression().accept(subEvaluatorRight);

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return null;
        }

        boolean isString = subEvaluatorLeft.isResultString() || subEvaluatorRight.isResultString();
        if (isString) {
            String message = String.format("Division not implemented for types %s and %s",
                    subEvaluatorLeft.result.getClass().getName(),
                    subEvaluatorRight.result.getClass().getName());
            log.error(message);
            throw new UnsupportedArgumentTypeException(message);
        }

        if (subEvaluatorLeft.isResultFloatNumber() || subEvaluatorRight.isResultFloatNumber()) {
            result = Double.doubleToLongBits(
                    subEvaluatorLeft.getResultFloatNumber() / subEvaluatorRight.getResultFloatNumber());
            return null;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() / subEvaluatorRight.getResultFixedNumber();
            return null;
        }
        String message = String.format("Division not implemented for types %s and %s",
                subEvaluatorLeft.result.getClass().getName(),
                subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public <S> Expression visit(Multiplication multiplication, S context) {
        log.trace("[EvaluateExpressionVisitor] Multiplication");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        multiplication.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        multiplication.getRightExpression().accept(subEvaluatorRight);

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return null;
        }

        boolean isString = subEvaluatorLeft.isResultString() || subEvaluatorRight.isResultString();
        if (isString) {
            String message = String.format("Multiplication not implemented for types %s and %s",
                    subEvaluatorLeft.result.getClass().getName(),
                    subEvaluatorRight.result.getClass().getName());
            log.error(message);
            throw new UnsupportedArgumentTypeException(message);
        }

        if (subEvaluatorLeft.isResultFloatNumber() || subEvaluatorRight.isResultFloatNumber()) {
            result = subEvaluatorLeft.getResultFloatNumber() * subEvaluatorRight.getResultFloatNumber();
            return null;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() * subEvaluatorRight.getResultFixedNumber();
            return null;
        }
        result = null;
        String message = String.format("Multiplication not implemented for types %s and %s",
                subEvaluatorLeft.result.getClass().getName(),
                subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public <S> Expression visit(Subtraction subtraction, S context) {
        log.trace("[EvaluateExpressionVisitor] Subtraction");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        subtraction.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        subtraction.getRightExpression().accept(subEvaluatorRight);

        if ((!subEvaluatorLeft.isResultNull() && !subEvaluatorRight.isResultNull())
                && ((subEvaluatorLeft.isResultDateTime() || subEvaluatorLeft.isResultDate()
                || subEvaluatorLeft.isResultTime()) &&
                (subEvaluatorRight.isResultFloatNumber() || subEvaluatorRight.isResultFixedNumber()))) {
            result = processDateNumberOperation(subEvaluatorLeft, subEvaluatorRight, false);
            return null;
        }

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return null;
        }

        boolean isString = subEvaluatorLeft.isResultString() || subEvaluatorRight.isResultString();
        if (isString) {
            String message = String.format("Subtraction not implemented for types %s and %s",
                    subEvaluatorLeft.result.getClass().getName(),
                    subEvaluatorRight.result.getClass().getName());
            log.error(message);
            throw new UnsupportedArgumentTypeException(message);
        }

        if (subEvaluatorLeft.isResultFloatNumber() || subEvaluatorRight.isResultFloatNumber()) {
            result = subEvaluatorLeft.getResultFloatNumber() - subEvaluatorRight.getResultFloatNumber();
            return null;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() - subEvaluatorRight.getResultFixedNumber();
            return null;
        }
        String message = String.format("Subtraction not implemented for types %s and %s",
                subEvaluatorLeft.result.getClass().getName(),
                subEvaluatorRight.result.getClass().getName());
        log.error(message);
        result = null;
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public <S> Expression visit(Column tableColumn, S context) {
        log.trace("[EvaluateExpressionVisitor] Column column={}", tableColumn.getColumnName());
        result = recordFieldsFromDB.get(tableColumn.getColumnName());
        return null;
    }

    @Override
    public <S> Expression visit(CaseExpression caseExpression, S context) {
        log.trace("[EvaluateExpressionVisitor] CaseExpression");
        EvaluateExpressionVisitor caseEvaluatorLeft = subVisitor();
        caseExpression.getSwitchExpression().accept(caseEvaluatorLeft);
        return null;
    }

    @Override
    public <S> Expression visit(Concat concat, S context) {
        log.trace("[EvaluateExpressionVisitor] Concat");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        concat.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        concat.getRightExpression().accept(subEvaluatorRight);

        result = subEvaluatorLeft.getResultString("null") + subEvaluatorRight.getResultString("null");
        return null;
    }
}
