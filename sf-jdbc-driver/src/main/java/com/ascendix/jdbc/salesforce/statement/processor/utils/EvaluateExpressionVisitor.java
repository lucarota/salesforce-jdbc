package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.exceptions.UnsupportedArgumentTypeException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Time;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.Date;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.schema.Column;

@Slf4j
public class EvaluateExpressionVisitor extends ExpressionVisitorAdapter {

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
    public void visit(BitwiseRightShift aThis) {
        log.warn("[EvaluateExpressionVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        log.warn("[EvaluateExpressionVisitor] BitwiseLeftShift");
    }

    @Override
    public void visit(NullValue nullValue) {
        log.trace("[EvaluateExpressionVisitor] NullValue");
        result = null;
    }

    @Override
    public void visit(Function function) {
        log.trace("[EvaluateExpressionVisitor] Function function={}", function.getName());
        if ("now".equalsIgnoreCase(function.getName())) {
            result = new Date();
            return;
        }
        if ("getdate".equalsIgnoreCase(function.getName())) {
            result = LocalDate.now();
            return;
        }
        throw new RuntimeException("Function '" + function.getName() + "' is not implemented.");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        log.trace("[EvaluateExpressionVisitor] DoubleValue={}", doubleValue.getValue());
        result = doubleValue.getValue();
    }

    @Override
    public void visit(LongValue longValue) {
        log.trace("[EvaluateExpressionVisitor] LongValue={}", longValue.getValue());
        result = longValue.getValue();
    }

    @Override
    public void visit(HexValue hexValue) {
        log.trace("[EvaluateExpressionVisitor] HexValue={}", hexValue.getValue());
        result = hexValue.getValue();
    }

    @Override
    public void visit(DateValue dateValue) {
        log.trace("[EvaluateExpressionVisitor] DateValue={}", dateValue.getValue());
        result = dateValue.getValue();
    }

    @Override
    public void visit(TimeValue timeValue) {
        log.trace("[EvaluateExpressionVisitor] BitwiseRightShift={}", timeValue.getValue());
        result = timeValue.getValue();
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        log.trace("[EvaluateExpressionVisitor] TimestampValue={}", timestampValue.getValue());
        result = timestampValue.getValue();
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        parenthesis.getExpression().accept(subEvaluatorLeft);
        result = subEvaluatorLeft.getResult();
        log.trace("[EvaluateExpressionVisitor] Parenthesis={}", result);
    }

    @Override
    public void visit(StringValue stringValue) {
        log.trace("[EvaluateExpressionVisitor] StringValue={}", stringValue.getValue());
        result = stringValue.getValue();
    }

    Object processDateNumberOperation(EvaluateExpressionVisitor left, EvaluateExpressionVisitor right,
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
    public void visit(Addition addition) {
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
            return;
        }

        boolean isString = subEvaluatorLeft.isResultString() || subEvaluatorRight.isResultString();
        try {
            if (subEvaluatorLeft.isResultFloatNumber() || subEvaluatorRight.isResultFloatNumber()) {
                result = subEvaluatorLeft.getResultFloatNumber() + subEvaluatorRight.getResultFloatNumber();
                return;
            }
            if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
                result = subEvaluatorLeft.getResultFixedNumber() + subEvaluatorRight.getResultFixedNumber();
                return;
            }
        } catch (NumberFormatException e) {
            isString = true;
        }

        if (isString) {
            // if string - convert to string "null"
            result = subEvaluatorLeft.getResultString("null") + subEvaluatorRight.getResultString("null");
            return;
        }

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return;
        }
        result = null;
        String message = String.format("Addition not implemented for types %s and %s",
            subEvaluatorLeft.result.getClass().getName(),
            subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public void visit(Division division) {
        log.trace("[EvaluateExpressionVisitor] Division");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        division.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        division.getRightExpression().accept(subEvaluatorRight);

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return;
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
            return;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() / subEvaluatorRight.getResultFixedNumber();
            return;
        }
        result = null;
        String message = String.format("Division not implemented for types %s and %s",
            subEvaluatorLeft.result.getClass().getName(),
            subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public void visit(IntegerDivision division) {
        log.trace("[EvaluateExpressionVisitor] IntegerDivision");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        division.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        division.getRightExpression().accept(subEvaluatorRight);

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return;
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
            return;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() / subEvaluatorRight.getResultFixedNumber();
            return;
        }
        String message = String.format("Division not implemented for types %s and %s",
            subEvaluatorLeft.result.getClass().getName(),
            subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public void visit(Multiplication multiplication) {
        log.trace("[EvaluateExpressionVisitor] Multiplication");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        multiplication.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        multiplication.getRightExpression().accept(subEvaluatorRight);

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return;
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
            return;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() * subEvaluatorRight.getResultFixedNumber();
            return;
        }
        result = null;
        String message = String.format("Multiplication not implemented for types %s and %s",
            subEvaluatorLeft.result.getClass().getName(),
            subEvaluatorRight.result.getClass().getName());
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public void visit(Subtraction subtraction) {
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
            return;
        }

        if (subEvaluatorLeft.isResultNull() || subEvaluatorRight.isResultNull()) {
            // if any of the parameters is null - return null
            result = null;
            return;
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
            return;
        }
        if (subEvaluatorLeft.isResultFixedNumber() || subEvaluatorRight.isResultFixedNumber()) {
            result = subEvaluatorLeft.getResultFixedNumber() - subEvaluatorRight.getResultFixedNumber();
            return;
        }
        String message = String.format("Subtraction not implemented for types %s and %s",
            subEvaluatorLeft.result.getClass().getName(),
            subEvaluatorRight.result.getClass().getName());
        log.error(message);
        result = null;
        throw new UnsupportedArgumentTypeException(message);
    }

    @Override
    public void visit(Column tableColumn) {
        log.trace("[EvaluateExpressionVisitor] Column column={}", tableColumn.getColumnName());
        result = recordFieldsFromDB.get(tableColumn.getColumnName());
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        log.trace("[EvaluateExpressionVisitor] CaseExpression");
        EvaluateExpressionVisitor caseEvaluatorLeft = subVisitor();
        caseExpression.getSwitchExpression().accept(caseEvaluatorLeft);
    }

    @Override
    public void visit(Concat concat) {
        log.trace("[EvaluateExpressionVisitor] Concat");
        EvaluateExpressionVisitor subEvaluatorLeft = subVisitor();
        concat.getLeftExpression().accept(subEvaluatorLeft);
        EvaluateExpressionVisitor subEvaluatorRight = subVisitor();
        concat.getRightExpression().accept(subEvaluatorRight);

        result = subEvaluatorLeft.getResultString("null") + subEvaluatorRight.getResultString("null");
    }
}
