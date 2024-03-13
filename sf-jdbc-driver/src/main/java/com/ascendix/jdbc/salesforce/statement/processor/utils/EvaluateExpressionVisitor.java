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
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.VariableAssignment;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.XMLSerializeExpr;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.FullTextSearch;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

@Slf4j
public class EvaluateExpressionVisitor implements ExpressionVisitor {

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

    public String getResultString(String ifNull) {
        if (result == null) {
            return ifNull;
        }
        if (result instanceof String) {
            return ((String) result);
        }
        return result.toString();
    }

    public long getResultFixedNumber() {
        if (result == null) {
            return 0;
        }
        if (result instanceof Double) {
            return ((Double) result).longValue();
        }
        if (result instanceof Float) {
            return ((Float) result).longValue();
        }
        if (result instanceof Long) {
            return ((Long) result);
        }
        if (result instanceof Integer) {
            return ((Integer) result).longValue();
        }
        if (result instanceof String) {
            return Long.parseLong((String) result);
        }
        String message = String.format("Cannot convert to Fixed type %s value %s", result.getClass().getName(), result);
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    public double getResultFloatNumber() {
        if (result == null) {
            return 0d;
        }
        if (result instanceof Double) {
            return (Double) result;
        }
        if (result instanceof Float) {
            return ((Float) result).doubleValue();
        }
        if (result instanceof Long) {
            return ((Long) result).doubleValue();
        }
        if (result instanceof Integer) {
            return ((Integer) result).doubleValue();
        }
        if (result instanceof String) {
            return Double.parseDouble((String) result);
        }
        String message = String.format("Cannot convert to Float type %s value %s", result.getClass().getName(), result);
        log.error(message);
        throw new UnsupportedArgumentTypeException(message);
    }

    public boolean isResultString() {
        return result != null && result instanceof String;
    }

    public boolean isResultFixedNumber() {
        return result != null && (
            result instanceof Long ||
                result instanceof Integer);
    }

    public boolean isResultFloatNumber() {
        return result != null && (
            result instanceof Double ||
                result instanceof Float);
    }

    public boolean isResultDateTime() {
        return result != null && (
            result instanceof Instant ||
                result instanceof java.sql.Timestamp);
    }

    public boolean isResultTime() {
        return result != null && (
            result instanceof java.sql.Time);
    }

    public boolean isResultDate() {
        return result != null && (
            result instanceof java.sql.Date);
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
    public void visit(SignedExpression signedExpression) {
        log.trace("[EvaluateExpressionVisitor] SignedExpression");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        log.warn("[EvaluateExpressionVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        log.warn("[EvaluateExpressionVisitor] JdbcNamedParameter");
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
        if (left.result instanceof java.sql.Date) {
            LocalDate instant = ((java.sql.Date) left.result).toLocalDate();
            instant = instant.plus(changeDays);
            result = java.sql.Date.valueOf(instant);
        }
        if (left.result instanceof Instant instant) {
            instant = instant.plus(changeDays);
            instant = instant.plus(changeSec);
            result = instant;
        }
        if (left.result instanceof java.sql.Timestamp) {
            Instant instant = ((java.sql.Timestamp) left.result).toInstant();
            instant = instant.plus(changeDays);
            instant = instant.plus(changeSec);
            result = new java.sql.Timestamp(instant.toEpochMilli());
        }
        if (left.result instanceof Time) {
            LocalTime instant = ((java.sql.Time) left.result).toLocalTime();
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

        if (!subEvaluatorLeft.isResultNull() && !subEvaluatorRight.isResultNull()) {
            if ((subEvaluatorLeft.isResultDateTime() || subEvaluatorLeft.isResultDate()
                || subEvaluatorLeft.isResultTime()) &&
                (subEvaluatorRight.isResultFloatNumber() || subEvaluatorRight.isResultFixedNumber())) {
                result = processDateNumberOperation(subEvaluatorLeft, subEvaluatorRight, true);
                return;
            }
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

        if (!subEvaluatorLeft.isResultNull() && !subEvaluatorRight.isResultNull()) {
            if ((subEvaluatorLeft.isResultDateTime() || subEvaluatorLeft.isResultDate()
                || subEvaluatorLeft.isResultTime()) &&
                (subEvaluatorRight.isResultFloatNumber() || subEvaluatorRight.isResultFixedNumber())) {
                result = processDateNumberOperation(subEvaluatorLeft, subEvaluatorRight, false);
                return;
            }
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
    public void visit(AndExpression andExpression) {
        log.warn("[EvaluateExpressionVisitor] AndExpression");
    }

    @Override
    public void visit(OrExpression orExpression) {
        log.warn("[EvaluateExpressionVisitor] OrExpression");
    }

    @Override
    public void visit(Between between) {
        log.warn("[EvaluateExpressionVisitor] Between");
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        log.warn("[EvaluateExpressionVisitor] EqualsTo");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        log.warn("[EvaluateExpressionVisitor] GreaterThan");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        log.warn("[EvaluateExpressionVisitor] GreaterThanEquals");
    }

    @Override
    public void visit(InExpression inExpression) {
        log.warn("[EvaluateExpressionVisitor] InExpression");
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        log.warn("[EvaluateExpressionVisitor] FullTextSearch");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        log.warn("[EvaluateExpressionVisitor] IsNullExpression");
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        log.warn("[EvaluateExpressionVisitor] IsBooleanExpression");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        log.warn("[EvaluateExpressionVisitor] LikeExpression");
    }

    @Override
    public void visit(MinorThan minorThan) {
        log.warn("[EvaluateExpressionVisitor] MinorThan");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        log.warn("[EvaluateExpressionVisitor] MinorThanEquals");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        log.warn("[EvaluateExpressionVisitor] NotEqualsTo");
    }

    @Override
    public void visit(Column tableColumn) {
        log.trace("[EvaluateExpressionVisitor] Column column={}", tableColumn.getColumnName());
        result = recordFieldsFromDB.get(tableColumn.getColumnName());
    }

    @Override
    public void visit(SubSelect subSelect) {
        log.warn("[VtoxSVisitor] SubSelect={}", subSelect.toString());
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        log.trace("[EvaluateExpressionVisitor] CaseExpression");
        EvaluateExpressionVisitor caseEvaluatorLeft = subVisitor();
        caseExpression.getSwitchExpression().accept(caseEvaluatorLeft);
    }

    @Override
    public void visit(WhenClause whenClause) {
        log.warn("[EvaluateExpressionVisitor] WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        log.warn("[EvaluateExpressionVisitor] ExistsExpression");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        log.warn("[EvaluateExpressionVisitor] AllComparisonExpression");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        log.warn("[EvaluateExpressionVisitor] AnyComparisonExpression");
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

    @Override
    public void visit(Matches matches) {
        log.warn("[EvaluateExpressionVisitor] Matches");
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        log.warn("[EvaluateExpressionVisitor] BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        log.warn("[EvaluateExpressionVisitor] BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        log.warn("[EvaluateExpressionVisitor] BitwiseXor");
    }

    @Override
    public void visit(CastExpression cast) {
        log.warn("[EvaluateExpressionVisitor] CastExpression");
    }

    @Override
    public void visit(Modulo modulo) {
        log.warn("[EvaluateExpressionVisitor] Modulo");
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        log.warn("[EvaluateExpressionVisitor] AnalyticExpression");
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        log.warn("[EvaluateExpressionVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        log.warn("[EvaluateExpressionVisitor] IntervalExpression");
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        log.warn("[EvaluateExpressionVisitor] OracleHierarchicalExpression");
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        log.warn("[EvaluateExpressionVisitor] RegExpMatchOperator");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        log.warn("[EvaluateExpressionVisitor] JsonExpression");
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        log.warn("[EvaluateExpressionVisitor] JsonOperator");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        log.warn("[EvaluateExpressionVisitor] RegExpMySQLOperator");
    }

    @Override
    public void visit(UserVariable var) {
        log.warn("[EvaluateExpressionVisitor] UserVariable");
    }

    @Override
    public void visit(NumericBind bind) {
        log.warn("[EvaluateExpressionVisitor] NumericBind");
    }

    @Override
    public void visit(KeepExpression aexpr) {
        log.warn("[EvaluateExpressionVisitor] KeepExpression");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        log.warn("[EvaluateExpressionVisitor] MySQLGroupConcat");
    }

    @Override
    public void visit(ValueListExpression valueList) {
        log.warn("[EvaluateExpressionVisitor] ValueListExpression");
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        log.warn("[EvaluateExpressionVisitor] RowConstructor");
    }

    @Override
    public void visit(OracleHint hint) {
        log.warn("[EvaluateExpressionVisitor] OracleHint");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        log.warn("[EvaluateExpressionVisitor] TimeKeyExpression");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        log.warn("[EvaluateExpressionVisitor] DateTimeLiteralExpression");
    }

    @Override
    public void visit(NotExpression aThis) {
        log.warn("[EvaluateExpressionVisitor] NotExpression");
    }

    @Override
    public void visit(NextValExpression aThis) {
        log.warn("[EvaluateExpressionVisitor] NextValExpression");
    }

    @Override
    public void visit(CollateExpression aThis) {
        log.warn("[EvaluateExpressionVisitor] CollateExpression");
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        log.warn("[EvaluateExpressionVisitor] SimilarToExpression");
    }

    @Override
    public void visit(ArrayExpression aThis) {
        log.warn("[EvaluateExpressionVisitor] ArrayExpression");
    }

    @Override
    public void visit(VariableAssignment aThis) {
        log.warn("[EvaluateExpressionVisitor] VariableAssignment");
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        log.warn("[EvaluateExpressionVisitor] XMLSerializeExpr");
    }
}
