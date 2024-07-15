package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.utils.ParamUtils;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
public class UpdateRecordVisitor extends ExpressionVisitorAdapter<Expression> {

    private final Map<String, Object> recordFieldsToUpdate;
    private final Map<String, Object> recordFieldsFromDB;
    private final String columnName;
    private final List<Object> parameters;

    public UpdateRecordVisitor(Map<String, Object> recordFieldsToUpdate,
                               Map<String, Object> recordFieldsFromDB, String columnName, List<Object> parameters) {
        this.recordFieldsToUpdate = recordFieldsToUpdate;
        this.recordFieldsFromDB = recordFieldsFromDB;
        this.columnName = columnName;
        this.parameters = parameters;
    }

    @Override
    public <S> Expression visit(NullValue nullValue, S context) {
        log.trace("[UpdateVisitor] NullValue column={}", columnName);
        recordFieldsToUpdate.put(columnName, null);
        return null;
    }

    @Override
    public <S> Expression visit(DoubleValue doubleValue, S context) {
        log.trace("[UpdateVisitor] DoubleValue={} column={}", doubleValue.getValue(), columnName);
        recordFieldsToUpdate.put(columnName, doubleValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(LongValue longValue, S context) {
        log.trace("[UpdateVisitor] LongValue={} column={}", columnName, longValue.getValue());
        recordFieldsToUpdate.put(columnName, longValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(HexValue hexValue, S context) {
        log.trace("[UpdateVisitor] HexValue={} column={}", hexValue.getValue(), columnName);
        recordFieldsToUpdate.put(columnName, hexValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(TimeValue timeValue, S context) {
        log.trace("[UpdateVisitor] BitwiseRightShift");
        recordFieldsToUpdate.put(columnName, timeValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(TimestampValue timestampValue, S context) {
        log.trace("[UpdateVisitor] TimestampValue");
        recordFieldsToUpdate.put(columnName, timestampValue.getValue());
        return null;
    }

    private EvaluateExpressionVisitor evaluator() {
        return new EvaluateExpressionVisitor(recordFieldsFromDB);
    }

    private void evaluate(Expression expr) {
        EvaluateExpressionVisitor subEvaluatorLeft = evaluator();
        expr.accept(subEvaluatorLeft);
        recordFieldsToUpdate.put(columnName, subEvaluatorLeft.getResult());
    }

    @Override
    public <S> Expression visit(StringValue stringValue, S context) {
        log.trace("[UpdateVisitor] StringValue={} column={}", stringValue.getValue(), columnName);
        recordFieldsToUpdate.put(columnName, stringValue.getValue());
        return null;
    }

    @Override
    public <S> Expression visit(Addition addition, S context) {
        log.trace("[UpdateVisitor] Addition");
        evaluate(addition);
        return null;
    }

    @Override
    public <S> Expression visit(Division division, S context) {
        log.trace("[UpdateVisitor] Division");
        evaluate(division);
        return null;
    }

    @Override
    public <S> Expression visit(IntegerDivision division, S context) {
        log.trace("[UpdateVisitor] IntegerDivision");
        evaluate(division);
        return null;
    }

    @Override
    public <S> Expression visit(Multiplication multiplication, S context) {
        log.trace("[UpdateVisitor] Multiplication");
        evaluate(multiplication);
        return null;
    }

    @Override
    public <S> Expression visit(Subtraction subtraction, S context) {
        log.trace("[UpdateVisitor] Subtraction");
        evaluate(subtraction);
        return null;
    }

    @Override
    public <S> Expression visit(AndExpression andExpression, S context) {
        log.trace("[UpdateVisitor] AndExpression");
        evaluate(andExpression);
        return null;
    }

    @Override
    public <S> Expression visit(OrExpression orExpression, S context) {
        log.trace("[UpdateVisitor] OrExpression");
        evaluate(orExpression);
        return null;
    }

    @Override
    public <S> Expression visit(Between between, S context) {
        log.trace("[UpdateVisitor] Between");
        evaluate(between);
        return null;
    }

    @Override
    public <S> Expression visit(EqualsTo equalsTo, S context) {
        log.trace("[UpdateVisitor] EqualsTo");
        evaluate(equalsTo);
        return null;
    }

    @Override
    public <S> Expression visit(GreaterThan greaterThan, S context) {
        log.trace("[UpdateVisitor] GreaterThan");
        evaluate(greaterThan);
        return null;
    }

    @Override
    public <S> Expression visit(GreaterThanEquals greaterThanEquals, S context) {
        log.trace("[UpdateVisitor] GreaterThanEquals");
        evaluate(greaterThanEquals);
        return null;
    }

    @Override
    public <S> Expression visit(InExpression inExpression, S context) {
        log.trace("[UpdateVisitor] InExpression");
        evaluate(inExpression);
        return null;
    }

    @Override
    public <S> Expression visit(FullTextSearch fullTextSearch, S context) {
        log.trace("[UpdateVisitor] FullTextSearch");
        evaluate(fullTextSearch);
        return null;
    }

    @Override
    public <S> Expression visit(IsNullExpression isNullExpression, S context) {
        log.trace("[UpdateVisitor] IsNullExpression");
        evaluate(isNullExpression);
        return null;
    }

    @Override
    public <S> Expression visit(IsBooleanExpression isBooleanExpression, S context) {
        log.trace("[UpdateVisitor] IsBooleanExpression");
        evaluate(isBooleanExpression);
        return null;
    }

    @Override
    public <S> Expression visit(LikeExpression likeExpression, S context) {
        log.trace("[UpdateVisitor] LikeExpression");
        evaluate(likeExpression);
        return null;
    }

    @Override
    public <S> Expression visit(MinorThan minorThan, S context) {
        log.trace("[UpdateVisitor] MinorThan");
        evaluate(minorThan);
        return null;
    }

    @Override
    public <S> Expression visit(MinorThanEquals minorThanEquals, S context) {
        log.trace("[UpdateVisitor] MinorThanEquals");
        evaluate(minorThanEquals);
        return null;
    }

    @Override
    public <S> Expression visit(NotEqualsTo notEqualsTo, S context) {
        log.trace("[UpdateVisitor] NotEqualsTo");
        evaluate(notEqualsTo);
        return null;
    }

    @Override
    public <S> Expression visit(Column tableColumn, S context) {
        log.trace("[UpdateVisitor] Column column={}", tableColumn.getColumnName());
        evaluate(tableColumn);
        return null;
    }

    @Override
    public <S> Expression visit(CaseExpression caseExpression, S context) {
        log.trace("[UpdateVisitor] CaseExpression");
        evaluate(caseExpression);
        return null;
    }

    @Override
    public <S> Expression visit(WhenClause whenClause, S context) {
        log.trace("[UpdateVisitor] WhenClause");
        evaluate(whenClause);
        return null;
    }

    @Override
    public <S> Expression visit(ExistsExpression existsExpression, S context) {
        log.trace("[UpdateVisitor] ExistsExpression");
        evaluate(existsExpression);
        return null;
    }

    @Override
    public <S> Expression visit(AnyComparisonExpression anyComparisonExpression, S context) {
        log.trace("[UpdateVisitor] AnyComparisonExpression");
        evaluate(anyComparisonExpression);
        return null;
    }

    @Override
    public <S> Expression visit(Concat concat, S context) {
        log.trace("[UpdateVisitor] Concat");
        evaluate(concat);
        return null;
    }

    @Override
    public <S> Expression visit(Matches matches, S context) {
        log.trace("[UpdateVisitor] Matches");
        evaluate(matches);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseAnd bitwiseAnd, S context) {
        log.trace("[UpdateVisitor] BitwiseAnd");
        evaluate(bitwiseAnd);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseOr aThis, S context) {
        log.trace("[UpdateVisitor] BitwiseOr");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseXor aThis, S context) {
        log.trace("[UpdateVisitor] BitwiseXor");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(CastExpression aThis, S context) {
        log.trace("[UpdateVisitor] CastExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(Modulo aThis, S context) {
        log.trace("[UpdateVisitor] Modulo");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(AnalyticExpression aThis, S context) {
        log.trace("[UpdateVisitor] AnalyticExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(ExtractExpression aThis, S context) {
        log.trace("[UpdateVisitor] BitwiseRightShift");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(IntervalExpression aThis, S context) {
        log.trace("[UpdateVisitor] IntervalExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(OracleHierarchicalExpression aThis, S context) {
        log.trace("[UpdateVisitor] OracleHierarchicalExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(RegExpMatchOperator aThis, S context) {
        log.trace("[UpdateVisitor] RegExpMatchOperator");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(JsonExpression aThis, S context) {
        log.trace("[UpdateVisitor] JsonExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(JsonOperator aThis, S context) {
        log.trace("[UpdateVisitor] JsonOperator");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(UserVariable aThis, S context) {
        log.trace("[UpdateVisitor] UserVariable");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(NumericBind aThis, S context) {
        log.trace("[UpdateVisitor] NumericBind");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(KeepExpression aThis, S context) {
        log.trace("[UpdateVisitor] KeepExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(MySQLGroupConcat aThis, S context) {
        log.trace("[UpdateVisitor] MySQLGroupConcat");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(RowConstructor<? extends Expression> aThis, S context) {
        log.trace("[UpdateVisitor] RowConstructor");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(OracleHint aThis, S context) {
        log.trace("[UpdateVisitor] OracleHint");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(TimeKeyExpression aThis, S context) {
        log.trace("[UpdateVisitor] TimeKeyExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(DateTimeLiteralExpression aThis, S context) {
        log.trace("[UpdateVisitor] DateTimeLiteralExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(NotExpression aThis, S context) {
        log.trace("[UpdateVisitor] NotExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(NextValExpression aThis, S context) {
        log.trace("[UpdateVisitor] NextValExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(CollateExpression aThis, S context) {
        log.trace("[UpdateVisitor] CollateExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(SimilarToExpression aThis, S context) {
        log.trace("[UpdateVisitor] SimilarToExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(ArrayExpression aThis, S context) {
        log.trace("[UpdateVisitor] ArrayExpression");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(VariableAssignment aThis, S context) {
        log.trace("[UpdateVisitor] VariableAssignment");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(XMLSerializeExpr aThis, S context) {
        log.trace("[UpdateVisitor] XMLSerializeExpr");
        evaluate(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(JdbcParameter parameter, S context) {
        int idx = parameter.getIndex() - 1;
        Object o = parameters.get(idx);
        recordFieldsToUpdate.put(columnName, getParameter(o));
        return null;
    }

    private static Object getParameter(Object o) {
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
        } else {
            return o;
        }
    }
}
