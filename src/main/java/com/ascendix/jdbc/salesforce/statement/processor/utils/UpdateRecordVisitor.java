package com.ascendix.jdbc.salesforce.statement.processor.utils;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.Map;

@Slf4j
public class UpdateRecordVisitor extends ExpressionVisitorAdapter {

    private final Map<String, Object> recordFieldsToUpdate;
    private final Map<String, Object> recordFieldsFromDB;
    private final String columnName;

    public UpdateRecordVisitor(Map<String, Object> recordFieldsToUpdate,
        Map<String, Object> recordFieldsFromDB, String columnName) {
        this.recordFieldsToUpdate = recordFieldsToUpdate;
        this.recordFieldsFromDB = recordFieldsFromDB;
        this.columnName = columnName;
    }

    @Override
    public void visit(NullValue nullValue) {
        log.trace("[UpdateVisitor] NullValue column={}", columnName);
        recordFieldsToUpdate.put(columnName, null);
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        log.trace("[UpdateVisitor] DoubleValue={} column={}", doubleValue.getValue(), columnName);
        recordFieldsToUpdate.put(columnName, doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        log.trace("[UpdateVisitor] LongValue={} column={}", columnName,longValue.getValue());
        recordFieldsToUpdate.put(columnName, longValue.getValue());
    }

    @Override
    public void visit(HexValue hexValue) {
        log.trace("[UpdateVisitor] HexValue={} column={}", hexValue.getValue() , columnName);
        recordFieldsToUpdate.put(columnName, hexValue.getValue());
    }

    @Override
    public void visit(TimeValue timeValue) {
        log.trace("[UpdateVisitor] BitwiseRightShift");
        recordFieldsToUpdate.put(columnName, timeValue.getValue());
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        log.trace("[UpdateVisitor] TimestampValue");
        recordFieldsToUpdate.put(columnName, timestampValue.getValue());
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
    public void visit(Parenthesis parenthesis) {
        log.trace("[UpdateVisitor] Parenthesis");
        evaluate(parenthesis);
    }

    @Override
    public void visit(StringValue stringValue) {
        log.trace("[UpdateVisitor] StringValue={} column={}", stringValue.getValue(), columnName);
        recordFieldsToUpdate.put(columnName, stringValue.getValue());
    }

    @Override
    public void visit(Addition addition) {
        log.trace("[UpdateVisitor] Addition");
        evaluate(addition);
    }

    @Override
    public void visit(Division division) {
        log.trace("[UpdateVisitor] Division");
        evaluate(division);
    }

    @Override
    public void visit(IntegerDivision division) {
        log.trace("[UpdateVisitor] IntegerDivision");
        evaluate(division);
    }

    @Override
    public void visit(Multiplication multiplication) {
        log.trace("[UpdateVisitor] Multiplication");
        evaluate(multiplication);
    }

    @Override
    public void visit(Subtraction subtraction) {
        log.trace("[UpdateVisitor] Subtraction");
        evaluate(subtraction);
    }

    @Override
    public void visit(AndExpression andExpression) {
        log.trace("[UpdateVisitor] AndExpression");
        evaluate(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        log.trace("[UpdateVisitor] OrExpression");
        evaluate(orExpression);
    }

    @Override
    public void visit(Between between) {
        log.trace("[UpdateVisitor] Between");
        evaluate(between);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        log.trace("[UpdateVisitor] EqualsTo");
        evaluate(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        log.trace("[UpdateVisitor] GreaterThan");
        evaluate(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        log.trace("[UpdateVisitor] GreaterThanEquals");
        evaluate(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        log.trace("[UpdateVisitor] InExpression");
        evaluate(inExpression);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        log.trace("[UpdateVisitor] FullTextSearch");
        evaluate(fullTextSearch);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        log.trace("[UpdateVisitor] IsNullExpression");
        evaluate(isNullExpression);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        log.trace("[UpdateVisitor] IsBooleanExpression");
        evaluate(isBooleanExpression);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        log.trace("[UpdateVisitor] LikeExpression");
        evaluate(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        log.trace("[UpdateVisitor] MinorThan");
        evaluate(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        log.trace("[UpdateVisitor] MinorThanEquals");
        evaluate(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        log.trace("[UpdateVisitor] NotEqualsTo");
        evaluate(notEqualsTo);
    }

    @Override
    public void visit(Column tableColumn) {
        log.trace("[UpdateVisitor] Column column={}", tableColumn.getColumnName());
        evaluate(tableColumn);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        log.trace("[UpdateVisitor] CaseExpression");
        evaluate(caseExpression);
    }

    @Override
    public void visit(WhenClause whenClause) {
        log.trace("[UpdateVisitor] WhenClause");
        evaluate(whenClause);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        log.trace("[UpdateVisitor] ExistsExpression");
        evaluate(existsExpression);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        log.trace("[UpdateVisitor] AnyComparisonExpression");
        evaluate(anyComparisonExpression);
    }

    @Override
    public void visit(Concat concat) {
        log.trace("[UpdateVisitor] Concat");
        evaluate(concat);
    }

    @Override
    public void visit(Matches matches) {
        log.trace("[UpdateVisitor] Matches");
        evaluate(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        log.trace("[UpdateVisitor] BitwiseAnd");
        evaluate(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr aThis) {
        log.trace("[UpdateVisitor] BitwiseOr");
        evaluate(aThis);
    }

    @Override
    public void visit(BitwiseXor aThis) {
        log.trace("[UpdateVisitor] BitwiseXor");
        evaluate(aThis);
    }

    @Override
    public void visit(CastExpression aThis) {
        log.trace("[UpdateVisitor] CastExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(Modulo aThis) {
        log.trace("[UpdateVisitor] Modulo");
        evaluate(aThis);
    }

    @Override
    public void visit(AnalyticExpression aThis) {
        log.trace("[UpdateVisitor] AnalyticExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(ExtractExpression aThis) {
        log.trace("[UpdateVisitor] BitwiseRightShift");
        evaluate(aThis);
    }

    @Override
    public void visit(IntervalExpression aThis) {
        log.trace("[UpdateVisitor] IntervalExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(OracleHierarchicalExpression aThis) {
        log.trace("[UpdateVisitor] OracleHierarchicalExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(RegExpMatchOperator aThis) {
        log.trace("[UpdateVisitor] RegExpMatchOperator");
        evaluate(aThis);
    }

    @Override
    public void visit(JsonExpression aThis) {
        log.trace("[UpdateVisitor] JsonExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(JsonOperator aThis) {
        log.trace("[UpdateVisitor] JsonOperator");
        evaluate(aThis);
    }

    @Override
    public void visit(UserVariable aThis) {
        log.trace("[UpdateVisitor] UserVariable");
        evaluate(aThis);
    }

    @Override
    public void visit(NumericBind aThis) {
        log.trace("[UpdateVisitor] NumericBind");
        evaluate(aThis);
    }

    @Override
    public void visit(KeepExpression aThis) {
        log.trace("[UpdateVisitor] KeepExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(MySQLGroupConcat aThis) {
        log.trace("[UpdateVisitor] MySQLGroupConcat");
        evaluate(aThis);
    }

    @Override
    public void visit(RowConstructor aThis) {
        log.trace("[UpdateVisitor] RowConstructor");
        evaluate(aThis);
    }

    @Override
    public void visit(OracleHint aThis) {
        log.trace("[UpdateVisitor] OracleHint");
        evaluate(aThis);
    }

    @Override
    public void visit(TimeKeyExpression aThis) {
        log.trace("[UpdateVisitor] TimeKeyExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(DateTimeLiteralExpression aThis) {
        log.trace("[UpdateVisitor] DateTimeLiteralExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(NotExpression aThis) {
        log.trace("[UpdateVisitor] NotExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(NextValExpression aThis) {
        log.trace("[UpdateVisitor] NextValExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(CollateExpression aThis) {
        log.trace("[UpdateVisitor] CollateExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        log.trace("[UpdateVisitor] SimilarToExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(ArrayExpression aThis) {
        log.trace("[UpdateVisitor] ArrayExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(VariableAssignment aThis) {
        log.trace("[UpdateVisitor] VariableAssignment");
        evaluate(aThis);
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        log.trace("[UpdateVisitor] XMLSerializeExpr");
        evaluate(aThis);
    }
}
