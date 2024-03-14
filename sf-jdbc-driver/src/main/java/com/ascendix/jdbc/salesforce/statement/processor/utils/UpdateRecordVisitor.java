package com.ascendix.jdbc.salesforce.statement.processor.utils;

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
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
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
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
import net.sf.jsqlparser.statement.update.Update;

@Slf4j
public class UpdateRecordVisitor extends ExpressionVisitorAdapter {

    private final Update updateStatement;
    private final Map<String, Object> recordFieldsToUpdate;
    private final Map<String, Object> recordFieldsFromDB;
    private final String columnName;
    private final java.util.function.Function<String, List<Map<String, Object>>> subSelectResolver;

    public UpdateRecordVisitor(Update updateStatement, Map<String, Object> recordFieldsToUpdate,
        Map<String, Object> recordFieldsFromDB, String columnName,
        java.util.function.Function<String, List<Map<String, Object>>> subSelectResolver) {
        this.updateStatement = updateStatement;
        this.recordFieldsToUpdate = recordFieldsToUpdate;
        this.recordFieldsFromDB = recordFieldsFromDB;
        this.columnName = columnName;
        this.subSelectResolver = subSelectResolver;
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

    EvaluateExpressionVisitor evaluator() {
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
    public void visit(SubSelect subSelect) {
        log.trace("[VtoxSVisitor] SubSelect={} column={}", subSelect.toString(), columnName);
//        Object value = null;
//        if (subSelectResolver != null) {
//            subSelect.setUseBrackets(false);
//            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
//            if (records.size() == 1 && records.get(0).size() == 1) {
//                 return the value as plain value
//                value = records.get(0).entrySet().iterator().next().getValue();
//                log.trace("[UpdateVisitor] resolved to {}", value);
//            }
//        } else {
//            log.trace("[UpdateVisitor] subSelectResolver is undefined");
//        }
//        recordFieldsToUpdate.put(columnName, value);
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
    public void visit(RegExpMySQLOperator aThis) {
        log.trace("[UpdateVisitor] RegExpMySQLOperator");
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
    public void visit(ValueListExpression aThis) {
        log.trace("[UpdateVisitor] ValueListExpression");
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
