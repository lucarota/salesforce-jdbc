package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.ForceDriver;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
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
import net.sf.jsqlparser.expression.Expression;
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
import net.sf.jsqlparser.statement.update.Update;

public class UpdateRecordVisitor implements ExpressionVisitor {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

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
    public void visit(BitwiseRightShift aThis) {
        logger.warning("[UpdateVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        logger.warning("[UpdateVisitor] BitwiseLeftShift");
    }

    @Override
    public void visit(NullValue nullValue) {
        logger.finest("[UpdateVisitor] NullValue column=" + columnName);
        recordFieldsToUpdate.put(columnName, null);
    }

    @Override
    public void visit(Function function) {
        logger.warning("[UpdateVisitor] Function function=" + function.getName());
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        logger.warning("[UpdateVisitor] SignedExpression");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        logger.warning("[UpdateVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        logger.warning("[UpdateVisitor] JdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        logger.finest("[UpdateVisitor] DoubleValue=" + doubleValue.getValue() + " column=" + columnName);
        recordFieldsToUpdate.put(columnName, doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        logger.finest("[UpdateVisitor] LongValue=" + longValue.getValue() + " column=" + columnName);
        recordFieldsToUpdate.put(columnName, longValue.getValue());
    }

    @Override
    public void visit(HexValue hexValue) {
        logger.finest("[UpdateVisitor] HexValue=" + hexValue.getValue() + " column=" + columnName);
        recordFieldsToUpdate.put(columnName, hexValue.getValue());
    }

    @Override
    public void visit(DateValue dateValue) {
        logger.warning("[UpdateVisitor] DateValue column=" + columnName);
    }

    @Override
    public void visit(TimeValue timeValue) {
        logger.finest("[UpdateVisitor] BitwiseRightShift");
        recordFieldsToUpdate.put(columnName, timeValue.getValue());
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        logger.finest("[UpdateVisitor] TimestampValue");
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
        logger.finest("[UpdateVisitor] Parenthesis");
        evaluate(parenthesis);
    }

    @Override
    public void visit(StringValue stringValue) {
        logger.finest("[UpdateVisitor] StringValue=" + stringValue.getValue() + " column=" + columnName);
        recordFieldsToUpdate.put(columnName, stringValue.getValue());
    }

    @Override
    public void visit(Addition addition) {
        logger.finest("[UpdateVisitor] Addition");
        evaluate(addition);
    }

    @Override
    public void visit(Division division) {
        logger.finest("[UpdateVisitor] Division");
        evaluate(division);
    }

    @Override
    public void visit(IntegerDivision division) {
        logger.finest("[UpdateVisitor] IntegerDivision");
        evaluate(division);
    }

    @Override
    public void visit(Multiplication multiplication) {
        logger.finest("[UpdateVisitor] Multiplication");
        evaluate(multiplication);
    }

    @Override
    public void visit(Subtraction subtraction) {
        logger.finest("[UpdateVisitor] Subtraction");
        evaluate(subtraction);
    }

    @Override
    public void visit(AndExpression andExpression) {
        logger.finest("[UpdateVisitor] AndExpression");
        evaluate(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        logger.finest("[UpdateVisitor] OrExpression");
        evaluate(orExpression);
    }

    @Override
    public void visit(Between between) {
        logger.finest("[UpdateVisitor] Between");
        evaluate(between);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        logger.finest("[UpdateVisitor] EqualsTo");
        evaluate(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        logger.finest("[UpdateVisitor] GreaterThan");
        evaluate(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        logger.finest("[UpdateVisitor] GreaterThanEquals");
        evaluate(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        logger.finest("[UpdateVisitor] InExpression");
        evaluate(inExpression);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        logger.finest("[UpdateVisitor] FullTextSearch");
        evaluate(fullTextSearch);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        logger.finest("[UpdateVisitor] IsNullExpression");
        evaluate(isNullExpression);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        logger.finest("[UpdateVisitor] IsBooleanExpression");
        evaluate(isBooleanExpression);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        logger.finest("[UpdateVisitor] LikeExpression");
        evaluate(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        logger.finest("[UpdateVisitor] MinorThan");
        evaluate(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        logger.finest("[UpdateVisitor] MinorThanEquals");
        evaluate(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        logger.finest("[UpdateVisitor] NotEqualsTo");
        evaluate(notEqualsTo);
    }

    @Override
    public void visit(Column tableColumn) {
        logger.finest("[UpdateVisitor] Column column=" + tableColumn.getColumnName());
        evaluate(tableColumn);
    }

    @Override
    public void visit(SubSelect subSelect) {
        logger.finest("[VtoxSVisitor] SubSelect=" + subSelect.toString() + " column=" + columnName);
//        Object value = null;
//        if (subSelectResolver != null) {
//            subSelect.setUseBrackets(false);
//            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
//            if (records.size() == 1 && records.get(0).size() == 1) {
//                 return the value as plain value
//                value = records.get(0).entrySet().iterator().next().getValue();
//                logger.finest("[UpdateVisitor] resolved to "+value);
//            }
//        } else {
//            logger.finest("[UpdateVisitor] subSelectResolver is undefined");
//        }
//        recordFieldsToUpdate.put(columnName, value);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        logger.finest("[UpdateVisitor] CaseExpression");
        evaluate(caseExpression);
    }

    @Override
    public void visit(WhenClause whenClause) {
        logger.finest("[UpdateVisitor] WhenClause");
        evaluate(whenClause);
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        logger.finest("[UpdateVisitor] ExistsExpression");
        evaluate(existsExpression);
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        logger.finest("[UpdateVisitor] AllComparisonExpression");
        evaluate(allComparisonExpression);
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        logger.finest("[UpdateVisitor] AnyComparisonExpression");
        evaluate(anyComparisonExpression);
    }

    @Override
    public void visit(Concat concat) {
        logger.finest("[UpdateVisitor] Concat");
        evaluate(concat);
    }

    @Override
    public void visit(Matches matches) {
        logger.finest("[UpdateVisitor] Matches");
        evaluate(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        logger.finest("[UpdateVisitor] BitwiseAnd");
        evaluate(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr aThis) {
        logger.finest("[UpdateVisitor] BitwiseOr");
        evaluate(aThis);
    }

    @Override
    public void visit(BitwiseXor aThis) {
        logger.finest("[UpdateVisitor] BitwiseXor");
        evaluate(aThis);
    }

    @Override
    public void visit(CastExpression aThis) {
        logger.finest("[UpdateVisitor] CastExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(Modulo aThis) {
        logger.finest("[UpdateVisitor] Modulo");
        evaluate(aThis);
    }

    @Override
    public void visit(AnalyticExpression aThis) {
        logger.finest("[UpdateVisitor] AnalyticExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(ExtractExpression aThis) {
        logger.finest("[UpdateVisitor] BitwiseRightShift");
        evaluate(aThis);
    }

    @Override
    public void visit(IntervalExpression aThis) {
        logger.finest("[UpdateVisitor] IntervalExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(OracleHierarchicalExpression aThis) {
        logger.finest("[UpdateVisitor] OracleHierarchicalExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(RegExpMatchOperator aThis) {
        logger.finest("[UpdateVisitor] RegExpMatchOperator");
        evaluate(aThis);
    }

    @Override
    public void visit(JsonExpression aThis) {
        logger.finest("[UpdateVisitor] JsonExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(JsonOperator aThis) {
        logger.finest("[UpdateVisitor] JsonOperator");
        evaluate(aThis);
    }

    @Override
    public void visit(RegExpMySQLOperator aThis) {
        logger.finest("[UpdateVisitor] RegExpMySQLOperator");
        evaluate(aThis);
    }

    @Override
    public void visit(UserVariable aThis) {
        logger.finest("[UpdateVisitor] UserVariable");
        evaluate(aThis);
    }

    @Override
    public void visit(NumericBind aThis) {
        logger.finest("[UpdateVisitor] NumericBind");
        evaluate(aThis);
    }

    @Override
    public void visit(KeepExpression aThis) {
        logger.finest("[UpdateVisitor] KeepExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(MySQLGroupConcat aThis) {
        logger.finest("[UpdateVisitor] MySQLGroupConcat");
        evaluate(aThis);
    }

    @Override
    public void visit(ValueListExpression aThis) {
        logger.finest("[UpdateVisitor] ValueListExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(RowConstructor aThis) {
        logger.finest("[UpdateVisitor] RowConstructor");
        evaluate(aThis);
    }

    @Override
    public void visit(OracleHint aThis) {
        logger.finest("[UpdateVisitor] OracleHint");
        evaluate(aThis);
    }

    @Override
    public void visit(TimeKeyExpression aThis) {
        logger.finest("[UpdateVisitor] TimeKeyExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(DateTimeLiteralExpression aThis) {
        logger.finest("[UpdateVisitor] DateTimeLiteralExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(NotExpression aThis) {
        logger.finest("[UpdateVisitor] NotExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(NextValExpression aThis) {
        logger.finest("[UpdateVisitor] NextValExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(CollateExpression aThis) {
        logger.finest("[UpdateVisitor] CollateExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        logger.finest("[UpdateVisitor] SimilarToExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(ArrayExpression aThis) {
        logger.finest("[UpdateVisitor] ArrayExpression");
        evaluate(aThis);
    }

    @Override
    public void visit(VariableAssignment aThis) {
        logger.finest("[UpdateVisitor] VariableAssignment");
        evaluate(aThis);
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        logger.finest("[UpdateVisitor] XMLSerializeExpr");
        evaluate(aThis);
    }
}
