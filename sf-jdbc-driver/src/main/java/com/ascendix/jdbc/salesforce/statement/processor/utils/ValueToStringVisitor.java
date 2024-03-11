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

public class ValueToStringVisitor implements ExpressionVisitor {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

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
    public void visit(BitwiseRightShift aThis) {
        logger.warning("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        logger.warning("[VtoSVisitor] BitwiseLeftShift");
    }

    @Override
    public void visit(NullValue nullValue) {
        logger.finest("[VtoSVisitor] NullValue");
        fieldValues.put(columnName, null);
    }

    @Override
    public void visit(Function function) {
        logger.warning("[VtoSVisitor] Function");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        logger.warning("[VtoSVisitor] SignedExpression");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        logger.warning("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        logger.warning("[VtoSVisitor] JdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        logger.warning("[VtoSVisitor] DoubleValue=" + doubleValue.getValue());
        fieldValues.put(columnName, doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        logger.warning("[VtoSVisitor] LongValue=" + longValue.getValue());
        fieldValues.put(columnName, longValue.getValue());
    }

    @Override
    public void visit(HexValue hexValue) {
        logger.warning("[VtoSVisitor] HexValue=" + hexValue.getValue());
        fieldValues.put(columnName, hexValue.getValue());
    }

    @Override
    public void visit(DateValue dateValue) {
        logger.warning("[VtoSVisitor] DateValue");
    }

    @Override
    public void visit(TimeValue timeValue) {
        logger.warning("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        logger.warning("[VtoSVisitor] TimestampValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        logger.warning("[VtoSVisitor] Parenthesis");
    }

    @Override
    public void visit(StringValue stringValue) {
        logger.finest("[VtoSVisitor] StringValue=" + stringValue.getValue());
        fieldValues.put(columnName, stringValue.getValue());
    }

    @Override
    public void visit(Addition addition) {
        logger.warning("[VtoSVisitor] Addition");
    }

    @Override
    public void visit(Division division) {
        logger.warning("[VtoSVisitor] Division");
    }

    @Override
    public void visit(IntegerDivision division) {
        logger.warning("[VtoSVisitor] IntegerDivision");
    }

    @Override
    public void visit(Multiplication multiplication) {
        logger.warning("[VtoSVisitor] Multiplication");
    }

    @Override
    public void visit(Subtraction subtraction) {
        logger.warning("[VtoSVisitor] Subtraction");
    }

    @Override
    public void visit(AndExpression andExpression) {
        logger.warning("[VtoSVisitor] AndExpression");
    }

    @Override
    public void visit(OrExpression orExpression) {
        logger.warning("[VtoSVisitor] OrExpression");
    }

    @Override
    public void visit(Between between) {
        logger.warning("[VtoSVisitor] Between");
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        logger.warning("[VtoSVisitor] EqualsTo");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        logger.warning("[VtoSVisitor] GreaterThan");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        logger.warning("[VtoSVisitor] GreaterThanEquals");
    }

    @Override
    public void visit(InExpression inExpression) {
        logger.warning("[VtoSVisitor] InExpression");
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        logger.warning("[VtoSVisitor] FullTextSearch");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        logger.warning("[VtoSVisitor] IsNullExpression");
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        logger.warning("[VtoSVisitor] IsBooleanExpression");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        logger.warning("[VtoSVisitor] LikeExpression");
    }

    @Override
    public void visit(MinorThan minorThan) {
        logger.warning("[VtoSVisitor] MinorThan");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        logger.warning("[VtoSVisitor] MinorThanEquals");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        logger.warning("[VtoSVisitor] NotEqualsTo");
    }

    @Override
    public void visit(Column tableColumn) {
        logger.warning("[VtoSVisitor] Column");
    }

    @Override
    public void visit(SubSelect subSelect) {
        logger.finest("[VtoxSVisitor] SubSelect=" + subSelect.toString());
        Object value = null;
        if (subSelectResolver != null) {
            subSelect.setUseBrackets(false);
            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
            if (records.size() == 1 && records.get(0).size() == 1) {
                // return the value as plain value
                value = records.get(0).entrySet().iterator().next().getValue();
                logger.finest("[VtoSVisitor] resolved to " + value);
            }
        } else {
            logger.finest("[VtoSVisitor] subSelectResolver is undefined");
        }
        fieldValues.put(columnName, value);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        logger.warning("[VtoSVisitor] CaseExpression");
    }

    @Override
    public void visit(WhenClause whenClause) {
        logger.warning("[VtoSVisitor] WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        logger.warning("[VtoSVisitor] ExistsExpression");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        logger.warning("[VtoSVisitor] AllComparisonExpression");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        logger.warning("[VtoSVisitor] AnyComparisonExpression");
    }

    @Override
    public void visit(Concat concat) {
        logger.warning("[VtoSVisitor] Concat");
    }

    @Override
    public void visit(Matches matches) {
        logger.warning("[VtoSVisitor] Matches");
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        logger.warning("[VtoSVisitor] BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        logger.warning("[VtoSVisitor] BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        logger.warning("[VtoSVisitor] BitwiseXor");
    }

    @Override
    public void visit(CastExpression cast) {
        logger.warning("[VtoSVisitor] CastExpression");
    }

    @Override
    public void visit(Modulo modulo) {
        logger.warning("[VtoSVisitor] Modulo");
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        logger.warning("[VtoSVisitor] AnalyticExpression");
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        logger.warning("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        logger.warning("[VtoSVisitor] IntervalExpression");
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        logger.warning("[VtoSVisitor] OracleHierarchicalExpression");
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        logger.warning("[VtoSVisitor] RegExpMatchOperator");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        logger.warning("[VtoSVisitor] JsonExpression");
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        logger.warning("[VtoSVisitor] JsonOperator");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        logger.warning("[VtoSVisitor] RegExpMySQLOperator");
    }

    @Override
    public void visit(UserVariable var) {
        logger.warning("[VtoSVisitor] UserVariable");
    }

    @Override
    public void visit(NumericBind bind) {
        logger.warning("[VtoSVisitor] NumericBind");
    }

    @Override
    public void visit(KeepExpression aexpr) {
        logger.warning("[VtoSVisitor] KeepExpression");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        logger.warning("[VtoSVisitor] MySQLGroupConcat");
    }

    @Override
    public void visit(ValueListExpression valueList) {
        logger.warning("[VtoSVisitor] ValueListExpression");
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        logger.warning("[VtoSVisitor] RowConstructor");
    }

    @Override
    public void visit(OracleHint hint) {
        logger.warning("[VtoSVisitor] OracleHint");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        logger.warning("[VtoSVisitor] TimeKeyExpression");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        logger.warning("[VtoSVisitor] DateTimeLiteralExpression");
    }

    @Override
    public void visit(NotExpression aThis) {
        logger.warning("[VtoSVisitor] NotExpression");
    }

    @Override
    public void visit(NextValExpression aThis) {
        logger.warning("[VtoSVisitor] NextValExpression");
    }

    @Override
    public void visit(CollateExpression aThis) {
        logger.warning("[VtoSVisitor] CollateExpression");
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        logger.warning("[VtoSVisitor] SimilarToExpression");
    }

    @Override
    public void visit(ArrayExpression aThis) {
        logger.warning("[VtoSVisitor] ArrayExpression");
    }

    @Override
    public void visit(VariableAssignment aThis) {
        logger.warning("[VtoSVisitor] VariableAssignment");
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        logger.warning("[VtoSVisitor] XMLSerializeExpr");
    }
}
