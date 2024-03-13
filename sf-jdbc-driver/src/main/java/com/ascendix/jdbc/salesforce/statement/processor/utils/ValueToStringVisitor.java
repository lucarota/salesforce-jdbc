package com.ascendix.jdbc.salesforce.statement.processor.utils;

import java.util.List;
import java.util.Map;
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
public class ValueToStringVisitor implements ExpressionVisitor {

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
        log.warn("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        log.warn("[VtoSVisitor] BitwiseLeftShift");
    }

    @Override
    public void visit(NullValue nullValue) {
        log.trace("[VtoSVisitor] NullValue");
        fieldValues.put(columnName, null);
    }

    @Override
    public void visit(Function function) {
        log.warn("[VtoSVisitor] Function");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        log.warn("[VtoSVisitor] SignedExpression");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        log.warn("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        log.warn("[VtoSVisitor] JdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        log.warn("[VtoSVisitor] DoubleValue={}", doubleValue.getValue());
        fieldValues.put(columnName, doubleValue.getValue());
    }

    @Override
    public void visit(LongValue longValue) {
        log.warn("[VtoSVisitor] LongValue={}", longValue.getValue());
        fieldValues.put(columnName, longValue.getValue());
    }

    @Override
    public void visit(HexValue hexValue) {
        log.warn("[VtoSVisitor] HexValue={}", hexValue.getValue());
        fieldValues.put(columnName, hexValue.getValue());
    }

    @Override
    public void visit(DateValue dateValue) {
        log.warn("[VtoSVisitor] DateValue");
    }

    @Override
    public void visit(TimeValue timeValue) {
        log.warn("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        log.warn("[VtoSVisitor] TimestampValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        log.warn("[VtoSVisitor] Parenthesis");
    }

    @Override
    public void visit(StringValue stringValue) {
        log.trace("[VtoSVisitor] StringValue={}", stringValue.getValue());
        fieldValues.put(columnName, stringValue.getValue());
    }

    @Override
    public void visit(Addition addition) {
        log.warn("[VtoSVisitor] Addition");
    }

    @Override
    public void visit(Division division) {
        log.warn("[VtoSVisitor] Division");
    }

    @Override
    public void visit(IntegerDivision division) {
        log.warn("[VtoSVisitor] IntegerDivision");
    }

    @Override
    public void visit(Multiplication multiplication) {
        log.warn("[VtoSVisitor] Multiplication");
    }

    @Override
    public void visit(Subtraction subtraction) {
        log.warn("[VtoSVisitor] Subtraction");
    }

    @Override
    public void visit(AndExpression andExpression) {
        log.warn("[VtoSVisitor] AndExpression");
    }

    @Override
    public void visit(OrExpression orExpression) {
        log.warn("[VtoSVisitor] OrExpression");
    }

    @Override
    public void visit(Between between) {
        log.warn("[VtoSVisitor] Between");
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        log.warn("[VtoSVisitor] EqualsTo");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        log.warn("[VtoSVisitor] GreaterThan");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        log.warn("[VtoSVisitor] GreaterThanEquals");
    }

    @Override
    public void visit(InExpression inExpression) {
        log.warn("[VtoSVisitor] InExpression");
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        log.warn("[VtoSVisitor] FullTextSearch");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        log.warn("[VtoSVisitor] IsNullExpression");
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        log.warn("[VtoSVisitor] IsBooleanExpression");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        log.warn("[VtoSVisitor] LikeExpression");
    }

    @Override
    public void visit(MinorThan minorThan) {
        log.warn("[VtoSVisitor] MinorThan");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        log.warn("[VtoSVisitor] MinorThanEquals");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        log.warn("[VtoSVisitor] NotEqualsTo");
    }

    @Override
    public void visit(Column tableColumn) {
        log.warn("[VtoSVisitor] Column");
    }

    @Override
    public void visit(SubSelect subSelect) {
        log.trace("[VtoxSVisitor] SubSelect={}", subSelect.toString());
        Object value = null;
        if (subSelectResolver != null) {
            subSelect.setUseBrackets(false);
            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
            if (records.size() == 1 && records.get(0).size() == 1) {
                // return the value as plain value
                value = records.get(0).entrySet().iterator().next().getValue();
                log.trace("[VtoSVisitor] resolved to {}", value);
            }
        } else {
            log.trace("[VtoSVisitor] subSelectResolver is undefined");
        }
        fieldValues.put(columnName, value);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        log.warn("[VtoSVisitor] CaseExpression");
    }

    @Override
    public void visit(WhenClause whenClause) {
        log.warn("[VtoSVisitor] WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        log.warn("[VtoSVisitor] ExistsExpression");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        log.warn("[VtoSVisitor] AllComparisonExpression");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        log.warn("[VtoSVisitor] AnyComparisonExpression");
    }

    @Override
    public void visit(Concat concat) {
        log.warn("[VtoSVisitor] Concat");
    }

    @Override
    public void visit(Matches matches) {
        log.warn("[VtoSVisitor] Matches");
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        log.warn("[VtoSVisitor] BitwiseAnd");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        log.warn("[VtoSVisitor] BitwiseOr");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        log.warn("[VtoSVisitor] BitwiseXor");
    }

    @Override
    public void visit(CastExpression cast) {
        log.warn("[VtoSVisitor] CastExpression");
    }

    @Override
    public void visit(Modulo modulo) {
        log.warn("[VtoSVisitor] Modulo");
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        log.warn("[VtoSVisitor] AnalyticExpression");
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        log.warn("[VtoSVisitor] BitwiseRightShift");
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        log.warn("[VtoSVisitor] IntervalExpression");
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        log.warn("[VtoSVisitor] OracleHierarchicalExpression");
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        log.warn("[VtoSVisitor] RegExpMatchOperator");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        log.warn("[VtoSVisitor] JsonExpression");
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        log.warn("[VtoSVisitor] JsonOperator");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        log.warn("[VtoSVisitor] RegExpMySQLOperator");
    }

    @Override
    public void visit(UserVariable var) {
        log.warn("[VtoSVisitor] UserVariable");
    }

    @Override
    public void visit(NumericBind bind) {
        log.warn("[VtoSVisitor] NumericBind");
    }

    @Override
    public void visit(KeepExpression aexpr) {
        log.warn("[VtoSVisitor] KeepExpression");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        log.warn("[VtoSVisitor] MySQLGroupConcat");
    }

    @Override
    public void visit(ValueListExpression valueList) {
        log.warn("[VtoSVisitor] ValueListExpression");
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        log.warn("[VtoSVisitor] RowConstructor");
    }

    @Override
    public void visit(OracleHint hint) {
        log.warn("[VtoSVisitor] OracleHint");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        log.warn("[VtoSVisitor] TimeKeyExpression");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        log.warn("[VtoSVisitor] DateTimeLiteralExpression");
    }

    @Override
    public void visit(NotExpression aThis) {
        log.warn("[VtoSVisitor] NotExpression");
    }

    @Override
    public void visit(NextValExpression aThis) {
        log.warn("[VtoSVisitor] NextValExpression");
    }

    @Override
    public void visit(CollateExpression aThis) {
        log.warn("[VtoSVisitor] CollateExpression");
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        log.warn("[VtoSVisitor] SimilarToExpression");
    }

    @Override
    public void visit(ArrayExpression aThis) {
        log.warn("[VtoSVisitor] ArrayExpression");
    }

    @Override
    public void visit(VariableAssignment aThis) {
        log.warn("[VtoSVisitor] VariableAssignment");
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        log.warn("[VtoSVisitor] XMLSerializeExpr");
    }
}
