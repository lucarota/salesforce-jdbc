package com.ascendix.jdbc.salesforce.statement.processor.utils;

import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
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

@Slf4j
public class ColumnsFinderVisitor implements ExpressionVisitor {

    private final Set<String> columns;
    @Getter
    private boolean functionFound = false;

    public ColumnsFinderVisitor(Set<String> columns) {
        this.columns = columns;
    }

    private void addColumn(Column column) {
        if (columns.add(column.getColumnName())) {
            log.info("New column found: {}", column.getColumnName());
        } else {
            log.info("Already detected column: {}", column.getColumnName());
        }
    }

    private void processExpression(Expression expr) {
        expr.accept(this);
    }

    private void processBinaryExpression(BinaryExpression expr) {
        expr.getLeftExpression().accept(this);
        expr.getRightExpression().accept(this);
    }

    @Override
    public void visit(BitwiseRightShift aThis) {
        log.trace("[ColumnsFinder] BitwiseRightShift");
        processBinaryExpression(aThis);
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        log.trace("[ColumnsFinder] BitwiseLeftShift");
        processBinaryExpression(aThis);
    }

    @Override
    public void visit(NullValue nullValue) {
        log.trace("[ColumnsFinder] NullValue ");
    }

    @Override
    public void visit(Function function) {
        log.trace("[ColumnsFinder] Function");
        functionFound = true;
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        log.trace("[ColumnsFinder] SignedExpression");
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        log.trace("[ColumnsFinder] JdbcParameter");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        log.trace("[ColumnsFinder] JdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        log.trace("[ColumnsFinder] DoubleValue");
    }

    @Override
    public void visit(LongValue longValue) {
        log.trace("[ColumnsFinder] LongValue");
    }

    @Override
    public void visit(HexValue hexValue) {
        log.trace("[ColumnsFinder] HexValue");
    }

    @Override
    public void visit(DateValue dateValue) {
        log.trace("[ColumnsFinder] DateValue");
    }

    @Override
    public void visit(TimeValue timeValue) {
        log.trace("[ColumnsFinder] BitwiseRightShift");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        log.trace("[ColumnsFinder] TimestampValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        log.trace("[ColumnsFinder] Parenthesis");
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        log.trace("[ColumnsFinder] StringValue");
    }

    @Override
    public void visit(Addition addition) {
        log.trace("[ColumnsFinder] Addition");
        processBinaryExpression(addition);
    }

    @Override
    public void visit(Division division) {
        log.trace("[ColumnsFinder] Division");
        processBinaryExpression(division);
    }

    @Override
    public void visit(IntegerDivision division) {
        log.trace("[ColumnsFinder] IntegerDivision");
        processBinaryExpression(division);
    }

    @Override
    public void visit(Multiplication multiplication) {
        log.trace("[ColumnsFinder] Multiplication");
        processBinaryExpression(multiplication);
    }

    @Override
    public void visit(Subtraction subtraction) {
        log.trace("[ColumnsFinder] Subtraction");
        processBinaryExpression(subtraction);
    }

    @Override
    public void visit(AndExpression andExpression) {
        log.trace("[ColumnsFinder] AndExpression");
        processBinaryExpression(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        log.trace("[ColumnsFinder] OrExpression");
        processBinaryExpression(orExpression);
    }

    @Override
    public void visit(Between between) {
        log.trace("[ColumnsFinder] Between");
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        log.trace("[ColumnsFinder] EqualsTo");
        processBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        log.trace("[ColumnsFinder] GreaterThan");
        processBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        log.trace("[ColumnsFinder] GreaterThanEquals");
        processBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        log.trace("[ColumnsFinder] InExpression");
        inExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        log.trace("[ColumnsFinder] FullTextSearch");
        fullTextSearch.getMatchColumns().forEach(this::addColumn);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        log.trace("[ColumnsFinder] IsNullExpression");
        isNullExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        log.trace("[ColumnsFinder] IsBooleanExpression");
        isBooleanExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        log.trace("[ColumnsFinder] LikeExpression");
        processExpression(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        log.trace("[ColumnsFinder] MinorThan");
        processBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        log.trace("[ColumnsFinder] MinorThanEquals");
        processBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        log.trace("[ColumnsFinder] NotEqualsTo");
        processBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(Column tableColumn) {
        log.trace("[ColumnsFinder] Column");
        addColumn(tableColumn);
    }

    @Override
    public void visit(SubSelect subSelect) {
        log.trace("[VtoxSVisitor] SubSelect={}", subSelect.toString());
//        Object value = null;
//        if (subSelectResolver != null) {
//            subSelect.setUseBrackets(false);
//            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
//            if (records.size() == 1 && records.get(0).size() == 1) {
//                 return the value as plain value
//                value = records.get(0).entrySet().iterator().next().getValue();
//                log.trace("[ColumnsFinder] resolved to {}", value);
//            }
//        } else {
//            log.trace("[ColumnsFinder] subSelectResolver is undefined");
//        }
//        recordFieldsToUpdate.put(columnName, value);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        log.trace("[ColumnsFinder] CaseExpression NOT_SUPPORTED");
    }

    @Override
    public void visit(WhenClause whenClause) {
        log.trace("[ColumnsFinder] WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        log.trace("[ColumnsFinder] ExistsExpression");
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        log.trace("[ColumnsFinder] AllComparisonExpression NOT_SUPPORTED!!!");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        log.trace("[ColumnsFinder] AnyComparisonExpression");
    }

    @Override
    public void visit(Concat concat) {
        log.trace("[ColumnsFinder] Concat");
        processBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        log.trace("[ColumnsFinder] Matches");
        processBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        log.trace("[ColumnsFinder] BitwiseAnd");
        processBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        log.trace("[ColumnsFinder] BitwiseOr");
        processBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        log.trace("[ColumnsFinder] BitwiseXor");
        processBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        log.trace("[ColumnsFinder] CastExpression");
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        log.trace("[ColumnsFinder] Modulo");
        processBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        log.trace("[ColumnsFinder] AnalyticExpression");
        aexpr.getExpression().accept(this);
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        log.trace("[ColumnsFinder] BitwiseRightShift");
        eexpr.getExpression().accept(this);
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        log.trace("[ColumnsFinder] IntervalExpression");
        iexpr.getExpression().accept(this);
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        log.trace("[ColumnsFinder] OracleHierarchicalExpression");
        oexpr.getStartExpression().accept(this);
        oexpr.getConnectExpression().accept(this);
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        log.trace("[ColumnsFinder] RegExpMatchOperator");
        processBinaryExpression(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        log.trace("[ColumnsFinder] JsonExpression");
        addColumn(jsonExpr.getColumn());
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        log.trace("[ColumnsFinder] JsonOperator");
        processBinaryExpression(jsonExpr);
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        log.trace("[ColumnsFinder] RegExpMySQLOperator");
        processBinaryExpression(regExpMySQLOperator);
    }

    @Override
    public void visit(UserVariable var) {
        log.trace("[ColumnsFinder] UserVariable");
    }

    @Override
    public void visit(NumericBind bind) {
        log.trace("[ColumnsFinder] NumericBind");
    }

    @Override
    public void visit(KeepExpression aexpr) {
        log.trace("[ColumnsFinder] KeepExpression");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        log.trace("[ColumnsFinder] MySQLGroupConcat");
        groupConcat.getExpressionList().getExpressions().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(ValueListExpression valueList) {
        log.trace("[ColumnsFinder] ValueListExpression");
        valueList.getExpressionList().getExpressions().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        log.trace("[ColumnsFinder] RowConstructor");
        rowConstructor.getExprList().getExpressions().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(OracleHint hint) {
        log.trace("[ColumnsFinder] OracleHint");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        log.trace("[ColumnsFinder] TimeKeyExpression");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        log.trace("[ColumnsFinder] DateTimeLiteralExpression");
    }

    @Override
    public void visit(NotExpression aThis) {
        log.trace("[ColumnsFinder] NotExpression");
        aThis.getExpression().accept(this);
    }

    @Override
    public void visit(NextValExpression aThis) {
        log.trace("[ColumnsFinder] NextValExpression");
    }

    @Override
    public void visit(CollateExpression aThis) {
        log.trace("[ColumnsFinder] CollateExpression");
        aThis.getLeftExpression().accept(this);
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        log.trace("[ColumnsFinder] SimilarToExpression");
        processBinaryExpression(aThis);
    }

    @Override
    public void visit(ArrayExpression aThis) {
        log.trace("[ColumnsFinder] ArrayExpression");
        aThis.getIndexExpression().accept(this);
        aThis.getObjExpression().accept(this);
    }

    @Override
    public void visit(VariableAssignment aThis) {
        log.trace("[ColumnsFinder] VariableAssignment");
        aThis.getExpression().accept(this);
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        log.trace("[ColumnsFinder] XMLSerializeExpr");
        aThis.getExpression().accept(this);
    }
}
