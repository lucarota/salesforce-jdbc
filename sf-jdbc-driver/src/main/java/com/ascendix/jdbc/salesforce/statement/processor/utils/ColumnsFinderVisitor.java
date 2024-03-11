package com.ascendix.jdbc.salesforce.statement.processor.utils;

import com.ascendix.jdbc.salesforce.ForceDriver;
import java.util.Set;
import java.util.logging.Logger;
import lombok.Getter;
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

public class ColumnsFinderVisitor implements ExpressionVisitor {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

    private final Set<String> columns;
    @Getter
    private boolean functionFound = false;

    public ColumnsFinderVisitor(Set<String> columns) {
        this.columns = columns;
    }

    private void addColumn(Column column) {
        if (columns.add(column.getColumnName())) {
            logger.info("New column found: " + column.getColumnName());
        } else {
            logger.info("Already detected column: " + column.getColumnName());
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
        logger.finest("[ColumnsFinder] BitwiseRightShift");
        processBinaryExpression(aThis);
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        logger.finest("[ColumnsFinder] BitwiseLeftShift");
        processBinaryExpression(aThis);
    }

    @Override
    public void visit(NullValue nullValue) {
        logger.finest("[ColumnsFinder] NullValue ");
    }

    @Override
    public void visit(Function function) {
        logger.finest("[ColumnsFinder] Function");
        functionFound = true;
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        logger.finest("[ColumnsFinder] SignedExpression");
        signedExpression.getExpression().accept(this);
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        logger.finest("[ColumnsFinder] JdbcParameter");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        logger.finest("[ColumnsFinder] JdbcNamedParameter");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        logger.finest("[ColumnsFinder] DoubleValue");
    }

    @Override
    public void visit(LongValue longValue) {
        logger.finest("[ColumnsFinder] LongValue");
    }

    @Override
    public void visit(HexValue hexValue) {
        logger.finest("[ColumnsFinder] HexValue");
    }

    @Override
    public void visit(DateValue dateValue) {
        logger.finest("[ColumnsFinder] DateValue");
    }

    @Override
    public void visit(TimeValue timeValue) {
        logger.finest("[ColumnsFinder] BitwiseRightShift");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        logger.finest("[ColumnsFinder] TimestampValue");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        logger.finest("[ColumnsFinder] Parenthesis");
        parenthesis.getExpression().accept(this);
    }

    @Override
    public void visit(StringValue stringValue) {
        logger.finest("[ColumnsFinder] StringValue");
    }

    @Override
    public void visit(Addition addition) {
        logger.finest("[ColumnsFinder] Addition");
        processBinaryExpression(addition);
    }

    @Override
    public void visit(Division division) {
        logger.finest("[ColumnsFinder] Division");
        processBinaryExpression(division);
    }

    @Override
    public void visit(IntegerDivision division) {
        logger.finest("[ColumnsFinder] IntegerDivision");
        processBinaryExpression(division);
    }

    @Override
    public void visit(Multiplication multiplication) {
        logger.finest("[ColumnsFinder] Multiplication");
        processBinaryExpression(multiplication);
    }

    @Override
    public void visit(Subtraction subtraction) {
        logger.finest("[ColumnsFinder] Subtraction");
        processBinaryExpression(subtraction);
    }

    @Override
    public void visit(AndExpression andExpression) {
        logger.finest("[ColumnsFinder] AndExpression");
        processBinaryExpression(andExpression);
    }

    @Override
    public void visit(OrExpression orExpression) {
        logger.finest("[ColumnsFinder] OrExpression");
        processBinaryExpression(orExpression);
    }

    @Override
    public void visit(Between between) {
        logger.finest("[ColumnsFinder] Between");
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        logger.finest("[ColumnsFinder] EqualsTo");
        processBinaryExpression(equalsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        logger.finest("[ColumnsFinder] GreaterThan");
        processBinaryExpression(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        logger.finest("[ColumnsFinder] GreaterThanEquals");
        processBinaryExpression(greaterThanEquals);
    }

    @Override
    public void visit(InExpression inExpression) {
        logger.finest("[ColumnsFinder] InExpression");
        inExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        logger.finest("[ColumnsFinder] FullTextSearch");
        fullTextSearch.getMatchColumns().forEach(this::addColumn);
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        logger.finest("[ColumnsFinder] IsNullExpression");
        isNullExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        logger.finest("[ColumnsFinder] IsBooleanExpression");
        isBooleanExpression.getLeftExpression().accept(this);
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        logger.finest("[ColumnsFinder] LikeExpression");
        processExpression(likeExpression);
    }

    @Override
    public void visit(MinorThan minorThan) {
        logger.finest("[ColumnsFinder] MinorThan");
        processBinaryExpression(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        logger.finest("[ColumnsFinder] MinorThanEquals");
        processBinaryExpression(minorThanEquals);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        logger.finest("[ColumnsFinder] NotEqualsTo");
        processBinaryExpression(notEqualsTo);
    }

    @Override
    public void visit(Column tableColumn) {
        logger.finest("[ColumnsFinder] Column");
        addColumn(tableColumn);
    }

    @Override
    public void visit(SubSelect subSelect) {
        logger.finest("[VtoxSVisitor] SubSelect=" + subSelect.toString());
//        Object value = null;
//        if (subSelectResolver != null) {
//            subSelect.setUseBrackets(false);
//            List<Map<String, Object>> records = subSelectResolver.apply(subSelect.toString());
//            if (records.size() == 1 && records.get(0).size() == 1) {
//                 return the value as plain value
//                value = records.get(0).entrySet().iterator().next().getValue();
//                logger.finest("[ColumnsFinder] resolved to "+value);
//            }
//        } else {
//            logger.finest("[ColumnsFinder] subSelectResolver is undefined");
//        }
//        recordFieldsToUpdate.put(columnName, value);
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        logger.finest("[ColumnsFinder] CaseExpression NOT_SUPPORTED");
    }

    @Override
    public void visit(WhenClause whenClause) {
        logger.finest("[ColumnsFinder] WhenClause");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        logger.finest("[ColumnsFinder] ExistsExpression");
        existsExpression.getRightExpression().accept(this);
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        logger.finest("[ColumnsFinder] AllComparisonExpression NOT_SUPPORTED!!!");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        logger.finest("[ColumnsFinder] AnyComparisonExpression");
    }

    @Override
    public void visit(Concat concat) {
        logger.finest("[ColumnsFinder] Concat");
        processBinaryExpression(concat);
    }

    @Override
    public void visit(Matches matches) {
        logger.finest("[ColumnsFinder] Matches");
        processBinaryExpression(matches);
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        logger.finest("[ColumnsFinder] BitwiseAnd");
        processBinaryExpression(bitwiseAnd);
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        logger.finest("[ColumnsFinder] BitwiseOr");
        processBinaryExpression(bitwiseOr);
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        logger.finest("[ColumnsFinder] BitwiseXor");
        processBinaryExpression(bitwiseXor);
    }

    @Override
    public void visit(CastExpression cast) {
        logger.finest("[ColumnsFinder] CastExpression");
        cast.getLeftExpression().accept(this);
    }

    @Override
    public void visit(Modulo modulo) {
        logger.finest("[ColumnsFinder] Modulo");
        processBinaryExpression(modulo);
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        logger.finest("[ColumnsFinder] AnalyticExpression");
        aexpr.getExpression().accept(this);
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        logger.finest("[ColumnsFinder] BitwiseRightShift");
        eexpr.getExpression().accept(this);
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        logger.finest("[ColumnsFinder] IntervalExpression");
        iexpr.getExpression().accept(this);
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        logger.finest("[ColumnsFinder] OracleHierarchicalExpression");
        oexpr.getStartExpression().accept(this);
        oexpr.getConnectExpression().accept(this);
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        logger.finest("[ColumnsFinder] RegExpMatchOperator");
        processBinaryExpression(rexpr);
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        logger.finest("[ColumnsFinder] JsonExpression");
        addColumn(jsonExpr.getColumn());
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        logger.finest("[ColumnsFinder] JsonOperator");
        processBinaryExpression(jsonExpr);
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        logger.finest("[ColumnsFinder] RegExpMySQLOperator");
        processBinaryExpression(regExpMySQLOperator);
    }

    @Override
    public void visit(UserVariable var) {
        logger.finest("[ColumnsFinder] UserVariable");
    }

    @Override
    public void visit(NumericBind bind) {
        logger.finest("[ColumnsFinder] NumericBind");
    }

    @Override
    public void visit(KeepExpression aexpr) {
        logger.finest("[ColumnsFinder] KeepExpression");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        logger.finest("[ColumnsFinder] MySQLGroupConcat");
        groupConcat.getExpressionList().getExpressions().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(ValueListExpression valueList) {
        logger.finest("[ColumnsFinder] ValueListExpression");
        valueList.getExpressionList().getExpressions().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        logger.finest("[ColumnsFinder] RowConstructor");
        rowConstructor.getExprList().getExpressions().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(OracleHint hint) {
        logger.finest("[ColumnsFinder] OracleHint");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        logger.finest("[ColumnsFinder] TimeKeyExpression");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        logger.finest("[ColumnsFinder] DateTimeLiteralExpression");
    }

    @Override
    public void visit(NotExpression aThis) {
        logger.finest("[ColumnsFinder] NotExpression");
        aThis.getExpression().accept(this);
    }

    @Override
    public void visit(NextValExpression aThis) {
        logger.finest("[ColumnsFinder] NextValExpression");
    }

    @Override
    public void visit(CollateExpression aThis) {
        logger.finest("[ColumnsFinder] CollateExpression");
        aThis.getLeftExpression().accept(this);
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        logger.finest("[ColumnsFinder] SimilarToExpression");
        processBinaryExpression(aThis);
    }

    @Override
    public void visit(ArrayExpression aThis) {
        logger.finest("[ColumnsFinder] ArrayExpression");
        aThis.getIndexExpression().accept(this);
        aThis.getObjExpression().accept(this);
    }

    @Override
    public void visit(VariableAssignment aThis) {
        logger.finest("[ColumnsFinder] VariableAssignment");
        aThis.getExpression().accept(this);
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        logger.finest("[ColumnsFinder] XMLSerializeExpr");
        aThis.getExpression().accept(this);
    }
}
