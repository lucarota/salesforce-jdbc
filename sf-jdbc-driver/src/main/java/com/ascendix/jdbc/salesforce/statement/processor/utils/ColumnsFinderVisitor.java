package com.ascendix.jdbc.salesforce.statement.processor.utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;

import java.util.Set;

@Slf4j
public class ColumnsFinderVisitor extends ExpressionVisitorAdapter {

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
    public void visit(Parenthesis parenthesis) {
        log.trace("[ColumnsFinder] Parenthesis");
        parenthesis.getExpression().accept(this);
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
    public void visit(ExistsExpression existsExpression) {
        log.trace("[ColumnsFinder] ExistsExpression");
        existsExpression.getRightExpression().accept(this);
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
        jsonExpr.getExpression().accept(this);
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        log.trace("[ColumnsFinder] JsonOperator");
        processBinaryExpression(jsonExpr);
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        log.trace("[ColumnsFinder] MySQLGroupConcat");
        groupConcat.getExpressionList().forEach(expression -> expression.accept(this));
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        log.trace("[ColumnsFinder] RowConstructor");
        rowConstructor.accept(this);
    }

    @Override
    public void visit(NotExpression aThis) {
        log.trace("[ColumnsFinder] NotExpression");
        aThis.getExpression().accept(this);
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
