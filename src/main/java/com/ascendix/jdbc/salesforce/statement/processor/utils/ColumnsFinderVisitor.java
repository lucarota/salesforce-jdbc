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
public class ColumnsFinderVisitor extends ExpressionVisitorAdapter<Expression> {

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
    public <S> Expression visit(BitwiseRightShift aThis, S context) {
        log.trace("[ColumnsFinder] BitwiseRightShift");
        processBinaryExpression(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseLeftShift aThis, S context) {
        log.trace("[ColumnsFinder] BitwiseLeftShift");
        processBinaryExpression(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(Function function, S context) {
        log.trace("[ColumnsFinder] Function");
        functionFound = true;
        return null;
    }

    @Override
    public <S> Expression visit(SignedExpression signedExpression, S context) {
        log.trace("[ColumnsFinder] SignedExpression");
        signedExpression.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(Addition addition, S context) {
        log.trace("[ColumnsFinder] Addition");
        processBinaryExpression(addition);
        return null;
    }

    @Override
    public <S> Expression visit(Division division, S context) {
        log.trace("[ColumnsFinder] Division");
        processBinaryExpression(division);
        return null;
    }

    @Override
    public <S> Expression visit(IntegerDivision division, S context) {
        log.trace("[ColumnsFinder] IntegerDivision");
        processBinaryExpression(division);
        return null;
    }

    @Override
    public <S> Expression visit(Multiplication multiplication, S context) {
        log.trace("[ColumnsFinder] Multiplication");
        processBinaryExpression(multiplication);
        return null;
    }

    @Override
    public <S> Expression visit(Subtraction subtraction, S context) {
        log.trace("[ColumnsFinder] Subtraction");
        processBinaryExpression(subtraction);
        return null;
    }

    @Override
    public <S> Expression visit(AndExpression andExpression, S context) {
        log.trace("[ColumnsFinder] AndExpression");
        processBinaryExpression(andExpression);
        return null;
    }

    @Override
    public <S> Expression visit(OrExpression orExpression, S context) {
        log.trace("[ColumnsFinder] OrExpression");
        processBinaryExpression(orExpression);
        return null;
    }

    @Override
    public <S> Expression visit(Between between, S context) {
        log.trace("[ColumnsFinder] Between");
        between.getLeftExpression().accept(this);
        between.getBetweenExpressionStart().accept(this);
        between.getBetweenExpressionEnd().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(EqualsTo equalsTo, S context) {
        log.trace("[ColumnsFinder] EqualsTo");
        processBinaryExpression(equalsTo);
        return null;
    }

    @Override
    public <S> Expression visit(GreaterThan greaterThan, S context) {
        log.trace("[ColumnsFinder] GreaterThan");
        processBinaryExpression(greaterThan);
        return null;
    }

    @Override
    public <S> Expression visit(GreaterThanEquals greaterThanEquals, S context) {
        log.trace("[ColumnsFinder] GreaterThanEquals");
        processBinaryExpression(greaterThanEquals);
        return null;
    }

    @Override
    public <S> Expression visit(InExpression inExpression, S context) {
        log.trace("[ColumnsFinder] InExpression");
        inExpression.getLeftExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(FullTextSearch fullTextSearch, S context) {
        log.trace("[ColumnsFinder] FullTextSearch");
        fullTextSearch.getMatchColumns().forEach(this::addColumn);
        return null;
    }

    @Override
    public <S> Expression visit(IsNullExpression isNullExpression, S context) {
        log.trace("[ColumnsFinder] IsNullExpression");
        isNullExpression.getLeftExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(IsBooleanExpression isBooleanExpression, S context) {
        log.trace("[ColumnsFinder] IsBooleanExpression");
        isBooleanExpression.getLeftExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(LikeExpression likeExpression, S context) {
        log.trace("[ColumnsFinder] LikeExpression");
        processExpression(likeExpression);
        return null;
    }

    @Override
    public <S> Expression visit(MinorThan minorThan, S context) {
        log.trace("[ColumnsFinder] MinorThan");
        processBinaryExpression(minorThan);
        return null;
    }

    @Override
    public <S> Expression visit(MinorThanEquals minorThanEquals, S context) {
        log.trace("[ColumnsFinder] MinorThanEquals");
        processBinaryExpression(minorThanEquals);
        return null;
    }

    @Override
    public <S> Expression visit(NotEqualsTo notEqualsTo, S context) {
        log.trace("[ColumnsFinder] NotEqualsTo");
        processBinaryExpression(notEqualsTo);
        return null;
    }

    @Override
    public <S> Expression visit(Column tableColumn, S context) {
        log.trace("[ColumnsFinder] Column");
        addColumn(tableColumn);
        return null;
    }

    @Override
    public <S> Expression visit(ExistsExpression existsExpression, S context) {
        log.trace("[ColumnsFinder] ExistsExpression");
        existsExpression.getRightExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(Concat concat, S context) {
        log.trace("[ColumnsFinder] Concat");
        processBinaryExpression(concat);
        return null;
    }

    @Override
    public <S> Expression visit(Matches matches, S context) {
        log.trace("[ColumnsFinder] Matches");
        processBinaryExpression(matches);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseAnd bitwiseAnd, S context) {
        log.trace("[ColumnsFinder] BitwiseAnd");
        processBinaryExpression(bitwiseAnd);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseOr bitwiseOr, S context) {
        log.trace("[ColumnsFinder] BitwiseOr");
        processBinaryExpression(bitwiseOr);
        return null;
    }

    @Override
    public <S> Expression visit(BitwiseXor bitwiseXor, S context) {
        log.trace("[ColumnsFinder] BitwiseXor");
        processBinaryExpression(bitwiseXor);
        return null;
    }

    @Override
    public <S> Expression visit(CastExpression cast, S context) {
        log.trace("[ColumnsFinder] CastExpression");
        cast.getLeftExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(Modulo modulo, S context) {
        log.trace("[ColumnsFinder] Modulo");
        processBinaryExpression(modulo);
        return null;
    }

    @Override
    public <S> Expression visit(AnalyticExpression aexpr, S context) {
        log.trace("[ColumnsFinder] AnalyticExpression");
        aexpr.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(ExtractExpression eexpr, S context) {
        log.trace("[ColumnsFinder] BitwiseRightShift");
        eexpr.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(IntervalExpression iexpr, S context) {
        log.trace("[ColumnsFinder] IntervalExpression");
        iexpr.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(OracleHierarchicalExpression oexpr, S context) {
        log.trace("[ColumnsFinder] OracleHierarchicalExpression");
        oexpr.getStartExpression().accept(this);
        oexpr.getConnectExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(RegExpMatchOperator rexpr, S context) {
        log.trace("[ColumnsFinder] RegExpMatchOperator");
        processBinaryExpression(rexpr);
        return null;
    }

    @Override
    public <S> Expression visit(JsonExpression jsonExpr, S context) {
        log.trace("[ColumnsFinder] JsonExpression");
        jsonExpr.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(JsonOperator jsonExpr, S context) {
        log.trace("[ColumnsFinder] JsonOperator");
        processBinaryExpression(jsonExpr);
        return null;
    }

    @Override
    public <S> Expression visit(MySQLGroupConcat groupConcat, S context) {
        log.trace("[ColumnsFinder] MySQLGroupConcat");
        groupConcat.getExpressionList().forEach(expression -> expression.accept(this));
        return null;
    }

    @Override
    public <S> Expression visit(RowConstructor<? extends Expression> rowConstructor, S context) {
        for (Expression expr : rowConstructor) {
            expr.accept(this, context);
        }
        return null;
    }

    @Override
    public <S> Expression visit(NotExpression aThis, S context) {
        log.trace("[ColumnsFinder] NotExpression");
        aThis.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(CollateExpression aThis, S context) {
        log.trace("[ColumnsFinder] CollateExpression");
        aThis.getLeftExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(SimilarToExpression aThis, S context) {
        log.trace("[ColumnsFinder] SimilarToExpression");
        processBinaryExpression(aThis);
        return null;
    }

    @Override
    public <S> Expression visit(ArrayExpression aThis, S context) {
        log.trace("[ColumnsFinder] ArrayExpression");
        aThis.getIndexExpression().accept(this);
        aThis.getObjExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(VariableAssignment aThis, S context) {
        log.trace("[ColumnsFinder] VariableAssignment");
        aThis.getExpression().accept(this);
        return null;
    }

    @Override
    public <S> Expression visit(XMLSerializeExpr aThis, S context) {
        log.trace("[ColumnsFinder] XMLSerializeExpr");
        aThis.getExpression().accept(this);
        return null;
    }
}
