package it.rotaliano.jdbc.salesforce.expression;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.schema.Column;
import java.util.ArrayList;
import java.util.List;

public class AstBuilder {
    public static Expression build(net.sf.jsqlparser.expression.Expression jsqlExpr) {
        if (jsqlExpr == null) {
            return null;
        }
        if (jsqlExpr instanceof StringValue sv) {
            return new LiteralExpression(sv.getValue());
        }
        if (jsqlExpr instanceof LongValue lv) {
            return new LiteralExpression(lv.getValue());
        }
        if (jsqlExpr instanceof DoubleValue dv) {
            return new LiteralExpression(dv.getValue());
        }
        if (jsqlExpr instanceof NullValue) {
            return new LiteralExpression(null);
        }
        if (jsqlExpr instanceof Column col) {
            return new ColumnExpression(col.getColumnName());
        }
        if (jsqlExpr instanceof net.sf.jsqlparser.expression.TrimFunction tf) {
            List<Expression> args = new ArrayList<>();
            if (tf.getFromExpression() != null) {
                args.add(build(tf.getFromExpression()));
            }
            if (tf.getExpression() != null) {
                args.add(build(tf.getExpression()));
            }
            return new FunctionExpression("TRIM", args);
        }
        if (jsqlExpr instanceof Function func) {
            List<Expression> args = new ArrayList<>();
            if (func.getParameters() != null) {
                for (net.sf.jsqlparser.expression.Expression p : func.getParameters()) {
                    args.add(build(p));
                }
            }
            return new FunctionExpression(func.getName(), args);
        }
        if (jsqlExpr instanceof EqualsTo eq) {
            return new BinaryExpression(build(eq.getLeftExpression()), build(eq.getRightExpression()), BinaryExpression.Operator.EQUALS);
        }
        if (jsqlExpr instanceof NotEqualsTo neq) {
            return new BinaryExpression(build(neq.getLeftExpression()), build(neq.getRightExpression()), BinaryExpression.Operator.NOT_EQUALS);
        }
        if (jsqlExpr instanceof AndExpression and) {
            return new LogicalExpression(build(and.getLeftExpression()), build(and.getRightExpression()), LogicalExpression.Operator.AND);
        }
        if (jsqlExpr instanceof OrExpression or) {
            return new LogicalExpression(build(or.getLeftExpression()), build(or.getRightExpression()), LogicalExpression.Operator.OR);
        }
        if (jsqlExpr instanceof net.sf.jsqlparser.expression.NotExpression not) {
            return new NotExpression(build(not.getExpression()));
        }
        if (jsqlExpr instanceof ParenthesedExpressionList parenList && !parenList.isEmpty()) {
            return build((net.sf.jsqlparser.expression.Expression) parenList.get(0));
        }
        if (jsqlExpr instanceof IsNullExpression isNull) {
            Expression left = build(isNull.getLeftExpression());
            BinaryExpression.Operator op = isNull.isNot() ? BinaryExpression.Operator.NOT_EQUALS : BinaryExpression.Operator.EQUALS;
            return new BinaryExpression(left, new LiteralExpression(null), op);
        }
        if (jsqlExpr instanceof InExpression in) {
            Expression left = build(in.getLeftExpression());
            net.sf.jsqlparser.expression.Expression right = in.getRightExpression();
            if (right instanceof ParenthesedExpressionList parenList) {
                if (parenList.isEmpty()) {
                    Expression alwaysFalse = new BinaryExpression(new LiteralExpression(1), new LiteralExpression(2), BinaryExpression.Operator.EQUALS);
                    return in.isNot() ? new NotExpression(alwaysFalse) : alwaysFalse;
                }
                Expression orChain = null;
                for (Object item : parenList) {
                    Expression cmp = new BinaryExpression(left, build((net.sf.jsqlparser.expression.Expression) item), BinaryExpression.Operator.EQUALS);
                    if (orChain == null) {
                        orChain = cmp;
                    } else {
                        orChain = new LogicalExpression(orChain, cmp, LogicalExpression.Operator.OR);
                    }
                }
                if (in.isNot()) {
                    return new NotExpression(orChain);
                }
                return orChain;
            }
            throw new UnsupportedOperationException("InExpression right side must be ParenthesedExpressionList: " + right);
        }
        if (jsqlExpr instanceof Between between) {
            Expression left = build(between.getLeftExpression());
            Expression start = build(between.getBetweenExpressionStart());
            Expression end = build(between.getBetweenExpressionEnd());
            Expression ge = new BinaryExpression(left, start, BinaryExpression.Operator.GREATER_THAN_EQUALS);
            Expression le = new BinaryExpression(left, end, BinaryExpression.Operator.LESS_THAN_EQUALS);
            Expression andExpr = new LogicalExpression(ge, le, LogicalExpression.Operator.AND);
            if (between.isNot()) {
                return new NotExpression(andExpr);
            }
            return andExpr;
        }
        throw new UnsupportedOperationException("Expression not supported in emulated engine: " + jsqlExpr);
    }
}
