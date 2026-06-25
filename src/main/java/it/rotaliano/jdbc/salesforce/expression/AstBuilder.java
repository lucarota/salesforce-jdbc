package it.rotaliano.jdbc.salesforce.expression;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
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
        throw new UnsupportedOperationException("Expression not supported in emulated engine: " + jsqlExpr);
    }
}
