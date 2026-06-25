package it.rotaliano.jdbc.salesforce.expression;

public class LogicalExpression implements Expression {
    public enum Operator {
        AND, OR
    }

    private final Expression left;
    private final Expression right;
    private final Operator op;

    public LogicalExpression(Expression left, Expression right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public Object evaluate(RowContext row) {
        Object lVal = left.evaluate(row);
        if (op == Operator.AND) {
            if (lVal != null && !isTruthy(lVal)) {
                return false;
            }
            Object rVal = right.evaluate(row);
            if (rVal != null && !isTruthy(rVal)) {
                return false;
            }
            if (lVal != null && rVal != null) {
                return isTruthy(lVal) && isTruthy(rVal);
            }
            return null;
        } else {
            if (lVal != null && isTruthy(lVal)) {
                return true;
            }
            Object rVal = right.evaluate(row);
            if (rVal != null && isTruthy(rVal)) {
                return true;
            }
            if (lVal != null && rVal != null) {
                return isTruthy(lVal) || isTruthy(rVal);
            }
            return null;
        }
    }

    private boolean isTruthy(Object val) {
        if (val == null) {
            return false;
        }
        if (val instanceof Boolean) {
            return (Boolean) val;
        }
        if (val instanceof Number) {
            return ((Number) val).doubleValue() != 0.0;
        }
        String s = val.toString().trim();
        return "true".equalsIgnoreCase(s) || "1".equals(s) || "yes".equalsIgnoreCase(s);
    }
}
