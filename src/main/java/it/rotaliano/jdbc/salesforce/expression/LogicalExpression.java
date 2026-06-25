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
            if (lVal == null || !((Boolean) lVal)) {
                return false;
            }
            Object rVal = right.evaluate(row);
            return rVal != null && ((Boolean) rVal);
        } else {
            if (lVal != null && ((Boolean) lVal)) {
                return true;
            }
            Object rVal = right.evaluate(row);
            return rVal != null && ((Boolean) rVal);
        }
    }
}
