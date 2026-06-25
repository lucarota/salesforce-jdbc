package it.rotaliano.jdbc.salesforce.expression;

import java.util.Objects;

public class BinaryExpression implements Expression {
    public enum Operator {
        EQUALS, NOT_EQUALS, GREATER_THAN, GREATER_THAN_EQUALS, LESS_THAN, LESS_THAN_EQUALS
    }

    private final Expression left;
    private final Expression right;
    private final Operator op;

    public BinaryExpression(Expression left, Expression right, Operator op) {
        this.left = left;
        this.right = right;
        this.op = op;
    }

    @Override
    public Object evaluate(RowContext row) {
        Object lVal = left.evaluate(row);
        Object rVal = right.evaluate(row);

        switch (op) {
            case EQUALS:
                return Objects.equals(lVal, rVal);
            case NOT_EQUALS:
                return !Objects.equals(lVal, rVal);
            default:
                if (lVal == null || rVal == null) {
                    return false;
                }
                int cmp = lVal.toString().compareTo(rVal.toString());
                if (lVal instanceof Comparable && rVal.getClass().isInstance(lVal)) {
                    cmp = ((Comparable<Object>) lVal).compareTo(rVal);
                }
                switch (op) {
                    case GREATER_THAN: return cmp > 0;
                    case GREATER_THAN_EQUALS: return cmp >= 0;
                    case LESS_THAN: return cmp < 0;
                    case LESS_THAN_EQUALS: return cmp <= 0;
                }
        }
        return false;
    }
}
