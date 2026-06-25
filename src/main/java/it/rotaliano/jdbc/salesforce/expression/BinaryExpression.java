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

        if (lVal instanceof Number && rVal instanceof Number) {
            double d1 = ((Number) lVal).doubleValue();
            double d2 = ((Number) rVal).doubleValue();
            int cmp = Double.compare(d1, d2);
            switch (op) {
                case EQUALS: return cmp == 0;
                case NOT_EQUALS: return cmp != 0;
                case GREATER_THAN: return cmp > 0;
                case GREATER_THAN_EQUALS: return cmp >= 0;
                case LESS_THAN: return cmp < 0;
                case LESS_THAN_EQUALS: return cmp <= 0;
            }
        }

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
                if (lVal instanceof Comparable && lVal.getClass().isInstance(rVal)) {
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
