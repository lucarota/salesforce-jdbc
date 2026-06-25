package it.rotaliano.jdbc.salesforce.expression;

public class NotExpression implements Expression {
    private final Expression operand;

    public NotExpression(Expression operand) {
        this.operand = operand;
    }

    @Override
    public Object evaluate(RowContext row) {
        Object val = operand.evaluate(row);
        if (val == null) {
            return null;
        }
        return !LogicalExpression.isTruthy(val);
    }
}
