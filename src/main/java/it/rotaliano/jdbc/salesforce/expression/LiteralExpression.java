package it.rotaliano.jdbc.salesforce.expression;

public class LiteralExpression implements Expression {
    private final Object value;

    public LiteralExpression(Object value) {
        this.value = value;
    }

    @Override
    public Object evaluate(RowContext row) {
        return value;
    }
}
