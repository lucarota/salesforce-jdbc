package it.rotaliano.jdbc.salesforce.expression;

public interface Expression {
    Object evaluate(RowContext row);
}
