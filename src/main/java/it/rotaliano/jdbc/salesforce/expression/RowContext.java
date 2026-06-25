package it.rotaliano.jdbc.salesforce.expression;

public interface RowContext {
    Object getValue(String columnName);
}
