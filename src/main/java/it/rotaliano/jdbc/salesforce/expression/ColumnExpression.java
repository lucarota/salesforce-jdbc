package it.rotaliano.jdbc.salesforce.expression;

public class ColumnExpression implements Expression {
    private final String columnName;

    public ColumnExpression(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public Object evaluate(RowContext row) {
        return row.getValue(columnName);
    }
}
