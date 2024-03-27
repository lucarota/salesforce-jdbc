package com.ascendix.jdbc.salesforce.resultset;

import java.sql.ResultSetMetaData;

public class CachedResultSetMetaData implements ResultSetMetaData {

    public static final CachedResultSetMetaData EMPTY = new CachedResultSetMetaData();

    @Override
    public int getColumnCount() {
        return 0;
    }

    @Override
    public boolean isAutoIncrement(final int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) {
        return false;
    }

    @Override
    public boolean isSearchable(final int column) {
        return false;
    }

    @Override
    public boolean isCurrency(final int column) {
        return false;
    }

    @Override
    public int isNullable(final int column) {
        return ResultSetMetaData.columnNoNulls;
    }

    @Override
    public boolean isSigned(final int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(final int column) {
        return 0;
    }

    @Override
    public String getColumnLabel(final int column) {
        return null;
    }

    @Override
    public String getColumnName(final int column) {
        return null;
    }

    @Override
    public String getSchemaName(final int column) {
        return null;
    }

    @Override
    public int getPrecision(final int column) {
        return 0;
    }

    @Override
    public int getScale(final int column) {
        return 0;
    }

    @Override
    public String getTableName(final int column) {
        return null;
    }

    @Override
    public String getCatalogName(int column) {
        return "";
    }

    @Override
    public int getColumnType(final int column) {
        return 0;
    }

    @Override
    public String getColumnTypeName(final int column) {
        return null;
    }

    @Override
    public boolean isReadOnly(final int column) {
        return false;
    }

    @Override
    public boolean isWritable(final int column) {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(final int column) {
        return false;
    }

    @Override
    public String getColumnClassName(final int column) {
        return null;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) {
        return false;
    }
}
