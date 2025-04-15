package com.ascendix.jdbc.salesforce.resultset;

import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import com.ascendix.jdbc.salesforce.metadata.TypeInfo;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;

public class CommandLogCachedResultSet extends CachedResultSet {

    private static final String ID_COLUMN = "Id";

    private static final ColumnMap<String, Object> DEFAULT_COLUMN_MAP = new ColumnMap<String, Object>()
        .add(ID_COLUMN, "Value", TypeInfo.ID_TYPE_INFO);

    public CommandLogCachedResultSet() {
        super(ForcePreparedStatement.createMetaData(DEFAULT_COLUMN_MAP));
    }

    public void setId(Object generatedKey) {
        ColumnMap<String, Object> columns = new ColumnMap<>();
        columns.add(ID_COLUMN, generatedKey, TypeInfo.ID_TYPE_INFO);
        addRow(columns);
    }
}
