package com.ascendix.jdbc.salesforce.resultset;

import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import com.ascendix.jdbc.salesforce.metadata.TypeInfo;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import java.util.List;

public class CommandLogCachedResultSet extends CachedResultSet {

    private static final String LOG_COLUMN = "Log";
    private static final String ID_COLUMN = "Id";

    private static final ColumnMap<String, Object> DEFAULT_COLUMN_MAP = new ColumnMap<String, Object>()
        .add(LOG_COLUMN, "Value", TypeInfo.SHORT_TYPE_INFO)
        .add(ID_COLUMN, "Value", TypeInfo.ID_TYPE_INFO);

    public CommandLogCachedResultSet() {
        super(ForcePreparedStatement.createMetaData(DEFAULT_COLUMN_MAP));
    }

    public CommandLogCachedResultSet(List<String> commandLog) {
        super(commandLog.stream()
            .map(logLine -> new ColumnMap<String, Object>().add(LOG_COLUMN, logLine, TypeInfo.SHORT_TYPE_INFO))
            .toList(), ForcePreparedStatement.createMetaData(DEFAULT_COLUMN_MAP));
    }

    public void log(String logLine) {
        log(logLine, null);
    }

    public void log(String logLine, Object generatedKey) {
        ColumnMap<String, Object> columns = new ColumnMap<>();
        columns.add(LOG_COLUMN, logLine, TypeInfo.SHORT_TYPE_INFO);
        columns.add(ID_COLUMN, generatedKey, TypeInfo.ID_TYPE_INFO);
        addRow(columns);
    }
}
