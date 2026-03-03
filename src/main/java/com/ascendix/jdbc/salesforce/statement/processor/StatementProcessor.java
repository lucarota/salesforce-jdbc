package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;

import java.sql.SQLException;

/**
 * Strategy interface for processing different types of SQL statements.
 * Implementations handle specific statement types (SELECT, INSERT, UPDATE, DELETE, ADMIN).
 */
@FunctionalInterface
public interface StatementProcessor {

    /**
     * Processes the statement and returns a result set.
     *
     * @param context the execution context containing all necessary data
     * @return CachedResultSet with the results
     * @throws SQLException if execution fails
     */
    CachedResultSet process(StatementContext context) throws SQLException;
}
