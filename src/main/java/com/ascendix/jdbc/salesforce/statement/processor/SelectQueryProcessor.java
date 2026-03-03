package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.ForceResultField;
import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.sforce.ws.ConnectionException;
import lombok.extern.slf4j.Slf4j;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Processor for SELECT queries.
 * Handles both cached and streaming query execution.
 */
@Slf4j
public class SelectQueryProcessor {

    /**
     * Processes a SELECT query and returns the result set.
     *
     * @param ctx the statement context
     * @return CachedResultSet with the query results
     * @throws SQLException if query execution fails
     */
    public static CachedResultSet processQuery(StatementContext ctx) throws SQLException {
        log.trace("[SelectProcessor] Processing SELECT query");

        ForcePreparedStatement stmt = ctx.getStatement();

        try {
            ResultSetMetaData metaData = stmt.getMetaData();
            String preparedQuery = stmt.prepareQueryForExecution();

            if (stmt.isNoCacheMode()) {
                stmt.setNeverQueriedMore(true);
                return new CachedResultSet(stmt, metaData);
            }

            List<List<ForceResultField>> forceQueryResult = ctx.getPartnerService()
                    .query(preparedQuery, stmt.getRootEntityFieldDefinitions());

            if (!forceQueryResult.isEmpty()) {
                List<ColumnMap<String, Object>> maps = Collections.synchronizedList(new LinkedList<>());
                forceQueryResult.forEach(rec -> maps.add(stmt.convertToColumnMap(rec)));
                return new CachedResultSet(maps, metaData);
            } else {
                return new CachedResultSet(Collections.emptyList(), metaData);
            }
        } catch (ConnectionException e) {
            throw new SQLException(e);
        }
    }
}
