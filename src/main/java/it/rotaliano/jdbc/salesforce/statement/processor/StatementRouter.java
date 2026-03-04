package it.rotaliano.jdbc.salesforce.statement.processor;

import lombok.extern.slf4j.Slf4j;

/**
 * Routes SQL statements to the appropriate processor based on statement type.
 *
 * <p>This class centralizes the logic for determining the type of a SQL statement,
 * making the code more maintainable and testable.
 */
@Slf4j
public class StatementRouter {

    private static final String KEEP_ALIVE_QUERY = "SELECT 'keep alive'";

    /**
     * Determines the statement type for a given query.
     *
     * @param soqlQuery the query to analyze
     * @param queryAnalyzer the analyzer to use (may be null for simple checks)
     * @return the StatementTypeEnum for this query
     */
    public static StatementTypeEnum getStatementType(String soqlQuery, QueryAnalyzer queryAnalyzer) {
        if (soqlQuery == null || soqlQuery.trim().isEmpty()) {
            return StatementTypeEnum.UNDEFINED;
        }

        if (isKeepAliveQuery(soqlQuery)) {
            return StatementTypeEnum.SELECT;
        }

        if (AdminQueryProcessor.isAdminQuery(soqlQuery)) {
            return StatementTypeEnum.SELECT; // Admin queries return ResultSet like SELECT
        }

        if (queryAnalyzer != null) {
            if (queryAnalyzer.analyse(soqlQuery, StatementTypeEnum.INSERT)) {
                return StatementTypeEnum.INSERT;
            }
            if (queryAnalyzer.analyse(soqlQuery, StatementTypeEnum.UPDATE)) {
                return StatementTypeEnum.UPDATE;
            }
            if (queryAnalyzer.analyse(soqlQuery, StatementTypeEnum.DELETE)) {
                return StatementTypeEnum.DELETE;
            }
        }

        return StatementTypeEnum.SELECT;
    }

    /**
     * Checks if the query is a keep-alive query.
     *
     * @param soqlQuery the query to check
     * @return true if it's a keep-alive query
     */
    public static boolean isKeepAliveQuery(String soqlQuery) {
        return KEEP_ALIVE_QUERY.equals(soqlQuery);
    }

    /**
     * Checks if the query is an admin query (CACHE, CONNECT commands).
     *
     * @param soqlQuery the query to check
     * @return true if it's an admin query
     */
    public static boolean isAdminQuery(String soqlQuery) {
        return AdminQueryProcessor.isAdminQuery(soqlQuery);
    }

    /**
     * Checks if the query is a DML statement (INSERT, UPDATE, DELETE).
     *
     * @param soqlQuery the query to check
     * @param queryAnalyzer the analyzer to use
     * @return true if it's a DML statement
     */
    public static boolean isDmlStatement(String soqlQuery, QueryAnalyzer queryAnalyzer) {
        StatementTypeEnum type = getStatementType(soqlQuery, queryAnalyzer);
        return type == StatementTypeEnum.INSERT ||
               type == StatementTypeEnum.UPDATE ||
               type == StatementTypeEnum.DELETE;
    }

    /**
     * Checks if the query is a SELECT statement.
     *
     * @param soqlQuery the query to check
     * @param queryAnalyzer the analyzer to use
     * @return true if it's a SELECT statement
     */
    public static boolean isSelectStatement(String soqlQuery, QueryAnalyzer queryAnalyzer) {
        return getStatementType(soqlQuery, queryAnalyzer) == StatementTypeEnum.SELECT;
    }
}
