package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.resultset.CachedResultSet;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;

/**
 * Factory that creates and returns the appropriate StatementProcessor
 * based on the SQL statement type.
 */
@Slf4j
public class StatementProcessorFactory {

    private static final String KEEP_ALIVE_QUERY = "SELECT 'keep alive'";

    /**
     * Returns a processor for the given context.
     * Analyzes the query and returns the appropriate processor.
     *
     * @param context the statement context
     * @return the appropriate StatementProcessor
     */
    public static StatementProcessor getProcessor(StatementContext context) {
        String soqlQuery = context.getSoqlQuery();

        // Keep-alive query
        if (KEEP_ALIVE_QUERY.equals(soqlQuery)) {
            log.trace("[Factory] Keep alive query");
            return ctx -> CachedResultSet.EMPTY;
        }

        // Admin queries (CONNECT commands)
        if (AdminQueryProcessor.isAdminQuery(soqlQuery)) {
            log.trace("[Factory] Admin query");
            return ctx -> AdminQueryProcessor.processQuery(ctx.getStatement(), ctx.getSoqlQuery());
        }

        // DML statements
        QueryAnalyzer analyzer = context.getQueryAnalyzer();
        if (analyzer != null) {
            if (analyzer.analyse(soqlQuery, StatementTypeEnum.INSERT)) {
                log.trace("[Factory] INSERT query");
                return StatementProcessorFactory::processInsert;
            }
            if (analyzer.analyse(soqlQuery, StatementTypeEnum.UPDATE)) {
                log.trace("[Factory] UPDATE query");
                return StatementProcessorFactory::processUpdate;
            }
            if (analyzer.analyse(soqlQuery, StatementTypeEnum.DELETE)) {
                log.trace("[Factory] DELETE query");
                return StatementProcessorFactory::processDelete;
            }
        }

        // Default: SELECT query
        log.trace("[Factory] SELECT query");
        return SelectQueryProcessor::processQuery;
    }

    private static CachedResultSet processInsert(StatementContext ctx) throws SQLException {
        InsertQueryAnalyzer insertAnalyzer = new InsertQueryAnalyzer(ctx.getQueryAnalyzer());
        return InsertQueryProcessor.processQuery(
                ctx.getStatement(),
                ctx.getParameters(),
                ctx.getPartnerService(),
                insertAnalyzer);
    }

    private static CachedResultSet processUpdate(StatementContext ctx) throws SQLException {
        UpdateQueryAnalyzer updateAnalyzer = new UpdateQueryAnalyzer(ctx.getQueryAnalyzer());
        return UpdateQueryProcessor.processQuery(
                ctx.getStatement(),
                ctx.getParameters(),
                ctx.getPartnerService(),
                updateAnalyzer);
    }

    private static CachedResultSet processDelete(StatementContext ctx) throws SQLException {
        DeleteQueryAnalyzer deleteAnalyzer = new DeleteQueryAnalyzer(ctx.getQueryAnalyzer());
        return DeleteQueryProcessor.processQuery(
                ctx.getParameters(),
                ctx.getPartnerService(),
                deleteAnalyzer);
    }
}
