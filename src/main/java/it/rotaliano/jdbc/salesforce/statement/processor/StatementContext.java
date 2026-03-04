package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.statement.ForcePreparedStatement;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * Context object containing all data needed to execute a statement.
 * Used by StatementProcessor implementations to avoid long parameter lists.
 */
@Getter
@Builder
public class StatementContext {

    private final ForcePreparedStatement statement;
    private final String soqlQuery;
    private final List<Object> parameters;
    private final PartnerService partnerService;
    private final QueryAnalyzer queryAnalyzer;

    /**
     * Creates a context from a ForcePreparedStatement.
     */
    public static StatementContext from(ForcePreparedStatement statement, String soqlQuery,
            List<Object> parameters, PartnerService partnerService, QueryAnalyzer queryAnalyzer) {
        return StatementContext.builder()
                .statement(statement)
                .soqlQuery(soqlQuery)
                .parameters(parameters)
                .partnerService(partnerService)
                .queryAnalyzer(queryAnalyzer)
                .build();
    }
}
