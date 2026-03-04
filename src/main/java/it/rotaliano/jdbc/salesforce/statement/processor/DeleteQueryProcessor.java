package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.resultset.CachedResultSet;
import it.rotaliano.jdbc.salesforce.resultset.CommandLogCachedResultSet;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.IError;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteQueryProcessor {

    public static CachedResultSet processQuery(List<Object> parameters, PartnerService partnerService,
        DeleteQueryAnalyzer deleteQueryAnalyzer) {

        String fromObjectName = deleteQueryAnalyzer.getFromObjectName();

        CommandLogCachedResultSet resultSet = DmlResultHandler.execute(
            "delete " + fromObjectName + " entities",
            rs -> {
                List<String> recordsToDelete = deleteQueryAnalyzer.getRecords(parameters);
                DeleteResult[] records = partnerService.deleteRecords(recordsToDelete);
                for (DeleteResult result : records) {
                    if (result.isSuccess()) {
                        rs.setId(result.getId());
                    } else {
                        rs.addWarning(fromObjectName + " failed to delete with error="
                            + Arrays.stream(result.getErrors()).map(IError::getMessage)
                                .collect(Collectors.joining(",")));
                    }
                }
            });

        return resultSet;
    }
}
