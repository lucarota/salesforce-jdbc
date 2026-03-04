package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.resultset.CachedResultSet;
import it.rotaliano.jdbc.salesforce.resultset.CommandLogCachedResultSet;
import it.rotaliano.jdbc.salesforce.statement.ForcePreparedStatement;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.ISaveResult;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdateQueryProcessor {

    public static CachedResultSet processQuery(ForcePreparedStatement statement, List<Object> parameters,
        PartnerService partnerService, UpdateQueryAnalyzer updateQueryAnalyzer) {

        String fromObjectName = updateQueryAnalyzer.getFromObjectName();
        int[] updateCount = {0};

        CommandLogCachedResultSet resultSet = DmlResultHandler.execute(
            "update " + fromObjectName + " entities",
            rs -> {
                List<Map<String, Object>> recordsToUpdate = updateQueryAnalyzer.getRecords(parameters);
                ISaveResult[] records = partnerService.saveRecords(fromObjectName, recordsToUpdate);
                for (ISaveResult result : records) {
                    if (result.isSuccess()) {
                        rs.setId(result.getId());
                        updateCount[0]++;
                    } else {
                        rs.addWarning(fromObjectName + " failed to update with error="
                            + Arrays.stream(result.getErrors()).map(IError::getMessage)
                                .collect(Collectors.joining(",")));
                    }
                }
                statement.setUpdateCount(updateCount[0]);
                statement.setResultSet(rs);
            });

        return resultSet;
    }
}
