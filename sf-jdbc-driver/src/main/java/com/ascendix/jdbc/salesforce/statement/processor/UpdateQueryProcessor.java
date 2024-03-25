package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import com.ascendix.jdbc.salesforce.resultset.CommandLogCachedResultSet;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement;
import com.sforce.soap.partner.IError;
import com.sforce.soap.partner.ISaveResult;
import com.sforce.ws.ConnectionException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateQueryProcessor {

    public static CachedResultSet processQuery(ForcePreparedStatement statement, String soqlQuery,
        PartnerService partnerService, UpdateQueryAnalyzer updateQueryAnalyzer) {
        CommandLogCachedResultSet resultSet = new CommandLogCachedResultSet();
        if (soqlQuery == null || soqlQuery.trim().isEmpty()) {
            resultSet.log("No UPDATE query found");
            return resultSet;
        }

        try {
            int updateCount = 0;
            List<Map<String, Object>> recordsToUpdate = updateQueryAnalyzer.getRecords();
            ISaveResult[] records = partnerService.saveRecords(updateQueryAnalyzer.getFromObjectName(),
                recordsToUpdate);
            for (ISaveResult result : records) {
                if (result.isSuccess()) {
                    resultSet.log(updateQueryAnalyzer.getFromObjectName() + " updated with Id=" + result.getId());
                    updateCount++;
                } else {
                    resultSet.addWarning(updateQueryAnalyzer.getFromObjectName() + " failed to update with error="
                        + Arrays.stream(result.getErrors()).map(IError::getMessage).collect(Collectors.joining(",")));
                }
            }
            statement.setUpdateCount(updateCount);
            statement.setResultSet(resultSet);
        } catch (ConnectionException e) {
            resultSet.addWarning("Failed request to update entities with error: " + e.getMessage());
            log.error("Failed request to update entities with error: {}", e.getMessage(), e);
        }
        return resultSet;
    }
}
