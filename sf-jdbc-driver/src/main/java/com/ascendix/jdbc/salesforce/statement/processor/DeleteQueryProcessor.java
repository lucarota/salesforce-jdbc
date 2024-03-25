package com.ascendix.jdbc.salesforce.statement.processor;

import com.ascendix.jdbc.salesforce.delegates.PartnerService;
import com.ascendix.jdbc.salesforce.resultset.CachedResultSet;
import com.ascendix.jdbc.salesforce.resultset.CommandLogCachedResultSet;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.IError;
import com.sforce.ws.ConnectionException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeleteQueryProcessor {

    public static CachedResultSet processQuery(String soqlQuery, PartnerService partnerService,
        DeleteQueryAnalyzer deleteQueryAnalyzer) {
        CommandLogCachedResultSet resultSet = new CommandLogCachedResultSet();
        if (soqlQuery == null || soqlQuery.trim().isEmpty()) {
            resultSet.log("No DELETE query found");
            return resultSet;
        }

        try {
            List<String> recordsToDelete = deleteQueryAnalyzer.getRecords();
            DeleteResult[] records = partnerService.deleteRecords(recordsToDelete);
            for (DeleteResult result : records) {
                if (result.isSuccess()) {
                    resultSet.log(deleteQueryAnalyzer.getFromObjectName() + " deleted with Id=" + result.getId());
                } else {
                    resultSet.addWarning(deleteQueryAnalyzer.getFromObjectName() + " failed to delete with error="
                        + Arrays.stream(result.getErrors()).map(IError::getMessage).collect(Collectors.joining(",")));
                }
            }
        } catch (ConnectionException e) {
            resultSet.addWarning("Failed request to delete entities with error: " + e.getMessage());
            log.error("Failed request to delete entities with error: {}", e.getMessage(), e);
        }
        return resultSet;
    }
}
