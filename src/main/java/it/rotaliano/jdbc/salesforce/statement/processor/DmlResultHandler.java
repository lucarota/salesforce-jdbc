package it.rotaliano.jdbc.salesforce.statement.processor;

import it.rotaliano.jdbc.salesforce.resultset.CommandLogCachedResultSet;
import com.sforce.ws.ConnectionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DmlResultHandler {

    @FunctionalInterface
    public interface DmlAction {
        void execute(CommandLogCachedResultSet resultSet) throws ConnectionException;
    }

    /**
     * Creates a CommandLogCachedResultSet, executes a DML action, and handles ConnectionException.
     *
     * @param operationName human-readable operation name for error messages
     * @param action        the action to execute; receives the result set for populating
     * @return the populated result set
     */
    public static CommandLogCachedResultSet execute(String operationName, DmlAction action) {
        CommandLogCachedResultSet resultSet = new CommandLogCachedResultSet();
        try {
            action.execute(resultSet);
        } catch (ConnectionException e) {
            resultSet.addWarning("Failed request to " + operationName + " with error: " + e.getMessage());
            log.error("Failed request to {} with error: {}", operationName, e.getMessage(), e);
        }
        return resultSet;
    }
}
