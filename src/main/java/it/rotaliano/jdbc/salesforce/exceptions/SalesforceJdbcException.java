package it.rotaliano.jdbc.salesforce.exceptions;

import java.sql.SQLException;

/**
 * Base exception for all Salesforce JDBC driver errors.
 * Extends SQLException to maintain JDBC compatibility.
 */
public class SalesforceJdbcException extends SQLException {

    public SalesforceJdbcException(String message) {
        super(message);
    }

    public SalesforceJdbcException(String message, Throwable cause) {
        super(message, cause);
    }

    public SalesforceJdbcException(Throwable cause) {
        super(cause);
    }
}
