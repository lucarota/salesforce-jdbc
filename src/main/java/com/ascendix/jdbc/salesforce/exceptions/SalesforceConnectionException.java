package com.ascendix.jdbc.salesforce.exceptions;

/**
 * Exception thrown when a connection to Salesforce cannot be established or is lost.
 */
public class SalesforceConnectionException extends SalesforceJdbcException {

    public SalesforceConnectionException(String message) {
        super(message);
    }

    public SalesforceConnectionException(String message, Throwable cause) {
        super(message, cause);
    }

    public SalesforceConnectionException(Throwable cause) {
        super(cause);
    }
}
