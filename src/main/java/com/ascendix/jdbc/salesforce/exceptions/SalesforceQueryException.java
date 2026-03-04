package com.ascendix.jdbc.salesforce.exceptions;

/**
 * Exception thrown when a SOQL query fails to execute or parse.
 */
public class SalesforceQueryException extends SalesforceJdbcException {

    public SalesforceQueryException(String message) {
        super(message);
    }

    public SalesforceQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public SalesforceQueryException(Throwable cause) {
        super(cause);
    }
}
