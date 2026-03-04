package it.rotaliano.jdbc.salesforce.exceptions;

/**
 * Unchecked exception for Salesforce JDBC driver errors.
 * Use this when a method signature doesn't allow checked exceptions
 * (e.g., Iterator methods, lambda expressions).
 */
public class SalesforceRuntimeException extends RuntimeException {

    public SalesforceRuntimeException(String message) {
        super(message);
    }

    public SalesforceRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SalesforceRuntimeException(Throwable cause) {
        super(cause);
    }
}
