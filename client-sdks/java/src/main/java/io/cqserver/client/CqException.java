package io.cqserver.client;

/** Raised for server-reported errors and protocol violations. */
public class CqException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CqException(String message) {
        super(message);
    }

    public CqException(String message, Throwable cause) {
        super(message, cause);
    }
}
