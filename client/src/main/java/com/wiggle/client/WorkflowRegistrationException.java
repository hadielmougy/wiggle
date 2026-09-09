package com.wiggle.client;

public class WorkflowRegistrationException extends RuntimeException {
    public WorkflowRegistrationException(String s, Exception e) {
        super(s, e);
    }
}
