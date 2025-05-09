package com.pinnacle.payloadconsumer.exception;


public class ExceptionTestRunner {

    public static void main(String[] args) {
        testPayloadParsingException();
        testDatabaseOperationException();
        testTableCreationException();
        testGenericPayloadConsumerException();
    }

    private static void testPayloadParsingException() {
        try {
            throw new PayloadParsingException("Test: Payload parsing failed", new RuntimeException("Invalid JSON"));
        } catch (PayloadParsingException e) {
            System.out.println("Caught PayloadParsingException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testDatabaseOperationException() {
        try {
            throw new DatabaseOperationException("Test: DB insert failed", new RuntimeException("Constraint violation"));
        } catch (DatabaseOperationException e) {
            System.out.println("Caught DatabaseOperationException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testTableCreationException() {
        try {
            throw new TableCreationException("Test: Table creation failed", new RuntimeException("SQL syntax error"));
        } catch (TableCreationException e) {
            System.out.println("Caught TableCreationException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void testGenericPayloadConsumerException() {
        try {
            throw new PayloadConsumerException("Test: Generic payload consumer failure", new RuntimeException("Unexpected"));
        } catch (PayloadConsumerException e) {
            System.out.println("Caught PayloadConsumerException: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
