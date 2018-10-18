package io.debezium.connector.mongodb;

public class MongoPrimaryChangedException extends java.lang.RuntimeException {
    public MongoPrimaryChangedException(String message) {
        super(message);
    }
}
