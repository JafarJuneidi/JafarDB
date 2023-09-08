package org.example;

public class Constants {
    public static final int pageNumSize = 8;
    public static final int nodeHeaderSize = 3;

    public static class WriteInsideReadTransactionException extends Exception {
        public WriteInsideReadTransactionException() {
            super("can't perform a write operation inside a read transaction");
        }
    }
}
