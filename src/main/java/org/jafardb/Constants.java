package org.jafardb;

public class Constants {
    public static final int PageNumSize = 8;
    public static final int NodeHeaderSize = 3;
    public static final int CounterSize = 4;
    public static final int MagicNumberSize = 4;
    public static final int CollectionSize = 16;

    public static class WriteInsideReadTransactionException extends Exception {
        public WriteInsideReadTransactionException() {
            super("can't perform a write operation inside a read transaction");
        }
    }

    public static class NotJafarDBFile extends Exception {
        public NotJafarDBFile() {
            super("This is not a JafarDB file!");
        }
    }
}
