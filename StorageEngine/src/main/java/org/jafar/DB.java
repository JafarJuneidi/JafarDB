package org.jafar;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DB {

    private final ReadWriteLock rwlock;
    private final DAL dal;

    public ReadWriteLock getRwlock() {
        return rwlock;
    }

    public DAL getDal() {
        return dal;
    }

    /**
     * Constructor for the DB class.
     * @param dal Data access layer instance.
     */
    private DB(DAL dal) {
        this.rwlock = new ReentrantReadWriteLock();
        this.dal = dal;
    }

    /**
     * Opens a new DB instance.
     * @param path The path to the database.
     * @param options Options for the DB.
     * @return A new DB instance.
     * @throws IOException if there's an issue opening the DB.
     */
    public static DB open(String path, Options options) throws IOException, Constants.NotJafarDBFile {
        DAL dal = new DAL(path, options);
        return new DB(dal);
    }

    /**
     * Closes the DB instance.
     * @throws IOException if there's an issue closing the DB.
     */
    public void close() throws IOException {
        dal.close();
    }

    /**
     * Acquires a read lock and returns a read-only transaction.
     * @return A new read-only transaction.
     */
    public Transaction readTransaction() {
        rwlock.readLock().lock();
        return new Transaction(this, false);
    }

    /**
     * Acquires a write lock and returns a writable transaction.
     * @return A new writable transaction.
     */
    public Transaction writeTransaction() {
        rwlock.writeLock().lock();
        return new Transaction(this, true);
    }
}
