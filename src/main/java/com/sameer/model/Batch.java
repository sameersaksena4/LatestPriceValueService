package com.sameer.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a batch of price records being processed
 * Tracks the state and records of a batch, and ensure thread-safe operations
 */
public class Batch {
    /**
     * Unique identifier
     */
    private final String batchId;
    /**
     * Current state (volatile for visibility across threads)
     */
    private volatile State state;
    /**
     * List of price records
     */
    private final List<PriceRecord> records;
    /**
     * For thread-safe access
     * Read-write locks allow concurrent reads
     * synchronized blocks all operations (read and write)
     * Reentrant : same thread can acquire the lock multiple times
     */
    private final ReadWriteLock lock;

    public Batch(String batchId) {
        this.batchId = batchId;
        this.state = State.STARTED;
        this.records = new ArrayList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    public String getBatchId() {
        return batchId;
    }

    /**
     * Uses a read lock as multiple users are allowed
     * @return the current state atomically
     */
    public State getState() {
        lock.readLock().lock();
        try {
            return state;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Uses a write lock (exclusive)
     * Validate if batch is started
     * @param newRecords are added to the batch
     */
    public void addRecords(List<PriceRecord> newRecords) {
        lock.writeLock().lock();
        try {
            if(state != State.STARTED) {
                throw new IllegalStateException("Cannot add records to batch " + batchId);
            }
            records.addAll(newRecords);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @return a deep copy to prevent external modification
     */
    public List<PriceRecord> getRecords() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(records);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void complete() {
        lock.writeLock().lock();
        try {
            if(state != State.STARTED) {
                throw new IllegalStateException("Cannot complete batch" + batchId);
            }
            state = State.COMPLETED;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void cancel() {
        lock.writeLock().lock();
        try {
            if(state != State.STARTED) {
                throw new IllegalStateException("Cannot cancel batch" + batchId);
            }
            state = State.CANCELLED;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
