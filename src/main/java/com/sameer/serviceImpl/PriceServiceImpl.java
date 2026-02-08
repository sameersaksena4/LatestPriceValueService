package com.sameer.serviceImpl;

import com.sameer.model.Batch;
import com.sameer.model.PriceRecord;
import com.sameer.model.State;
import com.sameer.service.PriceService;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * In-memory implementation of PriceService
 * Provides thread-safe access for Producers and consumers
 */
public class PriceServiceImpl implements PriceService {

    /**
     * Map to store all batches
     * Key : BatchId(e.g. "BATCH-1")
     * Value : Batch object containing price records and state
     * Concurrent Hash map is thread-safe, allows concurrent reads and writes
     */
    private final Map<String, Batch> batches = new ConcurrentHashMap<>();

    /**
     * Map to store latest prices per instrument for completed batches
     * Key : Instrument ID
     * Value : Latest Price Record (by asOf time)
     * Concurrent Hash map is thread-safe, allows concurrent reads
     */
    private final Map<String, PriceRecord> latestPrices = new ConcurrentHashMap<>();

    /**
     * Counter for creating unique batch ID's
     * AtomicLong ensures thread-safe increment without synchronization
     */
    private final AtomicLong batchIdCounter = new AtomicLong(0);

    /**
     * Lock for atomic batch completion operations
     */
    private final Object completionLock = new Object();

    @Override
    public String startBach() {
        String batchId = "BATCH-" + batchIdCounter.incrementAndGet();
        batches.put(batchId, new Batch(batchId));
        return batchId;
    }

    @Override
    public void uploadChunk(String batchId, List<PriceRecord> records) throws Exception {
        if(batchId == null || batchId.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid batch Id");
        }
        if(records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records cannot be null");
        }
        Batch batch = batches.get(batchId);
        if(batch == null) {
            throw new IllegalArgumentException("Batch does not exist");
        }
        if(batch.getState() != State.STARTED) {
            throw new IllegalStateException("Invalid batch state");
        }
        batch.addRecords(records);
    }

    @Override
    public void completeBatch(String batchId) throws Exception {
        if(batchId == null || batchId.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid batch Id");
        }
        Batch batch = batches.get(batchId);
        if(batch == null) {
            throw new IllegalArgumentException("Batch does not exist");
        }
        synchronized (completionLock) {
            // Double-checked locking
            if(batch.getState() != State.STARTED) {
                throw new IllegalStateException("Invalid batch state");
            }
            // Get all records from the batch
            List<PriceRecord> records = batch.getRecords();

            for(PriceRecord record : records) {
                updateLatestPrice(record);
            }

            // Mark batch as completed
            batch.complete();
        }
    }

    @Override
    public void cancelBatch(String batchId) throws Exception {
        if(batchId == null || batchId.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid batch Id");
        }
        Batch batch = batches.get(batchId);
        if(batch == null) {
            throw new IllegalArgumentException("Batch does not exist");
        }
        batch.cancel();
        // No need to add records of the batch to latestPrices
        // As consumers only need to see data from completed batches
    }

    @Override
    public Map<String, PriceRecord> getLastPrices(List<String> ids) {
        if(ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("Ids cannot be null");
        }
        // Return prices from completed batches
        // latestPrices map only contains record from completed batches
        return ids.stream()
                .filter(Objects::nonNull)  // Remove any null elements
                .filter(id -> !id.trim().isEmpty())  // Remove empty strings
                .filter(latestPrices::containsKey) // Keep IDs that exist as key in latestPrices map
                .collect(Collectors.toMap(        // Build a map from the remaining IDs
                    id -> id, latestPrices::get,
                        (existing, replacement) -> existing // Resolve collisions that occurs
                                                                                  // if same id appears more than once in ids
                ));
    }

    /**
     * Updates the latest record for an instrument if the new record has a more recent asOf time
     * Uses ConcurrentHashMap.compute() to atomically update
     * If no existing record, stores the new one
     * If existing, compares by 'asOf' and keeps the latest
     */
    private void updateLatestPrice(PriceRecord record) {
        latestPrices.compute(record.getId(), (id, existing) -> {
            if(existing == null) {
                return record;
            }
            LocalDateTime existingAsOf = existing.getAsOf();
            LocalDateTime newAsOf = record.getAsOf();
            return newAsOf.isAfter(existingAsOf) ? record : existing;
        });
    }
}
