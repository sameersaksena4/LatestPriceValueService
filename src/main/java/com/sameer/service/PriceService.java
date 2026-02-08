package com.sameer.service;

import com.sameer.model.PriceRecord;

import java.util.List;
import java.util.Map;

/**
 * Service interface for managing price records for financial instruments
 * Supports batch operations for producers and query operations for consumers
 */
public interface PriceService {

    /**
     * Starts a new batch
     * @return a unique batch ID
     */
    String startBach();

    /**
     * Upload a chunk of price records to the specified batch
     * @param batchId same as that returned by startBatch()
     * @param records List of price records up to 1000 price records
     * @throws Exception if batchId is invalid or batch is not in started state
     */
    void uploadChunk(String batchId, List<PriceRecord> records) throws Exception;

    /**
     * Completes a batch, making all its price records available for consumption atomically
     * @param batchId the batch ID to complete
     * @throws Exception if batchId is invalid or batch is not in started state
     */
    void completeBatch(String batchId) throws Exception;

    /**
     * Cancels a batch, discarding all its price records
     * @param batchId the batch ID to cancel
     * @throws Exception if batchId is invalid or batch is not in started state
     */
    void cancelBatch(String batchId) throws Exception;

    /**
     * Retrieves the last price records for the given instrument IDs
     * The last value is determined by the asOf time, as set by the producer, and not the order they came in
     * Only returns data from completed batches
     * @param ids list of instrument IDs to query
     * @return map of instrument ID to the latest PriceRecord (based on asOf time)
     */
    Map<String, PriceRecord> getLastPrices(List<String> ids);
}
