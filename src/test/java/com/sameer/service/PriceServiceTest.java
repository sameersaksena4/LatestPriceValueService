package com.sameer.service;

import com.sameer.model.PriceRecord;
import com.sameer.serviceImpl.PriceServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class PriceServiceTest {

    private PriceService priceService;

    @BeforeEach
    void setUp() {
        priceService = new PriceServiceImpl();
    }

    @Test
    void testStartBatch() {
        String batchId = priceService.startBach();
        assertNotNull(batchId);
        assertFalse(batchId.isEmpty());
    }

    // Helper method to create price records
    private List<PriceRecord> createPriceRecords(String instrumentId, int count) {
        LocalDateTime baseTime = LocalDateTime.now();
        return IntStream.range(0, count)
                .mapToObj(i -> new PriceRecord(instrumentId, baseTime.plusNanos(i), "Price" + i))
                .collect(Collectors.toList());
    }

    @Test
    void testCompleteBatchWithSingleChunk() throws Exception {
        String batchId = priceService.startBach();
        List<PriceRecord> records = createPriceRecords("INSTR1", 5);
        priceService.uploadChunk(batchId, records);
        priceService.completeBatch(batchId);
        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1"));
        assertEquals(1, lastPrices.size());
        assertTrue(lastPrices.containsKey("INSTR1"));
    }

    @Test
    void testCompleteBatchWithMultipleChunks() throws Exception {
        String batchId = priceService.startBach();
        // Upload multiple chunks
        priceService.uploadChunk(batchId, createPriceRecords("INSTR1", 1000));
        priceService.uploadChunk(batchId, createPriceRecords("INSTR2", 1000));
        priceService.uploadChunk(batchId, createPriceRecords("INSTR3", 1000));
        priceService.completeBatch(batchId);
        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1", "INSTR2", "INSTR3"));
        assertEquals(3, lastPrices.size());
    }

    @Test
    void testCancelBatch() throws Exception {
        String batchId = priceService.startBach();
        List<PriceRecord> records = createPriceRecords("INSTR1", 100);
        priceService.uploadChunk(batchId, records);
        priceService.cancelBatch(batchId);
        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1"));
        assertTrue(lastPrices.isEmpty());
    }

    @Test
    void testLastPriceDeterminesAsOfTime() throws Exception {
        String batchId1 = priceService.startBach();
        LocalDateTime earlier = LocalDateTime.now().minusHours(2);
        LocalDateTime later = LocalDateTime.now();

        // Upload record with earlier time
        priceService.uploadChunk(batchId1, Arrays.asList(new PriceRecord("INSTR1", earlier, "Price1")));
        priceService.completeBatch(batchId1);

        String batchId2 = priceService.startBach();
        // Upload record with later time
        priceService.uploadChunk(batchId2, Arrays.asList(new PriceRecord("INSTR1", later, "Price2")));
        priceService.completeBatch(batchId2);

        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1"));
        assertEquals(1, lastPrices.size());
        PriceRecord lastPrice = lastPrices.get("INSTR1");
        assertEquals("Price2", lastPrice.getPayload());
        assertEquals(later, lastPrice.getAsOf());
    }

    @Test
    void testConsumersDonNotSeeIncompleteBatches() throws Exception {
        String batchId = priceService.startBach();
        priceService.uploadChunk(batchId, createPriceRecords("INSTR1", 100));

        // Consumer should not see incomplete batch
        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1"));
        assertTrue(lastPrices.isEmpty());

        // After completion, consumer should see the data
        priceService.completeBatch(batchId);
        lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1"));
        assertEquals(1, lastPrices.size());
    }

    @Test
    void testAtomicBatchCompletion() throws Exception {
        String batchId = priceService.startBach();

        priceService.uploadChunk(batchId, createPriceRecords("INSTR1", 1000));
        priceService.uploadChunk(batchId, createPriceRecords("INSTR2", 1000));

        // Before completion, nothing should be visible
        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1", "INSTR2"));
        assertTrue(lastPrices.isEmpty());

        priceService.completeBatch(batchId);

        // After completion, all records should be visible at once
        lastPrices = priceService.getLastPrices(Arrays.asList("INSTR1", "INSTR2"));
        assertEquals(2, lastPrices.size());
        assertTrue(lastPrices.containsKey("INSTR1"));
        assertTrue(lastPrices.containsKey("INSTR2"));
    }

    @Test
    void testCancelThenComplete() throws Exception {
        String batchId = priceService.startBach();
        priceService.uploadChunk(batchId, createPriceRecords("INSTR1", 100));
        priceService.cancelBatch(batchId);

        // Cannot complete cancelled batch
        assertThrows(IllegalStateException.class, () -> {
            priceService.completeBatch(batchId);
        });
    }

    @Test
    void testConcurrentBatchOperations() throws Exception {
        int numBatches = 10;
        int recordsPerBatch = 100;
        ExecutorService executor = Executors.newFixedThreadPool(20);

        List<Future<String>> batchFutures = new ArrayList<>();
        // Start multiple batches concurrently
        for(int i = 0; i < numBatches; i++) {
            final int batchNum = i;
            Future<String> future = executor.submit(() -> {
                String batchId = priceService.startBach();
                // Upload chunks in parallel
                List<Future<?>> chunkFutures = new ArrayList<>();
                for(int j = 0; j < 5; j++) {
                    chunkFutures.add(executor.submit(() -> {
                        List<PriceRecord> records = createPriceRecords("INSTR" + batchNum, recordsPerBatch / 5);
                        try {
                            priceService.uploadChunk(batchId, records);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
                // Wait for all chunks
                for(Future<?> f : chunkFutures) {
                    try {
                        f.get();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                }
                try {
                    priceService.completeBatch(batchId);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return batchId;
            });
            batchFutures.add(future);
        }
        // Wait for all batches to complete
        for(Future<String> future : batchFutures) {
            future.get();
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        // Verify all prices are available
        List<String> ids = IntStream.range(0, numBatches)
                .mapToObj(i -> "INSTR" + i)
                .toList();
        Map<String, PriceRecord> lastPrices = priceService.getLastPrices(ids);
        assertEquals(numBatches, lastPrices.size());
    }

}
