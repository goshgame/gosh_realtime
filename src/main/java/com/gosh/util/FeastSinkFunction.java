package com.gosh.util;

import com.gosh.entity.ApiResponse;
import com.gosh.entity.FeastRequest;
import com.gosh.process.FeastApi;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Flink Sink Function for writing FeastRequest to Feast online store.
 * This implementation is asynchronous and batched to improve performance and
 * throughput.
 */
public class FeastSinkFunction extends RichSinkFunction<FeastRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(FeastSinkFunction.class);

    private final String jobName;
    private final int batchSize;
    private final long batchIntervalMs;
    private final int maxRetries;

    // Runtime state (transient)
    private transient List<FeastRequest> buffer;
    private transient ExecutorService executorService;
    private transient ScheduledExecutorService flusher;
    private transient volatile boolean isRunning;
    private transient volatile Exception asyncError;

    /**
     * Default constructor for Flink serialization.
     */
    public FeastSinkFunction() {
        this("unknown-job", 100, 5000, 3);
    }

    /**
     * Constructor with job name and default batching/retry settings.
     * 
     * @param jobName The name of the Flink job for logging purposes.
     */
    public FeastSinkFunction(String jobName) {
        this(jobName, 100, 5000, 3);
    }

    /**
     * Full constructor with detailed configuration.
     * 
     * @param jobName         The name of the Flink job.
     * @param batchSize       The number of records to buffer before writing.
     * @param batchIntervalMs The maximum time to wait before writing, even if the
     *                        buffer is not full.
     * @param maxRetries      The maximum number of retries for a failed write
     *                        attempt.
     */
    public FeastSinkFunction(String jobName, int batchSize, long batchIntervalMs, int maxRetries) {
        this.jobName = jobName;
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.maxRetries = maxRetries;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.buffer = new ArrayList<>();
        this.executorService = Executors.newFixedThreadPool(10); // Concurrency of 10
        this.flusher = Executors.newSingleThreadScheduledExecutor();
        this.isRunning = true;

        // Schedule a periodic flush to ensure data is not buffered for too long
        flusher.scheduleAtFixedRate(() -> {
            if (isRunning) {
                try {
                    flush();
                } catch (Exception e) {
                    LOG.error("FeastSink periodic flusher failed.", e);
                    asyncError = e; // Store exception to be re-thrown in the main Flink thread
                }
            }
        }, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);

        LOG.info("FeastSinkFunction for job '{}' opened with batchSize={}, intervalMs={}, retries={}.",
                jobName, batchSize, batchIntervalMs, maxRetries);
    }

    @Override
    public void invoke(FeastRequest value, Context context) throws Exception {
        checkAsyncError();

        if (value == null) {
            LOG.warn("FeastRequest is null, skipping write to online store.");
            return;
        }

        List<FeastRequest> batchToFlush = null;
        synchronized (buffer) {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                batchToFlush = new ArrayList<>(buffer);
                buffer.clear();
            }
        }

        if (batchToFlush != null) {
            asyncWrite(batchToFlush);
        }
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        if (flusher != null) {
            flusher.shutdown();
        }

        // Final flush before closing
        flush();

        if (executorService != null) {
            executorService.shutdown();
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                LOG.warn("Executor service did not shut down gracefully within 30 seconds.");
                executorService.shutdownNow();
            }
        }

        super.close();
        checkAsyncError(); // Check for any last-minute errors
        LOG.info("FeastSinkFunction for job '{}' closed.", jobName);
    }

    private synchronized void flush() {
        checkAsyncError();
        List<FeastRequest> batchToFlush;
        synchronized (buffer) {
            if (buffer.isEmpty()) {
                return;
            }
            batchToFlush = new ArrayList<>(buffer);
            buffer.clear();
        }
        asyncWrite(batchToFlush);
    }

    private void asyncWrite(List<FeastRequest> batch) {
        executorService.submit(() -> {
            for (FeastRequest request : batch) {
                writeWithRetry(request);
            }
        });
    }

    private void writeWithRetry(FeastRequest request) {
        int attempt = 0;
        boolean success = false;
        while (attempt < maxRetries && !success) {
            try {
                ApiResponse<Map<String, Object>> response = FeastApi.writeToOnlineStore(request);
                if (response != null && response.isSuccess()) {
                    LOG.debug("Successfully wrote to Feast for project: {}", request.getProject());
                    success = true;
                } else {
                    LOG.warn("Attempt {}/{} failed to write to Feast. Response: {}",
                            attempt + 1, maxRetries, response != null ? response.toString() : "null");
                }
            } catch (Exception e) {
                LOG.warn("Attempt {}/{} failed with exception while writing to Feast.",
                        attempt + 1, maxRetries, e);
            }

            if (!success) {
                attempt++;
                if (attempt < maxRetries) {
                    try {
                        // Exponential backoff
                        Thread.sleep(1000 * (long) Math.pow(2, attempt - 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break; // Exit retry loop if interrupted
                    }
                }
            }
        }
        if (!success) {
            LOG.error("Failed to write to Feast after {} retries for project: {}. Data might be lost.",
                    maxRetries, request.getProject());
            // Optional: Add logic here to send the failed 'request' to a dead-letter queue.
        }
    }

    private void checkAsyncError() {
        if (asyncError != null) {
            throw new RuntimeException("Asynchronous error in FeastSinkFunction", asyncError);
        }
    }
}