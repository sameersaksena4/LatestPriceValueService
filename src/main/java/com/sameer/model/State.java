package com.sameer.model;

/**
 * Defines the lifecycle states of a batch
 */
public enum State {

    /**
     * Batch is active, accepting records
     */
    STARTED,
    /**
     * Batch is finished, records are committed
     */
    COMPLETED,
    /**
     * Batch was cancelled, records are discarded
     */
    CANCELLED

}


