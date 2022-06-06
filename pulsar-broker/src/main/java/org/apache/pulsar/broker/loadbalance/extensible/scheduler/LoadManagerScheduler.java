package org.apache.pulsar.broker.loadbalance.extensible.scheduler;

/**
 * The load manager scheduler.
 */
public interface LoadManagerScheduler {

    /**
     * Execute the schedule task.
     */
    void execute();
}
