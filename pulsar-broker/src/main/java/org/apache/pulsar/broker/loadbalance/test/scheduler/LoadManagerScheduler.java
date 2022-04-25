package org.apache.pulsar.broker.loadbalance.test.scheduler;

/**
 * The load manager scheduler.
 */
public interface LoadManagerScheduler {

    /**
     * Execute the schedule task.
     */
    void execute();
}
