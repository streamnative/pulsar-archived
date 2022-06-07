package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

/**
 * Placement strategy which selects a broker based on which one has the least long term message rate.
 * See {@link org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate}
 */
@Slf4j
public class LeastLongTermMessageRateStrategyImpl extends AbstractBrokerSelectionStrategy {


    private static final String LOAD_BALANCER_NAME = "LeastLongTermMessageRateStrategy";
    private ArrayList<String> bestBrokers;

    public LeastLongTermMessageRateStrategyImpl() {
        bestBrokers = new ArrayList<>();
    }

    @Override
    public Optional<String> doSelect(List<String> brokers, BaseLoadManagerContext context) {
        double minScore = Double.POSITIVE_INFINITY;
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        for (String broker : brokers) {
            final double score = getScore(context, broker, context.brokerConfiguration());
            if (score == Double.POSITIVE_INFINITY) {
                final BrokerLoadData localData = context.brokerLoadDataStore().get(broker);
                log.warn(
                        "Broker {} is overloaded: CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                                + "BANDWIDTH OUT: {}%",
                        broker, localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                        localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                        localData.getBandwidthOut().percentUsage());

            }
            if (score < minScore) {
                // Clear best brokers since this score beats the other brokers.
                bestBrokers.clear();
                bestBrokers.add(broker);
                minScore = score;
            } else if (score == minScore) {
                // Add this broker to best brokers since it ties with the best score.
                bestBrokers.add(broker);
            }
        }
        if (bestBrokers.isEmpty()) {
            // All brokers are overloaded.
            // Assign randomly in this case.
            bestBrokers.addAll(brokers);
        }

        if (bestBrokers.isEmpty()) {
            // If still, it means there are no available brokers at this point
            return Optional.empty();
        }

        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }

    // Form a score for a broker using its preallocated bundle data and time average data.
    // This is done by summing all preallocated long-term message rates and adding them to the broker's overall
    // long-term message rate, which is itself the sum of the long-term message rate of every allocated bundle.
    // Any broker at (or above) the overload threshold will have a score of POSITIVE_INFINITY.
    private static double getScore(final BaseLoadManagerContext context,
                                   final String broker,
                                   final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        BrokerLoadData brokerLoadData = context.brokerLoadDataStore().get(broker);
        final double maxUsage = brokerLoadData == null ? 0.0f : brokerLoadData.getMaxResourceUsage();

        if (maxUsage > overloadThreshold) {
            log.warn("Broker {} is overloaded: max usage={}", broker, maxUsage);
            return Double.POSITIVE_INFINITY;
        }

        double totalMessageRate = 0;
        for (BundleData bundleData : context.preallocatedBundleData(broker).values()) {
            final TimeAverageMessageData longTermData = bundleData.getLongTermData();
            totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
        }

        // calculate estimated score
        final TimeAverageBrokerData timeAverageData = context.timeAverageBrokerLoadDataStore().get(broker);
        final double timeAverageLongTermMessageRate = timeAverageData == null ? 0.0
                : timeAverageData.getLongTermMsgRateIn() + timeAverageData.getLongTermMsgRateOut();
        final double totalMessageRateEstimate = totalMessageRate + timeAverageLongTermMessageRate;

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has long term message rate {}",
                    broker, totalMessageRateEstimate);
        }
        return totalMessageRateEstimate;
    }

    @Override
    public String name() {
        return LOAD_BALANCER_NAME;
    }
}
