package org.apache.pulsar.broker.loadbalance.extensible.channel;

/**
 * The bundle actions.
 */
public enum BundleAction {

    /**
     * Own the bundle ownership
     *    The owner broker is selected by the local load manager.
     */
    Own,

    /**
     * Transfer the bundle ownership to the destination broker.
     *  The source broker internally disables the bundle ownership.
     *  The destination broker owns the bundle.
     */
    Transfer,

    /**
     * Return deferred client connections with the destination broker URL
     *   Close the connections if already being served
     */
    Return,

    /**
     * Split the target(parent) bundle into child bundles.
     */
    Split,

    /**
     * Create the child bundle entries in the channel, initially assigned to the local broker.
     */
    Create,

    /**
     * Discard the bundle entry in the channel(tombstone operation)
     */
    Discard,

    /**
     * Unload the bundle ownership from the owner broker
     *   Disable the bundle ownership
     *   Close client connections under the bundle
     *   Run the Discard action
     */
    Unload
}
