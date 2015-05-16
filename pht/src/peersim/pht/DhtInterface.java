package peersim.pht;

import peersim.core.Node;
import peersim.pht.messages.PhtMessage;

/**
 * The only method needed for PHT from a DHT is dhtLoopkup.
 * In the implementation of PHT, the keys are Strings. It is the job of the
 * dhtLookup method to make the conversion between String and the type used by
 * the DHT if necessary.
 */
public interface DhtInterface {
    void send(PhtMessage message, String dest);
    Object getNodeId();
    Node getNode();
}
