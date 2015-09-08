package peersim.pht.dht;

import peersim.pht.messages.PhtMessage;

/**
 * <p>The only method needed for PHT from a DHT is dhtLoopkup.</p>
 * <p>In the implementation of PHT, the keys are Strings. It is the job of the
 * dhtLookup method to make the conversion between String and the type used by
 * the DHT if necessary.</p>
 */
public interface DhtInterface {
    void send(PhtMessage message, String dest);
}
