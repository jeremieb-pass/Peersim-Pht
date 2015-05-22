package peersim.pht;


import peersim.pht.PhtData;

import java.util.List;

/**
 * <p>
 * The purpose of this interface is to give the possibility to receive a
 * response from a previous request (insertion, suppression, exact lookup,
 * range query).
 * </p>
 *
 * <p>
 * PhtProtocol will register a client who made a request: when the node
 * receives the response it will forward it to the client.
 * </p>
 *
 * <p>
 * This interface is the external interface of the whole Pht: make requests
 * and get responses.
 * </p>
 */

public interface Client {

    void responseOk(long requestId, int ok);
    void responseValue(long requestId, String key, Object data);
    void responseList(long requestId, List<PhtData> resp);

    void lock();
    void release();

    void stop();
}
