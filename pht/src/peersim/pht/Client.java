package peersim.pht;


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
    // PhtProtocol must be able to respond to a client
    void responseOk(long requestId, int ok);
    void responseValue(long requestId, String key, Object data);
    void responseList(long requestId, List<PhtData> resp, boolean end);

    // PhtProtocol must be able to inform a client that a certain operation
    // has ended
    void splitOk();
    void mergeOk();
    void initOk();
}
