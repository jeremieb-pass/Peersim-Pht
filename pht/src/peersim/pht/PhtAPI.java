package peersim.pht;

public interface PhtAPI {
    void insertion (String key, Object data);
    void suppression (String key);
    void query (String key);
    void rangeQuery (String keyMin, String keyMax);
}
