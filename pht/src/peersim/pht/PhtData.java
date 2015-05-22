package peersim.pht;

/**
 * <p>Basic PhtData contains a String and an Object.</p>
 */
public class PhtData {
    private final String key;
    private final Object data;

    public PhtData(String key, Object data) {
        this.key = key;
        this.data = data;
    }

    public Object getData() {
        return data;
    }

    public String getKey() {
        return key;
    }
}
