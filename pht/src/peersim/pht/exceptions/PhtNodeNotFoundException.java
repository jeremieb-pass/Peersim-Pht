package peersim.pht.exceptions;

/**
 * When a physical machine which was supposed to have a certain PhtNode.
 */
public class PhtNodeNotFoundException extends PhtException {
    public PhtNodeNotFoundException(String message) {
        super(message);
    }
}
