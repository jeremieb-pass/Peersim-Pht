package peersim.pht.exceptions;

/**
 * Exception thrown when a PhtNode makes a unauthorized transition.
 */
public class WrongStateException extends PhtException {
    public WrongStateException(String message) {
        super(message);
    }
}
