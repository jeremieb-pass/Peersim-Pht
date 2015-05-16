package peersim.pht.messages;

import peersim.core.Node;

/**
 * A PMLookup is a class for a lookup message inside a PhtMessage.
 * It contains additional information:
 *   1/ a key
 *   2/ an operation id (see below)
 *   3/ space for other information if needed
 *
 * The operation field will be used when an operation starts with a lookup
 * like insertion (find the node who will receive the data), suppression,
 * range query (find the starting node). This field will tell after an
 * ACK_[BL]IN_LOOKUP to continue to a INSERTION / SEQ_QUERY / PAR_QUERY
 * operation.
 */
public class PMLookup {
    private Object less;
    private Node dest;
    private String destLabel;
    private final String key;
    private final int operation;

    public PMLookup(String key, int operation, Node dest, String destLabel) {
        this.key       = key;
        this.operation = operation;
        this.dest      = dest;
        this.destLabel = destLabel;
    }

    public PMLookup(String key, int operation, Node dest, String destLabel, Object less) {
        this.key       = key;
        this.operation = operation;
        this.dest      = dest;
        this.destLabel = destLabel;
        this.less      = less;
    }


   /* Getter */

    public String getKey() {
        return key;
    }

    public int getOperation() {
        return operation;
    }

    public Object getLess() {
        return less;
    }

    public Node getDest() {
        return dest;
    }

    public String getDestLabel() {
        return destLabel;
    }

   /* Setter */

    public void setLess(Object less) {
        this.less = less;
    }

    public void setDest(Node dest) {
        this.dest = dest;
    }

    public void setDestLabel(String destLabel) {
        this.destLabel = destLabel;
    }
}
