package peersim.pht.messages;

import peersim.core.Node;


/**
 * PhtMessage is the central message class for all communications at the
 * Pht level.
 * It contains constant values for all different type of messages (15 related
 * to requests, and 15 more for the corresponding acquittal).
 *
 * There is no big enumeration, because we some time need to do some
 * comparisons.
 *
 * A PhtMessage has:
 *   1/ a type, split / suppression / lookup / merge / etc. request
 *   2/ a source node (on the peersim level), this is needed for direct
 *      communications between nodes to avoid unnecessary dht-lookup operations.
 *   3/ a source node's (PhtNode) label: this will be used for acquittals
 *   4/ an id, each request (and hence each message) must have a unique one for
 *      identification
 *   5/ more space if needed, like another class in the peersim.pht.messages
 *      package or a boolean, or a data.
 */
public class PhtMessage {
    // For the first Pht node on the network
    public static final int INIT = -1;


    /* Indexes */

    public static final int ACK         = 100;

    /* Requests */

    public static final int SPLIT               = 1;
    public static final int SPLIT_DATA          = 2;
    public static final int SPLIT_LEAVES        = 3;
    public static final int UPDATE_PREV_LEAF    = 4;
    public static final int UPDATE_NEXT_LEAF    = 5;
    public static final int MERGE               = 6;
    public static final int NO_MERGE            = 7;
    public static final int MERGE_LEAVES        = 8;
    public static final int MERGE_DATA          = 9;
    public static final int MERGE_DONE          = 10;
    public static final int INSERTION           = 11;
    public static final int SUPRESSION          = 12;
    public static final int UPDATE_NBKEYS_MINUS = 13;
    public static final int UPDATE_NBKEYS       = 14;
    public static final int UPDATE_NBKEYS_PLUS  = 15;
    public static final int LIN_LOOKUP          = 16;
    public static final int BIN_LOOKUP          = 17;
    public static final int SEQ_QUERY           = 18;
    public static final int PAR_QUERY           = 19;

    /* ACK for requests */

    public static final int ACK_SPLIT               = SPLIT               + ACK;
    public static final int ACK_SPLIT_DATA          = SPLIT_DATA          + ACK;
    public static final int ACK_SPLIT_LEAVES        = SPLIT_LEAVES        + ACK;
    public static final int ACK_UPDATE_LEAVES       = UPDATE_PREV_LEAF    + ACK;
    public static final int ACK_UPDATE_NEXT_LEAF    = UPDATE_NEXT_LEAF    + ACK;
    public static final int ACK_MERGE               = MERGE               + ACK;
    public static final int ACK_MERGE_LEAVES        = MERGE_LEAVES        + ACK;
    public static final int ACK_MERGE_DATA          = MERGE_DATA          + ACK;
    public static final int ACK_MERGE_DONE          = MERGE_DONE          + ACK;
    public static final int ACK_INSERTION           = INSERTION           + ACK;
    public static final int ACK_SUPRESSION          = SUPRESSION          + ACK;
    public static final int ACK_UPDATE_NBKEYS_MINUS = UPDATE_NBKEYS_MINUS + ACK;
    public static final int ACK_UPDATE_NBKEYS_PLUS  = UPDATE_NBKEYS_PLUS  + ACK;
    public static final int ACK_LIN_LOOKUP          = LIN_LOOKUP          + ACK;
    public static final int ACK_BIN_LOOKUP          = BIN_LOOKUP          + ACK;
    public static final int ACK_SEQ_QUERY           = SEQ_QUERY           + ACK;
    public static final int ACK_PAR_QUERY           = PAR_QUERY           + ACK;
    public static final int ACK_PAR_QUERY_CLIENT    = ACK_PAR_QUERY       + 1;
    public static final int ACK_PAR_QUERY_CLIENT_F  = ACK_PAR_QUERY_CLIENT+ 1;


    private int type;
    private Node initiator;
    private String initiatorLabel;
    private Object more;
    private final long id;

    public PhtMessage(Node initiator) {
        this.type           = PhtMessage.INIT;
        this.initiator      = initiator;
        this.initiatorLabel = null;
        this.id             = (long) 0;
    }

    public PhtMessage(int type, Node initiator, String initiatorLabel, long id, Object more) {
        this.type = type;
        this.initiator = initiator;
        this.initiatorLabel = initiatorLabel;
        this.id = id;
        this.more = more;
    }

   /* Getter */

    public Object getMore() {
        return more;
    }

    public long getId() {
        return id;
    }

    public String getInitiatorLabel() {
        return initiatorLabel;
    }

    public Node getInitiator() {
        return initiator;
    }

    public int getType() {
        return type;
    }

    /* Setter */

    public void setInitiator(Node initiator) {
        this.initiator = initiator;
    }

    public void setInitiatorLabel(String initiatorLabel) {
        this.initiatorLabel = initiatorLabel;
    }

    public void setMore(Object more) {
        this.more = more;
    }

    public void setType(int type) {
        this.type = type;
    }
}
