package peersim.pht.state;

public enum PNState {
    STABLE,

    WAITING_SPLIT, ACK_SPLIT, ACK_SPLIT_LEAVES, ACK_SPLIT_DATA,
    WAITING_MERGE
}
