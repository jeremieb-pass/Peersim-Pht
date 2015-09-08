package peersim.pht.state;

/**
 * <p>PhtNodeState handles the current state of a PhtNode a has methods to handle
 * events and change state. The primary goal of this class is to ensure that
 * operations are happening in the correct order:</p>
 *
 * <p>for example, if an ack for a splitData arrives while the node is in a stable
 * state, there is a problem.</p>
 *
 * <p>Since the state of the current PhtNode depends on the ACK of his sons for
 * merge and split operations, we added two fields lson and son to limit the
 * number of state in the automaton: a son may finish the split process while
 * the other has just return an ACK_SPLIT_LEAVES.</p>
 */
public class PhtNodeState {

    public static final int STABLE   = 0;
    public static final int UNSTABLE = 1;

    public final static int WAITING_SPLIT    = 10;
    public final static int ACK_SPLIT        = 11;
    public final static int ACK_SPLIT_LEAVES = 12;
    public final static int ACK_SPLIT_DATA   = 13;

    public final static int WAITING_MERGE    = 20;
    public final static int ACK_MERGE        = 21;
    public final static int NO_MERGE         = 22;
    public final static int ACK_MERGE_LEAVES = 23;
    public final static int MERGE_DONE       = 24;

    public final static int WAITING_THREADED_LEAVES = 30;
    public final static int WAITING_PREV_LEAF       = 31;
    public final static int WAITING_NEXT_LEAF       = 32;

    public final static int ACK_PREV_LEAF = 40;
    public final static int ACK_NEXT_LEAF = 41;

    private int state;
    private int lson;
    private int rson;

    private int prevLeaf;
    private int nextLeaf;

    public PhtNodeState() {
        this.state = STABLE;
        this.lson  = STABLE;
        this.rson  = STABLE;
    }


    public boolean isStable() {
        return this.state == STABLE;
    }

    public void unstable () {
        this.state = UNSTABLE;
        this.lson  = UNSTABLE;
        this.rson  = UNSTABLE;
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ SPLIT _______________________________ */

    public boolean startSplit() {
        if (this.state == STABLE) {
            this.state = WAITING_SPLIT;
            this.lson  = WAITING_SPLIT;
            this.rson  = WAITING_SPLIT;
        } else {
            return false;
        }

        return true;
    }

    /* ______________________________ ACK_SPLIT _____________________________ */

    public boolean ackSplitLson() {
        if (this.lson == WAITING_SPLIT) {
            this.lson = ACK_SPLIT;

            if (this.rson == ACK_SPLIT) {
                this.state = ACK_SPLIT;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackSplitRson() {
        if (this.rson == WAITING_SPLIT) {
            this.rson = ACK_SPLIT;

            if (this.lson == ACK_SPLIT) {
                this.state = ACK_SPLIT;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean twoAckSplit() {
        return this.state == ACK_SPLIT;
    }

    /* ______________________________ ACK_LEAVES ____________________________ */

    public boolean ackSplitLeavesLson() {
        if (this.lson == ACK_SPLIT) {
            this.lson = ACK_SPLIT_LEAVES;

            if (this.rson == ACK_SPLIT_LEAVES) {
                this.state = ACK_SPLIT_LEAVES;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackSplitLeavesRson() {
        if (this.rson == ACK_SPLIT) {
            this.rson = ACK_SPLIT_LEAVES;

            if (this.lson == ACK_SPLIT_LEAVES) {
                this.state = ACK_SPLIT_LEAVES;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean twoAckSplitLeaves() {
        return this.state == ACK_SPLIT_LEAVES;
    }

    /* _______________________________ ACK_DATA _____________________________ */

    public boolean ackSDataLson() {
        if (this.lson == ACK_SPLIT_LEAVES) {
            this.lson = ACK_SPLIT_DATA;

            if ( (this.rson == ACK_SPLIT_DATA)
                    && (this.state == ACK_SPLIT_LEAVES) ) {
                this.state = STABLE;
                this.lson  = STABLE;
                this.rson  = STABLE;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackSDataRson() {
        if (this.rson == ACK_SPLIT_LEAVES) {
            this.rson = ACK_SPLIT_DATA;

            if ( (this.lson == ACK_SPLIT_DATA)
            && (this.state == ACK_SPLIT_LEAVES) ) {
                    this.state = STABLE;
                    this.rson  = STABLE;
                    this.lson  = STABLE;
            }
        } else {
            return false;
        }

        return true;
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ MERGE _______________________________ */


    public boolean startMerge() {
        if (this.state == STABLE) {
            this.state = WAITING_MERGE;
            this.lson  = WAITING_MERGE;
            this.rson  = WAITING_MERGE;
        } else {
            return false;
        }

        return true;
    }

    /* ______________________________ ACK_MERGE _____________________________ */

    public boolean ackMergeLSon () {
        if (this.lson == WAITING_MERGE) {
            this.lson = ACK_MERGE;

            if (this.rson == ACK_MERGE) {
                this.state = ACK_MERGE;
            }

            return true;
        }

        return false;
    }

    public boolean ackMergeRSon () {
        if (this.rson == WAITING_MERGE) {
            this.rson = ACK_MERGE;

            if (this.lson == ACK_MERGE) {
                this.state = ACK_MERGE;
            }

            return true;
        }

        return false;
    }

    /* _______________________________ NO_MERGE _____________________________ */

    public boolean noMergeLson () {
        if (this.lson == WAITING_MERGE) {
            this.lson = NO_MERGE;
            return true;
        }

        return false;
    }

    public boolean noMergeRson () {
        if (this.rson == WAITING_MERGE) {
            this.rson = NO_MERGE;
            return true;
        }

        return false;
    }

    public boolean noMerge () {
        return ( ((this.lson == NO_MERGE) && (this.rson == ACK_MERGE))
                || ((this.rson == NO_MERGE) && (this.lson == ACK_MERGE)) );
    }

    /* ______________________________ ACK_LEAVES ____________________________ */

    public boolean ackMergeLeavesLson () {
        if (this.lson == ACK_MERGE) {
            this.lson = ACK_MERGE_LEAVES;

            if (this.rson == ACK_MERGE_LEAVES) {
                this.state = ACK_MERGE_LEAVES;
            }

            return true;
        }

        return false;
    }

    public boolean ackMergeLeavesRson () {
        if (this.rson == ACK_MERGE) {
            this.rson = ACK_MERGE_LEAVES;

            if (this.lson == ACK_MERGE_LEAVES) {
                this.state = ACK_MERGE_LEAVES;
            }

            return true;
        }

        return false;
    }

    public boolean ackMergeLeaves () {
        return this.state == ACK_MERGE_LEAVES;
    }

    /* ______________________________ MERGE_DONE ____________________________ */

    public boolean mergeDoneLSon() {
        if (this.lson == ACK_MERGE_LEAVES) {
            this.lson = MERGE_DONE;

            if (this.rson == MERGE_DONE) {
                this.state = MERGE_DONE;
            }

            return true;
        }

        return false;
    }

    public boolean mergeDoneRSon() {
        if (this.rson == ACK_MERGE_LEAVES) {
            this.rson = MERGE_DONE;

            if (this.lson == MERGE_DONE) {
                this.state = MERGE_DONE;
            }

            return true;
        }

        return false;
    }

    public boolean mergeDone () {
        return this.state == MERGE_DONE;
    }


    public boolean ackMerge () {
        return this.state == ACK_MERGE;
    }

    public void stopMerge() {
        this.state = STABLE;
        this.lson  = STABLE;
        this.rson  = STABLE;
    }

    /* ___________________________                 __________________________ */
    /* ___________________________ THREADED LEAVES __________________________ */

    public void startThreaded () {
        this.prevLeaf = WAITING_PREV_LEAF;
        this.nextLeaf = WAITING_NEXT_LEAF;
    }


    public boolean ackPrevLeaf() {
        if (this.prevLeaf == WAITING_PREV_LEAF) {
            this.prevLeaf = ACK_PREV_LEAF;

            if (this.nextLeaf == ACK_NEXT_LEAF) {
                reset();
            }

            return true;
        }

        return false;
    }

    public boolean ackNextLeaf() {
        if (this.nextLeaf == WAITING_NEXT_LEAF) {
            this.nextLeaf = ACK_NEXT_LEAF;

            if (this.prevLeaf == ACK_PREV_LEAF) {
                reset();
            }

            return true;
        }

        return false;
    }

    public void reset () {
        this.state    = STABLE;
        this.lson     = STABLE;
        this.rson     = STABLE;
        this.prevLeaf = STABLE;
        this.nextLeaf = STABLE;
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ UTILS _______________________________ */

    public int getState () {
        return this.state;
    }

    @Override
    public String toString () {
        StringBuffer sb = new StringBuffer();

        sb.append("state: ");

        // State
        if (this.state == STABLE) {
            sb.append("STABLE");
        } else if (this.state == WAITING_SPLIT) {
            sb.append("WAITING_SPLIT");
        } else if (this.state == ACK_SPLIT) {
            sb.append("ACK_SPLIT");
        } else if (this.state == ACK_SPLIT_LEAVES) {
            sb.append("ACK_SPLIT_LEAVES");
        } else if (this.state == ACK_SPLIT_DATA) {
            sb.append("ACK_SPLIT_DATA");
        } else if (this.state == WAITING_MERGE) {
            sb.append("WAITING_MERGE");
        } else if (this.state == ACK_MERGE) {
            sb.append("ACK_MERGE");
        } else if (this.state == ACK_MERGE_LEAVES) {
            sb.append("ACK_MERGE_LEAVES");
        } else if (this.state == NO_MERGE) {
            sb.append("NO_MERGE");
        } else if (this.state == MERGE_DONE) {
            sb.append ("MERGE_DONE");
        } else if (this.state == WAITING_THREADED_LEAVES) {
            sb.append("WAITING_THREADED_LEAVES");
        } else if (this.state == ACK_PREV_LEAF) {
            sb.append("ACK_PREV_LEAF");
        } else if (this.state == ACK_NEXT_LEAF) {
            sb.append("ACK_NEXT_LEAF");
        }

        sb.append(" :: lson: ");

        // Lson
        if (this.lson == STABLE) {
            sb.append("STABLE");
        } else if (this.lson == WAITING_SPLIT) {
            sb.append("WAITING_SPLIT");
        } else if (this.lson == ACK_SPLIT) {
            sb.append("ACK_SPLIT");
        } else if (this.lson == ACK_SPLIT_LEAVES) {
            sb.append("ACK_SPLIT_LEAVES");
        } else if (this.lson == ACK_SPLIT_DATA) {
            sb.append("ACK_SPLIT_DATA");
        } else if (this.lson == WAITING_MERGE) {
            sb.append("WAITING_MERGE");
        } else if (this.lson == ACK_MERGE) {
            sb.append("ACK_MERGE");
        } else if (this.lson == ACK_MERGE_LEAVES) {
            sb.append("ACK_MERGE_LEAVES");
        } else if (this.lson == NO_MERGE) {
            sb.append("NO_MERGE");
        } else if (this.lson == MERGE_DONE) {
            sb.append ("MERGE_DONE");
        } else if (this.lson == WAITING_THREADED_LEAVES) {
            sb.append("WAITING_THREADED_LEAVES");
        } else if (this.lson == UNSTABLE) {
            sb.append("UNSTABLE");
        }

        sb.append(" :: rson: ");

        // Rson
        if (this.rson == STABLE) {
            sb.append("STABLE");
        } else if (this.rson == WAITING_SPLIT) {
            sb.append("WAITING_SPLIT");
        } else if (this.rson == ACK_SPLIT) {
            sb.append("ACK_SPLIT");
        } else if (this.rson == ACK_SPLIT_LEAVES) {
            sb.append("ACK_SPLIT_LEAVES");
        } else if (this.rson == ACK_SPLIT_DATA) {
            sb.append("ACK_SPLIT_DATA");
        } else if (this.rson == WAITING_MERGE) {
            sb.append("WAITING_MERGE");
        } else if (this.rson == ACK_MERGE) {
            sb.append("ACK_MERGE");
        } else if (this.rson == ACK_MERGE_LEAVES) {
            sb.append("ACK_MERGE_LEAVES");
        } else if (this.rson == NO_MERGE) {
            sb.append("NO_MERGE");
        } else if (this.rson == MERGE_DONE) {
            sb.append ("MERGE_DONE");
        } else if (this.rson == WAITING_THREADED_LEAVES) {
            sb.append("WAITING_THREADED_LEAVES");
        } else if (this.rson == UNSTABLE) {
            sb.append("UNSTABLE");
        }

        sb.append(" :: threaded prev: ");
        if (this.prevLeaf == STABLE) {
            sb.append("STABLE");
        } else if (this.prevLeaf == WAITING_PREV_LEAF) {
            sb.append("WAITING_PREV_LEAF");
        } else if (this.prevLeaf == ACK_PREV_LEAF) {
            sb.append("ACK_PREV_LEAF");
        }

        sb.append(" :: threaded next: ");
        if (this.nextLeaf == STABLE) {
            sb.append("STABLE");
        } else if (this.nextLeaf == WAITING_NEXT_LEAF) {
            sb.append("WAITING_NEXT_LEAF");
        } else if (this.nextLeaf == ACK_NEXT_LEAF) {
            sb.append("ACK_NEXT_LEAF");
        }

        return sb.toString();
    }
}
