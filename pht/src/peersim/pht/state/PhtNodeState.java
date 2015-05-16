package peersim.pht.state;

/**
 * PhtNodeState handles the current state of a PhtNode a has methods to handle
 * events and change state. The primary goal of this class is to ensure that
 * operations are happening in the correct order:
 *   for example, if an ack for a splitData arrives while the node is in a stable
 *   state, there is a problem.
 *
 * Since the state of the current PhtNode depends on the ACK of his sons for
 * merge and split operations, we added two fields lson and son to limit the
 * number of state in the automaton: a son may finish the split process while
 * the other has just return an ACK_SPLIT_LEAVES.
 *
 */
public class PhtNodeState {
    private PNState state;
    private PNState lson;
    private PNState rson;

    public PhtNodeState() {
        this.state = PNState.STABLE;
        this.lson  = PNState.STABLE;
        this.rson  = PNState.STABLE;
    }


    public boolean isStable() {
        return this.state == PNState.STABLE;
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ SPLIT _______________________________ */

    public boolean startSplit() {
        if (this.state == PNState.STABLE) {
            this.state = PNState.WAITING_SPLIT;
            this.lson  = PNState.WAITING_SPLIT;
            this.rson  = PNState.WAITING_SPLIT;
        } else {
            return false;
        }

        return true;
    }

    /* ______________________________ ACK_SPLIT _____________________________ */

    public boolean ackSplitLson() {
        if (this.lson == PNState.WAITING_SPLIT) {
            this.lson = PNState.ACK_SPLIT;

            if (this.rson == PNState.ACK_SPLIT) {
                this.state = PNState.ACK_SPLIT;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackSplitRson() {
        if (this.rson == PNState.WAITING_SPLIT) {
            this.rson = PNState.ACK_SPLIT;

            if (this.lson == PNState.ACK_SPLIT) {
                this.state = PNState.ACK_SPLIT;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean twoAckSplit() {
        return this.state == PNState.ACK_SPLIT;
    }

    /* ______________________________ ACK_LEAVES ____________________________ */

    public boolean ackSplitLeavesLson() {
        if (this.lson == PNState.ACK_SPLIT) {
            this.lson = PNState.ACK_SPLIT_LEAVES;

            if (this.rson == PNState.ACK_SPLIT_LEAVES) {
                this.state = PNState.ACK_SPLIT_LEAVES;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackSplitLeavesRson() {
        if (this.rson == PNState.ACK_SPLIT) {
            this.rson = PNState.ACK_SPLIT_LEAVES;

            if (this.lson == PNState.ACK_SPLIT_LEAVES) {
                this.state = PNState.ACK_SPLIT_LEAVES;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean twoAckSplitLeaves() {
        return this.state == PNState.ACK_SPLIT_LEAVES;
    }

    /* _______________________________ ACK_DATA _____________________________ */

    public boolean ackSDataLson() {
        if (this.lson == PNState.ACK_SPLIT_LEAVES) {
            this.lson = PNState.ACK_SPLIT_DATA;

            if ( (this.rson == PNState.ACK_SPLIT_DATA)
                    && (this.state == PNState.ACK_SPLIT_LEAVES) ) {
                this.state = PNState.STABLE;
                this.lson  = PNState.STABLE;
                this.rson  = PNState.STABLE;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackSDataRson() {
        if (this.rson == PNState.ACK_SPLIT_LEAVES) {
            this.rson = PNState.ACK_SPLIT_DATA;

            if ( (this.lson == PNState.ACK_SPLIT_DATA)
            && (this.state == PNState.ACK_SPLIT_LEAVES) ) {
                    this.state = PNState.STABLE;
                    this.rson  = PNState.STABLE;
                    this.lson  = PNState.STABLE;
            }
        } else {
            return false;
        }

        return true;
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ MERGE _______________________________ */


    public boolean startMerge() {
        if (this.state == PNState.STABLE) {
            this.state = PNState.WAITING_MERGE;
            this.lson  = PNState.WAITING_MERGE;
            this.rson  = PNState.WAITING_MERGE;
        } else {
            return false;
        }

        return true;
    }

    /* ______________________________ ACK_MERGE _____________________________ */

/*    public boolean ackMergeLson() {
        if (this.lson == PNState.WAITING_MERGE) {
            this.lson = PNState.ACK_MERGE;

            if ( (this.rson == PNState.ACK_MERGE) ||
                    (this.rson == PNState.ACK_MERGE_LEAVES) ||
                    (this.rson == PNState.ACK_MERGE_DATA) ) {
                this.state = PNState.ACK_MERGE;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackMergeRson() {
        if (this.rson == PNState.WAITING_MERGE) {
            this.rson = PNState.ACK_MERGE;

            if ( (this.lson == PNState.ACK_MERGE) ||
                    (this.lson == PNState.ACK_MERGE_LEAVES) ||
                    (this.lson == PNState.ACK_MERGE_DATA) ) {
                this.state = PNState.ACK_MERGE;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean twoAckMerge() {
        return this.state == PNState.ACK_MERGE;
    }*/

    /* ______________________________ ACK_LEAVES ____________________________ */

/*    public boolean ackMergeLeavesLson() {
        if (this.lson == PNState.ACK_MERGE) {
            this.lson = PNState.ACK_MERGE_LEAVES;

            if ( (this.rson == PNState.ACK_MERGE_LEAVES) ||
                    (this.rson == PNState.ACK_MERGE_DATA) ) {
                this.state = PNState.ACK_MERGE_LEAVES;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackMergeLeavesRson() {
        if (this.rson == PNState.ACK_MERGE) {
            this.rson = PNState.ACK_MERGE_LEAVES;

            if ( (this.lson == PNState.ACK_MERGE_LEAVES) ||
                    (this.lson == PNState.ACK_MERGE_DATA) ) {
                this.state = PNState.ACK_MERGE_LEAVES;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean twoAckMergeLeaves() {
        return this.state == PNState.ACK_MERGE_LEAVES;
    }*/

    /* _______________________________ ACK_DATA _____________________________ */

/*    public boolean ackMDataLson() {
        if (this.lson == PNState.ACK_MERGE_LEAVES) {
            this.lson = PNState.ACK_MERGE_DATA;

            if (this.rson == PNState.ACK_MERGE_DATA) {
                this.state = PNState.STABLE;
                this.lson  = PNState.STABLE;
                this.rson  = PNState.STABLE;
            }
        } else {
            return false;
        }

        return true;
    }

    public boolean ackMDataRson() {
        if (this.rson == PNState.ACK_MERGE_LEAVES) {
            this.rson = PNState.ACK_MERGE_DATA;

            if (this.lson == PNState.ACK_MERGE_DATA) {
                this.state = PNState.STABLE;
                this.lson  = PNState.STABLE;
                this.rson  = PNState.STABLE;
            }
        } else {
            return false;
        }

        return true;
    }*/


    public boolean mergeDoneLSon() {
        if (this.lson == PNState.WAITING_MERGE) {
            this.lson = PNState.STABLE;

            if (this.rson == PNState.STABLE) {
                this.state = PNState.STABLE;
            }

            return true;
        }

        return false;
    }

    public boolean mergeDoneRSon() {
        if (this.rson == PNState.WAITING_MERGE) {
            this.rson = PNState.STABLE;

            if (this.lson == PNState.STABLE) {
                this.state = PNState.STABLE;
            }

            return true;
        }

        return false;
    }

    public void noMerge() {
        this.state = PNState.STABLE;
        this.lson  = PNState.STABLE;
        this.rson  = PNState.STABLE;
    }
}
