package peersim.pht.statistics;

/**
 * Data holder class for a {@link peersim.core.Node}
 */
public class NodeStats implements Comparable<NodeStats> {
    private long id;
    private long usage;
    private long usageDest;

    public NodeStats(long id, long usage, long usageDest) {
        this.id = id;
        this.usage = usage;
        this.usageDest = usageDest;
    }

    /**
     * Compare the usage fields.
     * If there are equals (which is likely to happen), take in consideration the ids which are unique.
     *
     * @param nst NodeStats to compare to
     * @return -1, 0 or 1 if this is inferior, equal or superior to nst
     */
    @Override
    public int compareTo(NodeStats nst) {
        if (this.usage < nst.usage) {
            return -1;
        } else if (this.usage > nst.usage) {
            return 1;
        } else {

            if (this.id < nst.id) {
                return -1;
            } else if (this.id > nst.id) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    public long getId() {
        return id;
    }

    public long getUsage() {
        return usage;
    }

    public long getUsageDest() {
        return usageDest;
    }
}
