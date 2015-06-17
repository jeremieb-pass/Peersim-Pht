package peersim.pht.statistics;

/**
 * <p>
 *     Needed information about a PhtNode.
 *     Since there can be multiple phases in the simulation (see
 *     {@link peersim.pht.statistics.PhaseStats}), it is not possible to
 *     keep reference.
 * </p>
 */
class PhtNodeInfo {
    private String label;
    private long usage;
    private long usageDest;
    private int nbKeys;

    public PhtNodeInfo(String label, long usage, long usageDest, int nbKeys) {
        this.label = label;
        this.usage = usage;
        this.usageDest = usageDest;
        this.nbKeys = nbKeys;
    }

    public String getLabel() {
        return label;
    }

    public long getUsage() {
        return usage;
    }

    public long getUsageDest() {
        return usageDest;
    }

    public int getNbKeys() {
        return nbKeys;
    }
}
