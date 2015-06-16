package peersim.pht.statistics;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

/**
 * <p>
 *     Statistics on {@link peersim.core.Node} elements.
 * </p>
 * <ul>
 *     <li>Number of {@link peersim.core.Node} and their usage.</li>
 *     <li>Minimum, average and maximum number of {@link peersim.pht.PhtNode} per
 *     {@link peersim.core.Node}.</li>
 *     <li>Minimum, average and maximum number of {@link peersim.pht.PhtNode}
 *     leaves per {@link peersim.core.Node}.</li>
 *     <li>{@link peersim.core.Node} who has the {@link peersim.pht.PhtNode} root
 *     node.</li>
 * </ul>
 */
class NodeStats {
    private TreeSet<NodeHolder> nStats;

    protected NodeStats () {
        this.nStats = new TreeSet<NodeHolder>();
    }

    /* ___________________________                 __________________________ */
    /* ___________________________ Node statistics __________________________ */

    /**
     * Add a NodeStats into the TreeMap
     * @param nh Node information to add
     */
    public void addN (NodeHolder nh) {
        this.nStats.add(nh);
    }

    /**
     * Add a PhtNodeStats into the TreeMap
     * @param id Node's id
     * @param usage PhtNode's usage value
     * @param usageDest PhtNode's usageDest value
     */
    public void addN (long id, long usage, long usageDest) {
        this.nStats.add(new NodeHolder(id, usage, usageDest));
    }

    /**
     * Most used PhtNodes.
     * @param nb maximum number of Node
     * @return List of the nb most used Nodes
     */
    private List<NodeHolder> mostUsedNodes(int nb) {
        List<NodeHolder> munst = new LinkedList<NodeHolder>();

        for (int i = 0; i < nb; i++) {
            NodeHolder n = this.nStats.pollLast();

            if (n != null) {
                if (! munst.add(n)) {
                    break;
                }
            } else {
                break;
            }
        }

        return munst;
    }

    public void printAll (int mu) {
        System.out.printf("Number of Nodes: %d\n%d most used Nodes\n",
                this.nStats.size(), mu);
        for (NodeHolder nt: mostUsedNodes(mu)) {
            System.out.printf("\t[%d] used %d times (%d times as destination)\n",
                    nt.getId(),
                    nt.getUsage(),
                    nt.getUsageDest());
        }
    }

    /**
     * Data holder class for a {@link peersim.core.Node}
     */
    class NodeHolder implements Comparable<NodeHolder> {
        private long id;
        private long usage;
        private long usageDest;

        public NodeHolder(long id, long usage, long usageDest) {
            this.id = id;
            this.usage = usage;
            this.usageDest = usageDest;
        }

        /**
         * Compare the usage fields.
         * If there are equals (which is likely to happen), take in consideration the ids which are unique.
         *
         * @param nh NodeHolder to compare to
         * @return -1, 0 or 1 if this is inferior, equal or superior to nst
         */
        @Override
        public int compareTo(NodeHolder nh) {
            if (this.usage < nh.usage) {
                return -1;
            } else if (this.usage > nh.usage) {
                return 1;
            } else {

                if (this.id < nh.id) {
                    return -1;
                } else if (this.id > nh.id) {
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
}
