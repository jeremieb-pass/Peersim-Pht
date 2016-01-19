package peersim.pht.statistics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

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

    private static Comparator<NodeHolder> compUsage;
    private static Comparator<NodeHolder> compPhtNodes;
    private static Comparator<NodeHolder> compLeaves;

    private final ArrayList<NodeHolder> nodeStats;

    private NodeHolder root;

    private final int MIN  = 0;
    private final int AVG  = 1;
    private final int MAX  = 2;

    private final int MIN_LEAVES = 3;
    private final int AVG_LEAVES = 4;
    private final int MAX_LEAVES = 5;

    NodeStats() {
        this.nodeStats   = new ArrayList<>();

        compUsage = new Comparator<NodeHolder>() {
            /**
             * Compare the usage fields.
             * If they are equals (which is likely to happen), take in
             * consideration the ids which are unique.
             *
             * @param nodeHolder NodeHolder to compare with t1
             * @param t1 NodeHolder to compare with nodeHolder
             * @return -1, 0 or 1 if this is inferior, equal or superior to nst
             */
            @Override
            public int compare(NodeHolder nodeHolder, NodeHolder t1) {
                if (nodeHolder.usage < t1.usage) {
                    return -1;
                } else if (nodeHolder.usage > t1.usage) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };

        compPhtNodes = new Comparator<NodeHolder>() {
            /**
             * Compare the nbPhtNodes field.
             * If they are equal (which is likely to happen), sort by ids
             * which are unique.
             * @param nodeHolder NodeHolder to compare to t1
             * @param t1 NodeHolder to compare to nodeHoler
             * @return negative, nul or positive value if nodeHolder is inferior, equal,
             * inferior to t1
             */
            @Override
            public int compare(NodeHolder nodeHolder, NodeHolder t1) {
                if (nodeHolder.nbPhtNodes != t1.nbPhtNodes) {
                    return nodeHolder.nbPhtNodes - t1.nbPhtNodes;
                }
                return 0;
            }
        };

        compLeaves = new Comparator<NodeHolder>() {
            /**
             * Compare the nbLeaves field.
             * If they are equal (which is likely to happen), sort by ids
             * which are unique.
             * @param nodeHolder NodeHolder to compare to t1
             * @param t1 NodeHolder to compare to nodeHoler
             * @return negative, nul or positive value if nodeHolder is inferior, equal,
             * inferior to t1
             */
            @Override
            public int compare(NodeHolder nodeHolder, NodeHolder t1) {
                if (nodeHolder.nbLeaves != t1.nbLeaves) {
                    return nodeHolder.nbLeaves - t1.nbLeaves;
                }

                return 0;
            }
        };
    }

    /* ___________________________                 __________________________ */
    /* ___________________________ Node statistics __________________________ */

    /**
     * Add a PhtNodeStats into the TreeMap
     * @param id Node's id
     * @param usage PhtNode's usage value
     * @param usageDest PhtNode's usageDest value
     * @param nbPhtNodes number of PhtNodes
     * @param nbLeaves number of leaves
     */
    public void addN (long id, long usage, long usageDest,
                      int nbPhtNodes, int nbLeaves, boolean root) {
        NodeHolder nh;

        nh = new NodeHolder(id, usage, usageDest, nbPhtNodes, nbLeaves);
        this.nodeStats.add(nh);

        if (root) {
            this.root = nh;
        }
    }

    /**
     * Most used PhtNodes.
     * @param nb maximum number of Node
     * @return List of the nb most used Nodes
     */
    private List<NodeHolder> mostUsedNodes(int nb) {
        List<NodeHolder> munst = new LinkedList<>();

        this.nodeStats.sort(compUsage);

        for (int i = 0; (i < nb) && (i < this.nodeStats.size()); i++) {
            NodeHolder nh;

            nh = this.nodeStats.get( this.nodeStats.size()-i-1 );
            if (nh != null) {
                munst.add(nh);
            }
        }

        return munst;
    }

    /**
     * Min, average and maximum number of PhtNodes.
     * Min, average and maximum number of leaves.
     * @return Min, avg, and max for PhtNodes and leaves.
     */
    private float[] phtNodes () {
        int SIZE = 6;
        float [] pna = new float[SIZE];

        if (this.nodeStats.size() == 0) {
            for (int i = 0; i < SIZE; i++) {
                pna[i] = 0;
            }
            return pna;
        }

        /* All PhtNodes */
        this.nodeStats.sort(compPhtNodes);

        pna[MIN] = this.nodeStats.get(0).nbPhtNodes;
        pna[MAX] = this.nodeStats.get( this.nodeStats.size()-1 ).nbPhtNodes;

        pna[AVG] = 0;
        for (NodeHolder nh: this.nodeStats) {
            pna[AVG] += nh.nbPhtNodes;
        }
        pna[AVG] = pna[AVG] / (float)this.nodeStats.size();

        /* Leaves only */
        int minLeaves = 0;

        this.nodeStats.sort( compLeaves );

        pna[MAX_LEAVES] = this.nodeStats.get( this.nodeStats.size()-1 ).nbLeaves;

        pna[AVG_LEAVES] = 0;
        for (NodeHolder nh: this.nodeStats) {
            if (nh.nbLeaves == 0) {
                minLeaves++;
            } else {
                pna[AVG_LEAVES] += nh.nbLeaves;
            }
        }

        int minIdx = minLeaves < this.nodeStats.size() ? minLeaves : 0;
        pna[MIN_LEAVES] = this.nodeStats.get(minIdx).nbLeaves;
        pna[AVG_LEAVES] = pna[AVG_LEAVES] / (float)(this.nodeStats.size() - minIdx + 1);

        return pna;
    }

    public void printAll (int mu) {

        // Count
        System.out.printf("Number of Nodes: %d\n\n%d most used Nodes\n",
                this.nodeStats.size(), mu);
        for (NodeHolder nt: mostUsedNodes(mu)) {
            System.out.printf("\t[%d] used %d times (%d times as destination)\n",
                    nt.id,
                    nt.usage,
                    nt.usageDest);
        }

        // PhtNodes on the Node
        float[] pna = phtNodes();
        System.out.printf("\nMin, avg, max number of PhtNodes per Node: %.3f <> %.3f <> %.3f\n",
                pna[MIN], pna[AVG], pna[MAX]);
        System.out.printf("Min, avg, max number of leaves per Node: %.3f <> %.3f <> %.3f\n",
                pna[MIN_LEAVES], pna[AVG_LEAVES], pna[MAX_LEAVES]);

        // Node with the Pht's root
        if (this.root != null) {
            System.out.printf("\nNode with the Pht's root:\n");
            System.out.printf("\t[%d] used %d times (%d times as destination) <> %d PhtNodes, %d leaves\n",
                    this.root.getId(),
                    this.root.getUsage(),
                    this.root.getUsageDest(),
                    this.root.getNbPhtNodes(),
                    this.root.getNbLeaves());
        }
    }

    /* _____________________________             ____________________________ */
    /* _____________________________ Inner class ____________________________ */

    /**
     * Data holder class for a {@link peersim.core.Node}
     */
    class NodeHolder {
        private final long id;
        private final long usage;
        private final long usageDest;
        private final int nbPhtNodes;
        private final int nbLeaves;

        public NodeHolder(long id, long usage, long usageDest,
                          int nbPhtNodes, int nbLeaves) {
            this.id         = id;
            this.usage      = usage;
            this.usageDest  = usageDest;
            this.nbPhtNodes = nbPhtNodes;
            this.nbLeaves   = nbLeaves;
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

        public int getNbPhtNodes() {
            return nbPhtNodes;
        }

        public int getNbLeaves() {
            return nbLeaves;
        }
    }
}
