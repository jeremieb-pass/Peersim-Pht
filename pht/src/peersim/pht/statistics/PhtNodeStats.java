package peersim.pht.statistics;

import peersim.pht.PhtNode;
import peersim.pht.PhtProtocol;
import peersim.pht.PhtUtil;

import java.util.*;

/**
 * <p>
 *     Dedicated class for PhtNode statistics.
 * </p>
 */
public class PhtNodeStats {
    protected static PhtNodeStats pnStats;

    /*
     * Tree with all the PhtNodes in the network
     */
    private TreeSet<PhtNode> pn;

    /*
     * Trees with all the PhtNode leaves, the leaves with less than
     * PhtProtocol.B keys, with more than PhtProtocol.B keys
     *
     * SORTED BY NUMBER OF KEYS
     */
    private TreeSet<PhtNode> kpnLeavesBinf;
    private TreeSet<PhtNode> kpnLeavesBsup;

    // Number of keys
    private int nbKeysBinf;
    private int nbKeysBsup;

    // Min and max keys
    private PhtNode minKeysBinf;
    private PhtNode minKeysBsup;
    private PhtNode maxKeysBinf;
    private PhtNode maxKeysBsup;

    protected PhtNodeStats() {
    }

    public static synchronized PhtNodeStats getInstance() {
        if (pnStats == null) {
            pnStats    = new PhtNodeStats();
            pnStats.pn = new TreeSet<PhtNode>(new PhtNodeCompUsage());

            pnStats.kpnLeavesBinf = new TreeSet<PhtNode>(new PhtNodeCompKeys());
            pnStats.kpnLeavesBsup = new TreeSet<PhtNode>(new PhtNodeCompKeys());
        }

        return pnStats;
    }

    /**
     * Add a PhtNodeStats into the right TreeMap (leaf or not a leaf)
     * @param pn PhtNode information to add
     */
    public void addPN (PhtNode pn, boolean isLeaf) {
        if (isLeaf) {

            if (pn.getNbKeys() <= PhtProtocol.B) {
                this.kpnLeavesBinf.add(pn);
                this.nbKeysBinf += pn.getNbKeys();

                // Update min and max keys for the inferior to B tree
                if (minKeysBinf != null) {
                    if (pn.getNbKeys() < minKeysBinf.getNbKeys()) {
                        minKeysBinf = pn;
                    }
                } else {
                    minKeysBinf = pn;
                }

                if (maxKeysBinf != null) {
                    if (pn.getNbKeys() > maxKeysBinf.getNbKeys()) {
                        maxKeysBinf = pn;
                    }
                } else {
                    maxKeysBinf = pn;
                }
            } else {
                this.kpnLeavesBsup.add(pn);
                this.nbKeysBsup += pn.getNbKeys();

                // Update min and max keys for the superior to B tree
                if (minKeysBsup != null) {
                    if (pn.getNbKeys() < minKeysBsup.getNbKeys()) {
                        minKeysBsup = pn;
                    }
                } else {
                    minKeysBsup = pn;
                }

                if (maxKeysBsup != null) {
                    if (pn.getNbKeys() > maxKeysBsup.getNbKeys()) {
                        maxKeysBsup = pn;
                    }
                } else {
                    maxKeysBsup = pn;
                }
            }
        }
        this.pn.add(pn);
    }

    /**
     * Retrieve the nb upper nodes in the given TreeSet.
     * @param set Source list
     * @param nb maximum of PhtNode to return
     * @return list of nb (at most) PhtNode
     * keys in the TreeSet set.
     */
    private List<PhtNode> mostPN (TreeSet<PhtNode> set, int nb) {
        List<PhtNode> mukpn = new LinkedList<PhtNode>();

        Iterator<PhtNode> pnIt = set.descendingIterator();
        for (int i = 0; (i < nb) && (pnIt.hasNext()); i++) {
            PhtNode pn = pnIt.next();
            
            if (pn != null) {
                mukpn.add(pn);
            }
        }
        
        return mukpn;
    }

    public void printAll (int mu) {
        int nbLeavesBinf = this.kpnLeavesBinf.size();
        int nbLeavesBsup = this.kpnLeavesBsup.size();
        int nbLeaves     = this.kpnLeavesBinf.size() + this.kpnLeavesBsup.size();

        System.out.printf("Number of PhtNodes: %d (%d leaves)\n%d most used PhtNodes\n",
                this.pn.size(), nbLeaves, mu);
        for (PhtNode nd: mostPN(this.pn, mu)) {
            System.out.printf("\t[%s] used %d times (%d times as destination)\n",
                    nd.getLabel(), nd.getUsage(), nd.getUsageDest());
        }

        System.out.printf("\nNumber of leaves with less than %d keys: %d\n",
                PhtProtocol.B, nbLeavesBinf);
        for (PhtNode nd: mostPN(this.kpnLeavesBinf, mu)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    nd.getLabel(), nd.getNbKeys(), nd.getUsage());
        }

        System.out.printf("\nNumber of leaves with more than %d keys: %d\n",
                PhtProtocol.B, nbLeavesBsup);
        for (PhtNode nd: mostPN(this.kpnLeavesBsup, mu)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    nd.getLabel(), nd.getNbKeys(), nd.getUsage());
        }

        System.out.printf("\nMin, avg, max in the 'less than B keys set':\n"
                + "[%s] %d keys <> %.1f keys on average <> [%s] %d keys\n",
                this.minKeysBinf.getLabel(), this.minKeysBinf.getNbKeys(),
                (float)this.nbKeysBinf / (float)nbLeavesBinf,
                this.maxKeysBinf.getLabel(), this.maxKeysBinf.getNbKeys());

        System.out.printf("\nMin, avg, max in the 'more than B keys set':\n"
                        + "[%s] %d keys <> %.1f keys on average <> [%s] %d keys\n",
                this.minKeysBsup.getLabel(), this.minKeysBsup.getNbKeys(),
                (float)this.nbKeysBsup / (float)nbLeavesBsup,
                this.maxKeysBsup.getLabel(), this.maxKeysBsup.getNbKeys());
    }

    /* __________________                                   _________________ */
    /* __________________ Inner classes for TreeSet sorting _________________ */

    /**
     * Sort PhtNode by usage.
     */
    public static class PhtNodeCompUsage implements Comparator<PhtNode> {
        @Override
        public int compare(PhtNode phtNode, PhtNode t1) {

            // Avoid any possible error due to a long to int cast
            if (phtNode.getUsage() < t1.getUsage()) {
                return -1;
            } else if (phtNode.getUsage() == t1.getUsage()) {

                /*
                 * phtNode < t1 ? (if)
                 * phtNode > t1 ? (else if)
                 */
                if (PhtUtil.infTo(t1, phtNode)) {
                    return -1;
                } else if (PhtUtil.supTo(t1, phtNode)) {
                    return 1;
                } else {
                    return 0;
                }

            } else {
                return 1;
            }
        }
    }

    /**
     * Sort PhtNode by number of keys. If to number of keys are equal, sort
     * by usage.
     */
    public static class PhtNodeCompKeys implements Comparator<PhtNode> {
        @Override
        public int compare(PhtNode phtNode, PhtNode t1) {

            // Avoid any possible error due to a long to int cast
            if (phtNode.getNbKeys() < t1.getNbKeys()) {
                return -1;
            } else if (phtNode.getNbKeys() == t1.getNbKeys()) {

                if (phtNode.getUsage() < t1.getUsage()) {
                    return -1;
                } else if (phtNode.getUsage() == t1.getUsage()) {

                    /*
                     * phtNode < t1 ? (if)
                     * phtNode > t1 ? (else if)
                     */
                    if (PhtUtil.infTo(t1, phtNode)) {
                        return -1;
                    } else if (PhtUtil.supTo(t1, phtNode)) {
                        return 1;
                    } else {
                        return 0;
                    }

                } else {
                    return 1;
                }
            } else {
                return 1;
            }
        }
    }
}
