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
class PhtNodeStats {
    /*
     * Tree with all the PhtNodes in the network
     */
    private ArrayList<PhtNodeInfo> pn;

    /*
     * Trees with all the PhtNode leaves, the leaves with less than
     * PhtProtocol.B keys, with more than PhtProtocol.B keys
     *
     * SORTED BY NUMBER OF KEYS
     */
    private ArrayList<PhtNodeInfo> kpnLeavesBinf;
    private ArrayList<PhtNodeInfo> kpnLeavesBsup;

    // Number of keys
    private int nbKeysBinf;
    private int nbKeysBsup;

    // Min and max keys
    private PhtNode minKeysBinf;
    private PhtNode minKeysBsup;
    private PhtNode maxKeysBinf;
    private PhtNode maxKeysBsup;

    public PhtNodeStats() {
        this.pn            = new ArrayList<PhtNodeInfo>();
        this.kpnLeavesBinf = new ArrayList<PhtNodeInfo>();
        this.kpnLeavesBsup = new ArrayList<PhtNodeInfo>();
    }

    /**
     * Add a PhtNodeStats into the right TreeMap (leaf or not a leaf)
     * @param pn PhtNode information to add
     */
    public void addPN (PhtNode pn, boolean isLeaf) {
        PhtNodeInfo pni;

        pni = new PhtNodeInfo(pn.getLabel(), pn.getUsage(), pn.getUsageDest(), pn.getNbKeys());

        if (isLeaf) {

            if (pn.getNbKeys() <= PhtProtocol.B) {
                this.kpnLeavesBinf.add(pni);
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
                this.kpnLeavesBsup.add(pni);
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
        this.pn.add(pni);
    }

    /* ___________________                                ___________________ */
    /* ___________________ Sort by number of keys / usage ___________________ */

    /**
     * Retrieve the nb upper nodes in the given TreeSet.
     * The 'set' is sorted: do not use this method more than once on the
     * same set (it would be expansive).
     * @param set Source list
     * @param comparator for sorting
     * @param nb maximum of PhtNode to return
     * @return list of nb (at most) PhtNode
     * keys in the TreeSet set.
     */
    private List<PhtNodeInfo> mostPN (List<PhtNodeInfo> set, Comparator<PhtNodeInfo> comparator, int nb) {
        List<PhtNodeInfo> mukpn = new LinkedList<PhtNodeInfo>();

        if (set.isEmpty()) {
            return mukpn;
        }

        set.sort(comparator);
        for (int i = 0; i < nb; i++) {
            PhtNodeInfo pni;

            pni = set.get( set.size()-i-1 );
            
            if (pn != null) {
                mukpn.add(pni);
            }
        }
        
        return mukpn;
    }

    /* _______________________________        _______________________________ */
    /* _______________________________ Height _______________________________ */

    /**
     * Print every information on the PhtNodes.
     * @param mu Maximum number of PhtNodes to print each time many could
     *           be.
     */
    public void printAll (int mu) {
        int nbLeavesBinf = this.kpnLeavesBinf.size();
        int nbLeavesBsup = this.kpnLeavesBsup.size();
        int nbLeaves     = this.kpnLeavesBinf.size() + this.kpnLeavesBsup.size();

        System.out.printf("Number of PhtNodes: %d (%d leaves)\n%d most used PhtNodes\n",
                this.pn.size(), nbLeaves, mu);
        for (PhtNodeInfo ndi: mostPN(this.pn, new PhtNodeCompUsage(), mu)) {
            System.out.printf("\t[%s] used %d times (%d times as destination)\n",
                    ndi.getLabel(), ndi.getUsage(), ndi.getUsageDest());
        }

        System.out.printf("\nNumber of leaves with less than %d keys: %d\n",
                PhtProtocol.B, nbLeavesBinf);
        for (PhtNodeInfo ndi: mostPN(this.kpnLeavesBinf, new PhtNodeCompKeys(), mu)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        System.out.printf("\nNumber of leaves with more than %d keys: %d\n",
                PhtProtocol.B, nbLeavesBsup);
        for (PhtNodeInfo ndi: mostPN(this.kpnLeavesBsup, new PhtNodeCompKeys(), mu)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        if (this.minKeysBinf != null && this.maxKeysBinf != null) {
            System.out.printf("\nMin, avg, max in the 'less than B keys set':\n"
                            + "[%s] %d keys <> %.1f keys on average <> [%s] %d keys\n",
                    this.minKeysBinf.getLabel(), this.minKeysBinf.getNbKeys(),
                    (float) this.nbKeysBinf / (float) nbLeavesBinf,
                    this.maxKeysBinf.getLabel(), this.maxKeysBinf.getNbKeys());
        }

        if (this.minKeysBsup != null && this.maxKeysBsup != null) {
            System.out.printf("\nMin, avg, max in the 'more than B keys set':\n"
                            + "[%s] %d keys <> %.1f keys on average <> [%s] %d keys\n",
                    this.minKeysBsup.getLabel(), this.minKeysBsup.getNbKeys(),
                    (float) this.nbKeysBsup / (float) nbLeavesBsup,
                    this.maxKeysBsup.getLabel(), this.maxKeysBsup.getNbKeys());
        }
    }

    /* __________________                                   _________________ */
    /* __________________ Inner classes for TreeSet sorting _________________ */

    /**
     * Sort PhtNode by usage.
     */
    public static class PhtNodeCompUsage implements Comparator<PhtNodeInfo> {
        @Override
        public int compare(PhtNodeInfo phtNode, PhtNodeInfo t1) {

            // Avoid any possible error due to a long to int cast
            if (phtNode.getUsage() < t1.getUsage()) {
                return -1;
            } else if (phtNode.getUsage() == t1.getUsage()) {

                /*
                 * phtNode < t1 ? (if)
                 * phtNode > t1 ? (else if)
                 */
                if (PhtUtil.infTo(t1.getLabel(), phtNode.getLabel())) {
                    return -1;
                } else if (PhtUtil.supTo(t1.getLabel(), phtNode.getLabel())) {
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
    public static class PhtNodeCompKeys implements Comparator<PhtNodeInfo> {
        @Override
        public int compare(PhtNodeInfo phtNode, PhtNodeInfo t1) {

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
                    if (PhtUtil.infTo(t1.getLabel(), phtNode.getLabel())) {
                        return -1;
                    } else if (PhtUtil.supTo(t1.getLabel(), phtNode.getLabel())) {
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
