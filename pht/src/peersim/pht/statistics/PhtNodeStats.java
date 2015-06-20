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

    private static Comparator<PhtNodeInfo> compKeys;
    private static Comparator<PhtNodeInfo> compUsage;
    private static Comparator<PhtNodeInfo> compHeight;

    /*
     * The three ArrayList above must be sorted before starting some
     * computing.
     */
    private boolean sorted = false;

    /*
     * Number of keys
     */
    private int nbKeysBinf;
    private int nbKeysBsup;

    /*
     * Min and max keys
     */
    private PhtNode minKeysBinf;
    private PhtNode minKeysBsup;
    private PhtNode maxKeysBinf;
    private PhtNode maxKeysBsup;

    /*
     * Final fields the height and 0 keys parts (uses arrays)
     */
    private static final int MIN  = 0;
    private static final int AVG  = 1;
    private static final int MED  = 2;
    private static final int MAX  = 3;
    private static final int TOT  = 4;

    private static final int HEIGHT_SIZE = 4;
    private static final int ZKEYS_SIZE  = 5;

    public PhtNodeStats() {
        this.pn            = new ArrayList<PhtNodeInfo>();
        this.kpnLeavesBinf = new ArrayList<PhtNodeInfo>();
        this.kpnLeavesBsup = new ArrayList<PhtNodeInfo>();

        // Sort PhtNode by number of keys. If to number of keys are equal,
        // sort by usage. If they are equal, sort by label.
        compKeys =  new Comparator<PhtNodeInfo>() {
            @Override
            public int compare(PhtNodeInfo pni, PhtNodeInfo t1) {

                // Avoid any possible error due to a long to int cast
                if (pni.getNbKeys() < t1.getNbKeys()) {
                    return -1;
                } else if (pni.getNbKeys() == t1.getNbKeys()) {

                    if (pni.getUsage() < t1.getUsage()) {
                        return -1;
                    } else if (pni.getUsage() == t1.getUsage()) {

                        /*
                         * pni < t1 ? (if)
                         * pni > t1 ? (else if)
                         */
                        if (PhtUtil.infTo(t1.getLabel(), pni.getLabel())) {
                            return -1;
                        } else if (PhtUtil.supTo(t1.getLabel(), pni.getLabel())) {
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
        };

        // Sort PhtNode by usage. If they are equal, sort by label.
        compUsage =  new Comparator<PhtNodeInfo>() {
            @Override
            public int compare(PhtNodeInfo pni, PhtNodeInfo t1) {

                // Avoid any possible error due to a long to int cast
                if (pni.getUsage() < t1.getUsage()) {
                    return -1;
                } else if (pni.getUsage() == t1.getUsage()) {

                    /*
                     * pni < t1 ? (if)
                     * pni > t1 ? (else if)
                     */
                    if (PhtUtil.infTo(t1.getLabel(), pni.getLabel())) {
                        return -1;
                    } else if (PhtUtil.supTo(t1.getLabel(), pni.getLabel())) {
                        return 1;
                    } else {
                        return 0;
                    }

                } else {
                    return 1;
                }
            }
        };

        // Sort by label length. If they are equal, sort by label.
        compHeight = new Comparator<PhtNodeInfo>() {
            @Override
            public int compare(PhtNodeInfo pni, PhtNodeInfo t1) {
                if (pni.getLabel().length() != t1.getLabel().length()) {
                    return pni.getLabel().length() - t1.getLabel().length();
                } else {

                    /*
                     * pni < t1 ? (if)
                     * pni > t1 ? (else if)
                     */
                    if (PhtUtil.infTo(t1.getLabel(), pni.getLabel())) {
                        return -1;
                    } else if (PhtUtil.supTo(t1.getLabel(), pni.getLabel())) {
                        return 1;
                    } else {
                        return 0;
                    }

                }
            }
        };
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
     * @param nb maximum of PhtNode to return
     * @return list of nb (at most) PhtNode
     * keys in the TreeSet set.
     */
    private List<PhtNodeInfo> mostPN (List<PhtNodeInfo> set, int nb) {
        List<PhtNodeInfo> mukpn = new LinkedList<PhtNodeInfo>();

        if (set.isEmpty()) {
            return mukpn;
        }

        if (! this.sorted) {
            sortAll();
        }

        for (int i = 0; (i < nb) && (i < set.size()); i++) {
            PhtNodeInfo pni;

            pni = set.get( set.size()-i-1 );
            
            if (pn != null) {
                mukpn.add(pni);
            }
        }
        
        return mukpn;
    }

    /* ________________________________      ________________________________ */
    /* ________________________________ Sort ________________________________ */

    private void sortAll () {
        this.pn.sort( compUsage );
        this.kpnLeavesBinf.sort( compKeys );
        this.kpnLeavesBsup.sort( compKeys );

        this.sorted = true;
    }

    /* _______________________________        _______________________________ */
    /* _______________________________ Height _______________________________ */

    /**
     * Heights
     * @return min, avg, median, max heights
     */
    private int[] heights () {
        int size = 0;
        int[] ha = new int[HEIGHT_SIZE];
        List<PhtNodeInfo> pnil = new ArrayList<PhtNodeInfo>( this.pn );

        if (! this.sorted) {
            sortAll();
        }

        pnil.sort( compHeight );

        // Min and max
        ha[MIN] = pnil.get(0).getLabel().length();
        ha[MAX] = pnil.get( pnil.size()-1 ).getLabel().length();

        // Average
        ha[AVG] = 0;
        for (PhtNodeInfo pni: pnil) {
            ha[AVG] += pni.getLabel().length();
            size++;
        }
        ha[AVG] = ha[AVG] / size;

        // Median
        if (size % 2 != 0) {
            ha[MED] = pnil.get( (size/2)+1 ).getLabel().length();
        } else {
            ha[MED]  = pnil.get( size/2 ).getLabel().length();
            ha[MED] += pnil.get( (size/2)+1 ).getLabel().length();
            ha[MED]  = ha[MED] / 2;
        }

        return ha;
    }

    /* __________________________                 ___________________________ */
    /* __________________________ 0 Keys PhtNodes ___________________________ */

    private int[] zeroKeysLeaves () {
        int min;    // Minimum height
        int max;    // Maximum height
        int tot;    // Total heights (for average)
        int minIdx; // Number of leaves
        int[] zkl = new int[ZKEYS_SIZE];
        List<PhtNodeInfo> zkPni = new LinkedList<PhtNodeInfo>();

        if (! this.sorted) {
            sortAll();
        }

        zkPni.addAll( this.kpnLeavesBinf );
        zkPni.addAll( this.kpnLeavesBsup );
        zkPni.sort( compKeys );

        min = zkPni.get(0).getLabel().length();
        max = min;
        tot = 0;

        for (minIdx = 0; minIdx < zkPni.size(); minIdx++) {
            PhtNodeInfo pni = zkPni.get(minIdx);

            if (pni.getNbKeys() > 0) {
                minIdx++;
                break;
            } else if (pni.getLabel().length() < min) {
                min = pni.getLabel().length();
            } else if (pni.getLabel().length() > max) {
                max = pni.getLabel().length();
            }

            tot += pni.getLabel().length();
        }

        zkl[MIN] = min;
        zkl[AVG] = tot / minIdx;
        zkl[MAX] = max;
        zkl[TOT] = minIdx;

        return zkl;
    }

    /**
     * Print every information on the PhtNodes.
     * @param mu Maximum number of PhtNodes to print each time many could
     *           be.
     */
    public void printAll (int mu) {
        int nbLeavesBinf = this.kpnLeavesBinf.size();
        int nbLeavesBsup = this.kpnLeavesBsup.size();
        int nbLeaves     = this.kpnLeavesBinf.size() + this.kpnLeavesBsup.size();

        /* ---------- Usage ---------- */

        System.out.printf("Number of PhtNodes: %d (%d leaves)\n\n%d most used PhtNodes\n",
                this.pn.size(), nbLeaves, mu);
        for (PhtNodeInfo ndi: mostPN(this.pn, mu)) {
            System.out.printf("\t[%s] used %d times (%d times as destination)\n",
                    ndi.getLabel(), ndi.getUsage(), ndi.getUsageDest());
        }

        /* ---------- Keys ---------- */

        System.out.println("\n---------- Keys ----------");
        System.out.printf("\nNumber of leaves with less than %d keys: %d\n",
                PhtProtocol.B, nbLeavesBinf);
        for (PhtNodeInfo ndi: mostPN(this.kpnLeavesBinf, mu)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        System.out.printf("\nNumber of leaves with more than %d keys: %d\n",
                PhtProtocol.B, nbLeavesBsup);
        for (PhtNodeInfo ndi: mostPN(this.kpnLeavesBsup, mu)) {
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

        /* ---------- Zero key leaves ---------- */
        int[] zkl = zeroKeysLeaves();
        System.out.println("\n---------- Zero key leaves ----------");
        System.out.printf("\nMin, avg, max heights of leaves with zero keys: %d, %d, %d\n" +
                        "Total number of keys with zero keys: %d\n",
                zkl[MIN], zkl[AVG], zkl[MAX], zkl[TOT]);

        /* ---------- Heights ---------- */

        int[] ha = heights();
        System.out.println("\n---------- Heights ----------");
        System.out.printf("\nMin, avg, median, max heights: \n%d, %d, %d, %d\n",
                ha[MIN], ha[AVG], ha[MED], ha[MAX]);
    }
}
