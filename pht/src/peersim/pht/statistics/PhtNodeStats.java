package peersim.pht.statistics;

import peersim.pht.PhtNode;
import peersim.pht.PhtProtocol;

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

    protected PhtNodeStats() {
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
                        return 0;
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
                    return 0;
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
                    return 0;
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
     * Retrieve the nb upper nodes in the given List.
     * The 'set' is sorted: do not use this method more than once on the
     * same set (it would be expansive).
     * @param set Source list
     * @param nb maximum of PhtNode to return
     * @return list of nb (at most) PhtNode
     * keys in the List.
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
            PhtNodeInfo pni = set.get( set.size()-i-1 );
            
            if (pn != null) {
                mukpn.add(pni);
            }
        }
        
        return mukpn;
    }

    /**
     * Retrieve the nb lower nodes in the given List.
     * The 'set' is sorted: do not use this method more than once on the
     * same set (it would be expansive).
     * @param set Source list
     * @param nb maximum of PhtNode to return
     * @return list of nb (at most) PhtNode
     * keys in the List.
     */
    private List<PhtNodeInfo> lessPN (List<PhtNodeInfo> set, int nb) {
        List<PhtNodeInfo> lkpn = new LinkedList<PhtNodeInfo>();

        if (set.isEmpty()) {
            return lkpn;
        }

        if (! this.sorted) {
            sortAll();
        }

        for (int i = 0; (i < nb) && (i < set.size()); i++) {
            PhtNodeInfo pni = set.get(i);

            if (pn != null) {
                lkpn.add(pni);
            }
        }

        Collections.reverse(lkpn);
        return lkpn;
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
     * todo change to floats and repare the median
     * @return min, avg, median, max heights
     */
    private int[] heights () {
        int size = 0;
        int[] ha = new int[HEIGHT_SIZE];
        List<PhtNodeInfo> pnil = new ArrayList<PhtNodeInfo>( this.pn );

        if (! this.sorted) {
            sortAll();
        }

        if (this.pn.size() == 0) {
            ha[MIN] = ha[MAX] = ha[AVG] = ha[MED] = 0;
            return ha;
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
        int med1;
        int med2;

        if (pnil.size() <= 2) {
            med1 = 0;
            med2 = pnil.size()/2;
        } else {
            med1 = size/2;
            med2 = (size/2) + 1;
        }

        if (size % 2 != 0) {
            ha[MED] = pnil.get( med2 ).getLabel().length();
        } else {
            ha[MED]  = pnil.get( med1 ).getLabel().length();
            ha[MED] += pnil.get( med2 ).getLabel().length();
            ha[MED]  = ha[MED] / 2;
        }

        return ha;
    }

    /* __________________________                 ___________________________ */
    /* __________________________ 0 Keys PhtNodes ___________________________ */

    private float[] zeroKeysLeaves () {
        int min;    // Minimum height
        int max;    // Maximum height
        int tot;    // Total heights (for average)
        int minIdx; // Number of leaves
        float[] zkl = new float[ZKEYS_SIZE];
        List<PhtNodeInfo> zkPni = new LinkedList<PhtNodeInfo>();

        if (! this.sorted) {
            sortAll();
        }

        if ( (this.kpnLeavesBinf.size() + this.kpnLeavesBsup.size()) == 0 ) {
            zkl[MIN] = zkl[AVG] = zkl[MAX] = zkl[TOT] = 0;
            return zkl;
        }

        zkPni.addAll( this.kpnLeavesBinf );
        zkPni.addAll( this.kpnLeavesBsup );
        zkPni.sort( compKeys );

        if (zkPni.get(0).getNbKeys() > 0) {
            zkl[MIN] = 0;
            zkl[AVG] = 0;
            zkl[MAX] = 0;
            zkl[TOT] = 0;
            return zkl;
        }

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
        minIdx--;

        zkl[MIN] = min;
        zkl[AVG] = (float) tot / (float) minIdx;
        zkl[MAX] = max;
        zkl[TOT] = minIdx;

        return zkl;
    }

    /**
     * Print every information on the PhtNodes.
     * @param nb Maximum number of PhtNodes to print each time many could
     *           be.
     */
    public void printAll (int nb) {
        int even;
        int max;
        int nbLeavesBinf = this.kpnLeavesBinf.size();
        int nbLeavesBsup = this.kpnLeavesBsup.size();
        int nbLeaves     = this.kpnLeavesBinf.size() + this.kpnLeavesBsup.size();

        /* ---------- Usage ---------- */

        max = nb < this.pn.size() ? nb : this.pn.size();

        System.out.printf("Number of PhtNodes: %d (%d leaves)\n\n%d most used PhtNodes\n",
                this.pn.size(), nbLeaves, max > this.pn.size() ?  this.pn.size(): max);
        for (PhtNodeInfo ndi: mostPN(this.pn, max)) {
            System.out.printf("\t[%s] used %d times (%d times as destination)\n",
                    ndi.getLabel(), ndi.getUsage(), ndi.getUsageDest());
        }

        /* ---------- Keys (less than max) ---------- */

        max  = nb < this.kpnLeavesBinf.size() ? nb : this.kpnLeavesBinf.size();
        even = max % 2 == 0 ? 0 : 1;

        System.out.printf("\n---------- Keys (less than %d) ----------\n", PhtProtocol.B);
        System.out.printf("\nNumber of leaves with less than %d keys: %d\n"
                        + "\twith the most keys: \n",
                PhtProtocol.B, nbLeavesBinf);
        for (PhtNodeInfo ndi: mostPN(this.kpnLeavesBinf, (max/2) + even)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        System.out.printf("\n\twith the least keys: \n");
        for (PhtNodeInfo ndi: lessPN(this.kpnLeavesBinf, max/2)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        /* ---------- Keys (more than max) ---------- */

        max  = nb < this.kpnLeavesBsup.size() ? nb : this.kpnLeavesBsup.size();
        even = max % 2 == 0 ? 0 : 1;

        System.out.printf("\n\n---------- Keys (more than %d) ----------\n", PhtProtocol.B);
        System.out.printf("\nNumber of leaves with more than %d keys: %d\n"
                + "\twith the most keys: \n",
                PhtProtocol.B, nbLeavesBsup);
        for (PhtNodeInfo ndi: mostPN(this.kpnLeavesBsup, (max/2) + even)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        System.out.printf("\n\twith the least keys: \n");
        for (PhtNodeInfo ndi: lessPN(this.kpnLeavesBsup, max/2)) {
            System.out.printf("\t[%s] %d keys, used %d times\n",
                    ndi.getLabel(), ndi.getNbKeys(), ndi.getUsage());
        }

        /* ---------- Min, max, avg ---------- */

        System.out.printf("\n\n---------- Min, max , avg ----------\n");
        if (this.minKeysBinf != null && this.maxKeysBinf != null) {
            System.out.printf("\nMin, avg, max in the 'less than B keys set':\n"
                            + "[%s] %d keys <> %.3f keys on average <> [%s] %d keys\n",
                    this.minKeysBinf.getLabel(), this.minKeysBinf.getNbKeys(),
                    (float) this.nbKeysBinf / (float) nbLeavesBinf,
                    this.maxKeysBinf.getLabel(), this.maxKeysBinf.getNbKeys());
        }

        if (this.minKeysBsup != null && this.maxKeysBsup != null) {
            System.out.printf("\nMin, avg, max in the 'more than B keys set':\n"
                            + "[%s] %d keys <> %.3f keys on average <> [%s] %d keys\n",
                    this.minKeysBsup.getLabel(), this.minKeysBsup.getNbKeys(),
                    (float) this.nbKeysBsup / (float) nbLeavesBsup,
                    this.maxKeysBsup.getLabel(), this.maxKeysBsup.getNbKeys());
        }

        /* ---------- Zero key leaves ---------- */
        float[] zkl = zeroKeysLeaves();
        System.out.println("\n---------- Zero key leaves ----------");
        System.out.printf("\nMin, avg, max heights of leaves with zero keys:\n\t"
                        + "%.3f <> %.3f <> %.3f\n"
                        + "\nTotal number of leaves with zero keys: %.3f\n",
                zkl[MIN], zkl[AVG], zkl[MAX], zkl[TOT]);

        /* ---------- Heights ---------- */

        int[] ha = heights();
        System.out.println("\n---------- Heights ----------");
        System.out.printf("\nMin, avg, median, max heights: \n\t%d <> %d <> %d <> %d\n",
                ha[MIN], ha[AVG], ha[MED], ha[MAX]);
    }
}
