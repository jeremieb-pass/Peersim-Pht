package peersim.pht.statistics;

import peersim.core.Network;
import peersim.pht.PhtNode;
import peersim.pht.PhtProtocol;
import peersim.pht.messages.PMRangeQuery;
import peersim.pht.messages.PhtMessage;

import java.util.*;

/**
 * <p>Main class for Pht statistics.</p>
 *
 * <p>
 *     The goal is to collect as much information as possible about
 * </p>
 * <ol>
 *     <li>PhtNode usage</li>
 *     <li>Node usage</li>
 *     <li>Keys in leaves</li>
 *     <li>Lookups</li>
 * </ol>
 *
 * <p>
 *     This is a singleton class. The instantiation will be done before the beginning of the simulation, hence the two
 *     methods 'init' and 'getInstance'.
 * </p>
 * <p>
 *     Data on PhtNodes and Nodes is gathered at the end of the simulation (or at the end of a phase - defined by the
 *     user). This is done to avoid an expansive heavy-centralized design in which everything has to pass through the
 *     Stats class (access to a hash table or a tree to find a PhtNode by its label, update the correct field each
 *     time). Lookups or operation information on the other hand can be handled directly.
 * </p>
 */
public class Stats {
    protected static Stats stats;

    private TreeSet<NodeStats> nStats;
    private PhtNodeStats pnStats;
    private RQueryStats rqStats;

    /* __________ Operations counters __________ */

    /*
     * Lookup operation requested by the client.
     */
    private long linCountOp;
    private long binCountOp;

    /*
     * Lookup messages
     */
    private long linCount;
    private long binCount;

    /*
     * Insert and delete operations requested by the client.
     */
    private long insertCount;
    private long deleteCount;

    /*
     * Split and merge operations requested by the PhtNodes.
     */
    private long splitCount;
    private long mergeCount;

    /*
     * How many avoided due to choices made (one split by insertion max,
     * one merge by delete max).
     */
    private long splitAvoided;
    private long mergeAvoided;


    protected Stats() {
    }

    public static void init() {
        stats         = new Stats();
        stats.nStats  = new TreeSet<NodeStats>();
        stats.pnStats = PhtNodeStats.getInstance();
        stats.rqStats = RQueryStats.getInstance();
    }

    public static Stats getInstance() {
        return stats;
    }

    /* _________________________                    _________________________ */
    /* _________________________ PhtNode statistics _________________________ */


    /* ___________________________                 __________________________ */
    /* ___________________________ Node statistics __________________________ */

    /**
     * Add a NodeStats into the TreeMap
     * @param nst Node information to add
     */
    public void addN (NodeStats nst) {
        this.nStats.add(nst);
    }

    /**
     * Add a PhtNodeStats into the TreeMap
     * @param id Node's id
     * @param usage PhtNode's usage value
     * @param usageDest PhtNode's usageDest value
     */
    public void addN (long id, long usage, long usageDest) {
        this.nStats.add(new NodeStats(id, usage, usageDest));
    }

    /**
     * Most used PhtNodes.
     * @param nb maximum number of Node
     * @return List of the nb most used Nodes
     */
    public List<NodeStats> mostUsedNST (int nb) {
        List<NodeStats> munst = new LinkedList<NodeStats>();

        for (int i = 0; i < nb; i++) {
            NodeStats n = stats.nStats.pollLast();

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

    /* ________________________                        ______________________ */
    /* ________________________ Add PhtNodes and Nodes ______________________ */

    /**
     * Add all the Nodes and all the PhtNodes into the appropriate trees.
     * This method should be call and the end of the simulation to get all
     * the information before making the statistics
     */
    public void addNetwork () {
        int phtid = PhtProtocol.getPid();

        for (int i = 0; i < Network.size(); i++) {
            PhtProtocol prot;

            prot = (PhtProtocol) Network.get(i).getProtocol(phtid);
            addN( prot.getId(), prot.getUsage(), prot.getUsageDest() );

            for (PhtNode nd: prot.getNodes().values()) {
                this.pnStats.addPN(nd, nd.isLeaf());
            }
        }
    }

    /* _________________________                   __________________________ */
    /* _________________________ Lookup statistics __________________________ */

    /**
     * Increment by one the number of lookups requested by the client.
     * @param type PhtMessage.LIN_LOOKUP or PhtMessage.BIN_LOOKUP
     */
    public void incClientLookup (int type) {
        if (type == PhtMessage.LIN_LOOKUP) {
            this.linCountOp++;
        } else if (type == PhtMessage.BIN_LOOKUP) {
            this.binCountOp++;
        }
    }

    /**
     * Increment by one the number of lookups.
     * @param type PhtMessage.LIN_LOOKUP or PhtMessage.BIN_LOOKUP
     */
    public void incLookup (int type) {
        if (type == PhtMessage.LIN_LOOKUP) {
            this.linCount++;
        } else if (type == PhtMessage.BIN_LOOKUP) {
            this.binCount++;
        }
    }

    /**
     * Linear lookups requested.
     * @return Number of linear lookups requested by the client.
     */
    public long linClientLookups () {
        return this.linCountOp;
    }

    /**
     * Linear lookups messages
     * @return Number of linear lookups during the simulation.
     */
    public long linLookups () {
        return this.linCount;
    }

    /**
     * Binary lookups requested.
     * @return Number of linear lookups requested by the client.
     */
    public long binClientLookups () {
        return this.binCountOp;
    }

    /**
     * Binary lookups messages
     * @return Number of linear lookups during the simulation.
     */
    public long binLookups () {
        return this.binCount;
    }

    /* _____________________________               __________________________ */
    /* _____________________________ Range queries __________________________ */

    /**
     * Increment by one the number of range queries requested by the client.
     * @param type PhtMessage.SEQ_QUERY or PhtMessage.PAR_QUERY.
     */
    public void incClientRangeQuery(int type) {
        this.rqStats.incClientRangeQuery(type);
    }

    /**
     * Increment by one the number of range queries messages.
     * @param type PhtMessage.SEQ_QUERY or PhtMessage.PAR_QUERY.
     */
    public void incRangeQuery(int type) {
        this.rqStats.incRangeQuery(type);
    }

    /**
     * Sequential range queries requested by the client.
     * @return Number of sequential range queries requested by the client.
     */
    public long seqClientRQueries () {
        return this.rqStats.seqClientRQueries();
    }

    /**
     * Sequential range queries messages.
     * @return Number of sequential range queries messages.
     */
    public long seqRQueries () {
        return this.rqStats.seqRQueries();
    }

    /**
     * Parallel range queries requested by the client.
     * @return Number of parallel range queries requested by the client.
     */
    public long parClientRQueries () {
        return this.rqStats.parClientRQueries();
    }
    /**
     * Parallel range queries messages.
     * @return Number of parallel range queries messages.
     */
    public long parRQueries () {
        return this.rqStats.parRQueries();
    }

    /**
     * Add a new node in the PMRQueryHolder TreeSet
     * @param message To get the id
     * @param pmrq Get the other fields
     */
    public void addSeqQuery (PhtMessage message, PMRangeQuery pmrq) {
        this.rqStats.addPMRQuery(message, pmrq, true);
    }

    /**
     * Add a new node in the PMRQueryHolder TreeSet
     * @param message To get the id
     * @param pmrq Get the other fields
     */
    public void addParQuery (PhtMessage message, PMRangeQuery pmrq) {
        this.rqStats.addPMRQuery(message, pmrq, false);
    }

    /* __________________________                   _________________________ */
    /* __________________________ Insert statistics _________________________ */

    /**
     * Increment by one the number of insert requested by the client.
     */
    public void incInsert () {
        this.insertCount++;
    }

    /* __________________________                   _________________________ */
    /* __________________________ Delete statistics _________________________ */

    /**
     * Increment by one the number of delete requested by the client.
     */
    public void incDelete() {
        this.deleteCount++;
    }

    /* __________________________                  __________________________ */
    /* __________________________ Split statistics __________________________ */

    /**
     * Increment by one the number of split requested by PhtNodes.
     */
    public void incSplit () {
        this.splitCount++;
    }

    /**
     * Increment by one the number of split that has been avoided due to the
     * choices made (no more than one split per insertion).
     */
    public void incSplitAvoid () {
        this.splitAvoided++;
    }

    /* __________________________                  __________________________ */
    /* __________________________ Merge statistics __________________________ */

    /**
     * Increment by one the number of merge requested by PhtNodes.
     */
    public void incMerge () {
        this.mergeCount++;
    }

    /**
     * Increment by one the number of merge that has been avoided due to the
     * choices made (no more than one merge per deletion).
     */
    public void incMergeAvoid () {
        this.mergeAvoided++;
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ Print _______________________________ */

    public void printAll() {
        final int mu = 5;

        if (this.nStats == null) {
            System.out.println("<> stats.nst null");
        }

        // PhtNodes
        System.out.printf("\n---------- PhtNode statistics ---------- \n\n");
        this.pnStats.printAll(mu);

        // Nodes
        System.out.printf("\n---------- Node statistics ------------- \n\n");
        System.out.printf("Number of Nodes: %d\n%d most used Nodes\n",
                this.nStats.size(), mu);
        for (NodeStats nt: mostUsedNST(mu)) {
            System.out.printf("\t[%d] used %d times (%d times as destination)\n",
                    nt.getId(),
                    nt.getUsage(),
                    nt.getUsageDest());
        }

        // Range queries
        System.out.printf("\n---------- Range queries statistics ------------- \n\n");
        this.rqStats.printAll();
    }
}
