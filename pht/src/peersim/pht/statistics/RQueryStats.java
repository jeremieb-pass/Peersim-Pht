package peersim.pht.statistics;

import peersim.pht.messages.PMRangeQuery;
import peersim.pht.messages.PhtMessage;

import java.util.*;

/**
 * <p>
 *     Singleton class for Range queries statistics.
 * </p>
 * <ul>
 *     <li>Sort range queries by count</li>
 *     <li>count queries requested by the client</li>
 *     <li>count queries messages</li>
 * </ul>
 */
class RQueryStats {
    private static RQueryStats rqst;

    /*
     * Sequential queries sorted by count.
     */
    private TreeSet<PMRQueryHolder> seqh;

    /*
     * Parallel queries sorted by count.
     */
    private TreeSet<PMRQueryHolder> parh;

    /*
     * Sequential and parallel range queries requested by the client.
     */
    private long seqCountOp;
    private long parCountOp;

    /*
     * Sequential and parallel range queries messages.
     */
    private long seqCount;
    private long parCount;

    private RQueryStats() {
        this.seqh = new TreeSet<PMRQueryHolder>();
        this.parh = new TreeSet<PMRQueryHolder>();
    }

    public static synchronized RQueryStats getInstance () {
        if (rqst == null) {
            rqst = new RQueryStats();
        }

        return rqst;
    }

    /* _______________________                        _______________________ */
    /* _______________________ Range queries counters _______________________ */

    /**
     * Increment by one the number of range queries requested by the client.
     * @param type PhtMessage.SEQ_QUERY or PhtMessage.PAR_QUERY.
     */
    public void incClientRangeQuery(int type) {
        if (type == PhtMessage.SEQ_QUERY) {
            this.seqCountOp++;
        } else {
            this.parCountOp++;
        }
    }

    /**
     * Increment by one the number of range queries messages.
     * @param type PhtMessage.SEQ_QUERY or PhtMessage.PAR_QUERY.
     */
    public void incRangeQuery(int type) {
        if (type == PhtMessage.SEQ_QUERY) {
            this.seqCount++;
        } else {
            this.parCount++;
        }
    }

    /**
     * Sequential range queries requested by the client.
     * @return Number of sequential range queries requested by the client.
     */
    public long seqClientRQueries () {
        return this.seqCountOp;
    }

    /**
     * Sequential range queries messages.
     * @return Number of sequential range queries messages.
     */
    public long seqRQueries () {
        return this.seqCount;
    }

    /**
     * Parallel range queries requested by the client.
     * @return Number of parallel range queries requested by the client.
     */
    public long parClientRQueries () {
        return this.parCountOp;
    }
    /**
     * Parallel range queries messages.
     * @return Number of parallel range queries messages.
     */
    public long parRQueries () {
        return this.parCount;
    }

    /* _________________________                     ________________________ */
    /* _________________________ Range query TreeSet ________________________ */

    /**
     * Add a new node in the PMRQueryHolder TreeSet
     * @param message To get the id
     * @param pmrq Get the other fields
     * @param seq Sequential or parallel query
     */
    public void addPMRQuery (PhtMessage message, PMRangeQuery pmrq, boolean seq) {
        if (seq) {
            this.seqh.add(
                    new PMRQueryHolder(
                            message.getId(),
                            pmrq.getCount(),
                            pmrq.getKeyMin(),
                            pmrq.getKeyMax()
                    )
            );
        } else {
            this.parh.add(
                    new PMRQueryHolder(
                            message.getId(),
                            pmrq.getCount(),
                            pmrq.getKeyMin(),
                            pmrq.getKeyMax()
                    )
            );
        }
    }

    /**
     * List of the maximum Range queries by count (queries that passed
     * through PhtNodes)
     * @param set TreeSet to use
     * @param nb Maximum number of items in the list
     * @return List of max PMRQueryHolder
     */
    public List<PMRQueryHolder> maxRQueries (TreeSet<PMRQueryHolder> set, int nb) {
        List<PMRQueryHolder> max = new LinkedList<PMRQueryHolder>();

        if (set.isEmpty()) {
            return max;
        }

        Iterator<PMRQueryHolder> pmrqhIt = set.descendingIterator();
        for (int i = 0; (i < nb) && (pmrqhIt.hasNext()); i++) {
            PMRQueryHolder pmrqh = pmrqhIt.next();

            if (pmrqh != null) {
                max.add(pmrqh);
            }
        }

        Collections.reverse(max);
        return max;
    }

    /**
     * List of the minimum Range queries by count (queries that passed
     * through PhtNodes)
     * @param set TreeSet to use
     * @param nb Maximum number of items in the list
     * @return List of max PMRQueryHolder
     */
    public List<PMRQueryHolder> minRQueries (TreeSet<PMRQueryHolder> set, int nb) {
        List<PMRQueryHolder> min = new LinkedList<PMRQueryHolder>();

        if (set.isEmpty()) {
            return min;
        }

        Iterator<PMRQueryHolder> pmrqhIt = set.iterator();
        for (int i = 0; (i < nb) && (pmrqhIt.hasNext()); i++) {
            PMRQueryHolder pmrqh = pmrqhIt.next();

            if (pmrqh != null) {
                min.add(pmrqh);
            }
        }

        return min;
    }


    /* ________________________________      ________________________________ */
    /* ________________________________ View ________________________________ */

    public void printAll () {
        int nb = 2;

        // General
        System.out.printf("Number of range queries: %d (%d sequential, %d parallel)\n",
                this.seqCountOp + this.parCountOp,
                this.seqCountOp,
                this.parCountOp);
        System.out.printf("Number of range queries messages: %d (%d sequential, %d parallel)\n",
                this.seqCount + this.parCount,
                this.seqCount,
                this.parCount);

        // Sequential queries
        System.out.printf("\n%d minimum sequential queries (number of PhtNodes involded)\n",
                this.seqCountOp < nb ? 0 : nb);
        for (PMRQueryHolder seqh: minRQueries(this.seqh, nb)) {
            System.out.printf("['%s' to '%s'] %d PhtNodes involded\n",
                    seqh.getKeyMin(), seqh.getKeyMax(), seqh.getCount());
        }
        System.out.printf("\n%d maximum sequential queries (number of PhtNodes involded)\n",
                this.seqCountOp < nb ? 0 : nb);
        for (PMRQueryHolder seqh: maxRQueries(this.seqh, nb)) {
            System.out.printf("['%s' to '%s'] %d PhtNodes involded\n",
                    seqh.getKeyMin(), seqh.getKeyMax(), seqh.getCount());
        }

        // Parallel queries
        System.out.printf("\n%d minimum parallel queries (number of PhtNodes involded)\n",
                this.parCountOp < nb ? 0 : nb);
        for (PMRQueryHolder parh: maxRQueries(this.parh, nb)) {
            System.out.printf("['%s' to '%s'] %d PhtNodes involded\n",
                    parh.getKeyMin(), parh.getKeyMax(), parh.getCount());
        }
        System.out.printf("\n%d maximum parallel queries (number of PhtNodes involded)\n",
                this.parCountOp < nb ? 0 : nb);
        for (PMRQueryHolder parh: maxRQueries(this.parh, nb)) {
            System.out.printf("['%s' to '%s'] %d PhtNodes involded\n",
                    parh.getKeyMin(), parh.getKeyMax(), parh.getCount());
        }
    }
}
