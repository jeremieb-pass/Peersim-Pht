package peersim.pht.statistics;

import peersim.pht.messages.PMRangeQuery;
import peersim.pht.messages.PhtMessage;

import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * <p>
 *     Singleton class containing a stack of {@link peersim.pht.statistics.PhaseStats}
 *     and two general information about the simulation:
 * </p>
 * <ol>
 *     <li>Number of operations (equal to {@link peersim.pht.PhtProtocol}'s nextId field).</li>
 *     <li>Number of {@link peersim.pht.statistics.PhaseStats}.</li>
 * </ol>
 * <p>
 *     Stats creates phases and provides an access to the current (latest) phase.
 * </p>
 *
 */
public class Stats {
    private static Stats stats;

    private Stack<PhaseStats> ps;
    private PhaseStats curr;

    // Number of operations triggered during the simulation.
    private long opCount;

    // Number of phases.
    private int nbPhases;

    private Stats() {
        this.ps = new Stack<PhaseStats>();
        this.ps.add(new PhaseStats());
        this.curr = this.ps.peek();
    }

    public synchronized static Stats getInstance() {
        if (stats == null) {
            stats = new Stats();
        }

        return stats;
    }

    /**
     * Current phase
     *
     * @return return the current phase.
     */
    public PhaseStats curr() {
        return this.curr;
    }

    /**
     * Add a new phase for new statistics.
     */
    public void newPhase() {
        this.ps.push(new PhaseStats());
        this.curr = this.ps.peek();
    }
}
