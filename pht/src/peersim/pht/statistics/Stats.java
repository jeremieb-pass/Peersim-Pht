package peersim.pht.statistics;

import java.util.LinkedList;

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

    private LinkedList<PhaseStats> ps;
    private PhaseStats curr;

    // Number of operations triggered during the simulation.
    private long opCount;

    // When the simulation started
    private long startTime;

    // When the simulation ended
    private long endTime;

    private Stats() {
        this.ps = new LinkedList<PhaseStats>();
        this.ps.addFirst(new PhaseStats());
        this.curr      = this.ps.peekLast();
        this.startTime = System.currentTimeMillis();
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
        this.ps.addLast(new PhaseStats());
        this.curr = this.ps.peekLast();
    }

    public int getNbPhases () {
        return this.ps.size();
    }

    public void end() {
        this.endTime = System.currentTimeMillis();
    }

    /**
     * Remove last phase.
     * Usefull when you add a new phase after each computing: at the end of
     * the simulation, there will be an empty phase (which will be uselessly
     * printed)
     */
    public void removeLast () {
        this.ps.removeLast();
    }

    public void printAll() {
        int i = 0;
        long min  = 0;
        long sec  = 0;
        long msec = 0;
        long tot  = 0;

        if (this.endTime == 0) {
            end();
        }

        System.out.println(AsciiStats.phtSimulation);

        for (PhaseStats pst: this.ps) {
            System.out.println(AsciiStats.newPhase);
            System.out.printf("-------------------- Phase: %d --------------------\n", i);
            pst.printAll();
            i++;
        }

        tot = this.endTime - this.startTime;
        min = tot / 60000;

        tot = tot - (min * 60000);
        sec = tot / 1000;

        msec = tot - (sec * 1000);

        System.out.println(AsciiStats.generalInfo);
        System.out.printf ("The simulation lasted %d min, %d sec and %d msec\n",
                min, sec, msec);

        System.out.println(AsciiStats.toBoldlyGo);
    }
}
