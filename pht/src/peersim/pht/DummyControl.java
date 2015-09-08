package peersim.pht;

import peersim.core.CommonState;
import peersim.core.Control;
import peersim.pht.statistics.Stats;

/**
 * TODO: clean this up
 */
public class DummyControl implements Control {
    private ClientInterlocutor ci;

    /**
     * Default constructor: get the ClientInterlocutor instance.
     * @param prefix not used (PeerSim wants a constructor with a String)
     */
    public DummyControl (String prefix) {
        this.ci = ClientInterlocutor.getInstance();
    }

    @Override
    public boolean execute() {

        if (CommonState.getPhase() == CommonState.POST_SIMULATION) {
            if (! this.ci.queueEmpty()) {
                System.out.println("DC :: post-simulation");
                CommonState.setPhase(CommonState.PHASE_UNKNOWN);
            } else {
                Stats.getInstance().end();
                return true;
            }
        }

        this.ci.execute();
        return false;
    }
}
