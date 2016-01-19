package peersim.pht.dht.mspastry;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.pastry.MSPastryProtocol;
import peersim.pastry.Message;
import peersim.pht.messages.PhtMessage;

/**
 * The main purpose of this class is to deliver the message received at the
 * MSPastryProtocol level to the PhtProtocol on the Node.
 */
public class MSPastryListener implements MSPastryProtocol.Listener, EDProtocol {
    private static String prefix;

    private final int phtid;
    private Node node;

    public MSPastryListener(String prefix) {
        MSPastryListener.prefix = prefix;
        this.phtid  = Configuration.getPid(prefix + ".phtid");
    }

    /**
     * Extract the PhtMessage from the Message (or the String for some tests
     * during initialization phase).
     * @param m Message received
     */
    @Override
    public void receive(Message m) {
        String init;
        PhtMessage message = null;

        if (m.body instanceof PhtMessage) {
            message = (PhtMessage) m.body;
        } else if (m.body instanceof String) {
            init = (String) m.body;
            if (init.equals("init")) {
                System.out.println("PHT init message received");

                message = new PhtMessage(this.node);
                EDSimulator.add(0, message, this.node, this.phtid);
                return;
            }
        } else {
            System.err.println("PHT @@@@@ not a PhtMessage @@@@@"
                    + m.body.getClass().getName());
            return;
        }

        if (message == null) {
            System.err.printf("((nodeId: %d)) message null",
                    this.node.getID());
            return;
        }

        System.out.printf("((nodeId: %d)) received %d from %d\n",
                this.node.getID(), message.getType(), message.getInitiator().getID());

        EDSimulator.add(0, message, this.node, this.phtid);
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Object clone () {
        return new MSPastryListener(prefix);
    }

    public Node getNode() {
        return node;
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
    }
}
