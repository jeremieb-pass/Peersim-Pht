package peersim.pht.dht;

import peersim.config.Configuration;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.pastry.MSPastryProtocol;
import peersim.pht.PhtUtil;
import peersim.pht.messages.PhtMessage;

import java.math.BigInteger;


/**
 * MSPastry is the concrete interface between the peersim.pastry package and
 * PhtProtocol. It's main method is send, others provide some informations.
 */
public class MSPastry implements DhtInterface, EDProtocol {
    private final String prefix;
    private final int mspId;
    private MSPastryProtocol msp;

   public MSPastry (String prefix) {
       this.prefix     = prefix;
       this.mspId      = Configuration.getPid(prefix + ".mspid");
   }

    /**
     * Send a message using MSPastry. The label of the wanted PhtNode is
     * hashed and transformed into a BigInteger
     * @param message The body of the dht's message
     * @param dest Label of the searched PhtNode
     */
    @Override
    public void send(PhtMessage message, String dest) {
        BigInteger recipient;
        byte code[];

        code = PhtUtil.hashMe(dest);
        recipient = new BigInteger(code);
        MSPClient.lock();
        System.out.printf("\n[[%d]] PHT MSPastry::send %s\n\n", this.msp.nodeId, recipient);
//        this.msp.setMspastryid(this.mspId);


        this.msp.send( recipient, message);
    }

    @Override
    public Object getNodeId() {
        return this.msp.nodeId;
    }

    @Override
    public Node getNode() {
        return PhtUtil.nodeIdtoNode(this.msp.nodeId, this.mspId);
    }

    @Override
    public Object clone() {
        return new MSPastry(prefix);
    }

    public void setMSP(MSPastryProtocol msp) {
        this.msp = msp;
    }

    @Override
    public void processEvent(Node node, int pid, Object event) {
    }
}
