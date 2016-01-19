package peersim.pht.dht.mspastry;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.pastry.MSPastryProtocol;
import peersim.pht.PhtProtocol;
import peersim.pht.statistics.Stats;

public class MSPInit implements Control{
    private final int mspid;
    private final int dhtid;
    private final int lstid;
    private final int phtid;
    private final int bootstrap;

    public MSPInit(String prefix) {
        this.mspid = Configuration.getPid(prefix + ".mspid");
        this.dhtid = Configuration.getPid(prefix + ".dhtid");
        this.lstid = Configuration.getPid(prefix + ".lstid");
        this.phtid = Configuration.getPid(prefix + ".phtid");
        this.bootstrap = Configuration.getInt(prefix + ".bootstrap");
    }

    /**
     * Bug setup for the entire network. Called once.
     * @return false
     */
    @Override
    public boolean execute() {
        MSPastryProtocol msprot;
        MSPastry dht;
        PhtProtocol prot = null;
        MSPastryListener lst;

        Stats.getInstance();

        for (int i = 0; i < Network.size(); i++) {
            Node nd = Network.get(i);

            msprot = ((MSPastryProtocol)nd.getProtocol(this.mspid));
            dht    = (MSPastry)nd.getProtocol(this.dhtid);
            prot   = ((PhtProtocol)nd.getProtocol(this.phtid));
            lst    = ((MSPastryListener)nd.getProtocol(this.lstid));

            lst.setNode(nd);
            msprot.setListener(lst);
            msprot.setMspastryid(this.mspid);
            dht.setMSP(msprot);
            prot.setDht(dht);
            prot.setNode(nd);
            prot.setNodeId(i);
        }

        if (prot != null) {
            prot.setStats();
        }

        PhtProtocol pht;
        pht = (PhtProtocol) Network.get(this.bootstrap).getProtocol(this.phtid);
        pht.sendInit("");

        return false;
    }
}
