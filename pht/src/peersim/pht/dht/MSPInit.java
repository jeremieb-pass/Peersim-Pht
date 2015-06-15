package peersim.pht.dht;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
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

    @Override
    public boolean execute() {
        MSPastryProtocol msprot;
        MSPastry dht;
        PhtProtocol prot = null;
        MSPastryListener lst;

        Stats.init();

        for (int i = 0; i < Network.size(); i++) {
            msprot = ((MSPastryProtocol)Network.get(i).getProtocol(this.mspid));
            dht    = ((MSPastry)Network.get(i).getProtocol(this.dhtid));
            prot   = ((PhtProtocol)Network.get(i).getProtocol(this.phtid));
            lst    = ((MSPastryListener)Network.get(i).getProtocol(this.lstid));

            lst.setNode(Network.get(i));
            msprot.setListener(lst);
            msprot.setMspastryid(this.mspid);
            dht.setMSP(msprot);
            prot.setDht(dht);
            prot.setNodeId(i);
        }

        if (prot != null) {
            prot.setStats();
        }

        ((PhtProtocol)(Network.get(this.bootstrap).getProtocol(this.phtid))).sendInit("");

        return false;
    }
}
