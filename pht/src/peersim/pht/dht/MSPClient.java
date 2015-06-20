package peersim.pht.dht;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.pht.*;
import peersim.pht.statistics.Stats;

import java.util.LinkedList;
import java.util.List;

/**
 * MSPClient is a basic client for a simulation using MSPastry.
 * It contains keys and data to insert, search or remove. By default it will
 * start every request from the same Node the bootstrap.
 */
public class MSPClient implements Control, Client {

    private static boolean exe = false;

    private static int next;
    private static LinkedList<PhtData> kdata;
    private static List<String> inserted;
    private static List<String> removed;
    private final PhtProtocol pht;

    private static int nextOp = 0;

    public MSPClient(String prefix) {
        int phtid     = Configuration.getPid(prefix + ".phtid");
        int len       = Configuration.getInt(prefix + ".len");
        int maxKeys   = Configuration.getInt(prefix + ".max");
        int bootstrap = Configuration.getInt(prefix + ".bootstrap");

        List<String> keys = PhtUtil.genKeys(len);

        System.out.println("MSPClient");

        kdata    = new LinkedList<PhtData>();
        next     = 0;
        exe      = true;
        this.pht = (PhtProtocol) Network.get(bootstrap).getProtocol(phtid);
        inserted = new LinkedList<String>();
        removed  = new LinkedList<String>();

        for (int i = 0; (i < maxKeys) && (i < keys.size()); i++) {
            kdata.add( new PhtData( keys.get(i), Integer.parseInt(keys.get(i), 2)) );
        }

        System.out.printf("[MSPClient] kdata size: %d\n", kdata.size());
    }

    /**
     * This method is the where requests are spread into the network.
     * @return true to stop the simulation, false otherwise
     */
    @Override
    public boolean execute() {
        if (! exe) {
            return false;
        }

        PhtData data;

        if (next >= kdata.size()) {
            next = 0;
            nextOp++;
            System.out.printf("[MSPClient] nextOp: %d\n", nextOp);
            PhtUtil.checkTrie(kdata, inserted, removed);
            PhtUtil.allKeys(inserted);

            if (nextOp == 3) {
                Stats st = Stats.getInstance();

                st.curr().start();
                st.printAll();
            }
        }

        data = kdata.get(next);
        System.out.printf("[MSPClient] switch: %d\n", nextOp);
        switch (nextOp) {
            case 0:
                System.out.printf("|| MSPClient || key: '%s'\n", data.getKey());
                if ( this.pht.insertion( data.getKey(), data.getData(), this) >= 0) {
                    lock();
                    System.out.printf("[MSPClient] insertion\n");
                    inserted.add(kdata.get(next).getKey());
                    next++;
                }
                break;

            case 1:
                if (this.pht.query(data.getKey(), this) >= 0) {
                    lock();
                    System.out.printf("[MSPClient] query\n");
                    next++;
                }
                break;

            case 3:
                if (this.pht.suppression(data.getKey(), this) >= 0) {
                    lock();
                    inserted.remove(kdata.get(next).getKey());
                    removed.add(kdata.get(next).getKey());
                    next++;
                }
//                next += kdata.size();
                break;

            case 2:
                if (this.pht.rangeQuery(
                        kdata.get(next).getKey(),
                        kdata.get(kdata.size()-1).getKey(),
                        this) >= 0) {
                    lock();
                    System.out.printf("[MSPClient] rangeQuery '%s' to '%s'\n",
                            kdata.get(next).getKey(),
                            kdata.get(kdata.size()-1).getKey());
                    next += kdata.size() / 4;
                }
                break;

            default:
                System.out.printf("[MSPClient] case4\n");

                Stats st = Stats.getInstance();

                st.newPhase();
                st.curr().start();
                st.printAll();
                return true;
        }

        return false;
    }

    public static void lock() {
        exe = false;
    }

    public static void release() {
        exe = true;
    }

    @Override
    public void responseOk(long requestId, int res) {
    }

    @Override
    public void responseValue(long requestId, String key, Object data) {
        int res = 0;

        if (data instanceof  Integer) {
            res = (Integer) data;
        } else {
            System.out.println("::MSPClient:: responseValue error: "
                    + data.getClass().getName());
        }

        if (res == PhtUtil.keyToData(key)) {
            System.out.println("::MSPClient:: responseValue correct !");
        } else {
            System.out.printf("::MSPClient:: responseValue error: %d != %d\n",
                    res, PhtUtil.keyToData(key));
        }
    }

    @Override
    public void responseList(long requestId, List<PhtData> resp) {
        System.out.printf("::responseList received :\n");
        for (PhtData data: resp) {
            System.out.printf("::responseList:: '%s' : %s\n",
                    data.getKey(), data.getData().toString());
        }
    }

}
