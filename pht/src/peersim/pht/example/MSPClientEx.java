package peersim.pht.example;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.pht.Client;
import peersim.pht.ClientInterlocutor;
import peersim.pht.PhtData;
import peersim.pht.PhtUtil;
import peersim.pht.statistics.Stats;

import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 *     MSPClient is a basic client for a simulation using MSPastry.
 *     It contains keys and data to insert, search or remove.
 * </p>
 * <p>
 *     This is just an example: insert random keys, suppress some (all) of
 *     them and make a few range queries and statistics.
 * </p>
 */
public class MSPClientEx implements Control, Client {

    private int nbResponses;
    private boolean statsDone = false;
    private int next;
    private LinkedList<PhtData> kdata;
    private List<String> inserted;
    private List<String> removed;
    private ClientInterlocutor ci;
    private int maxKeys;

    private int nextOp = 0;

    public MSPClientEx(String prefix) {
        int pid         = Configuration.getPid(prefix + ".phtid");
        int len         = Configuration.getInt(prefix + ".len");
        int bootstrap   = Configuration.getInt(prefix + ".bootstrap");
        boolean shuffle = Configuration.getBoolean(prefix + ".shuffle");

        List<String> keys = PhtUtil.genKeys(len, shuffle);

        System.out.println("MSPClientEx " + pid);

        this.next     = 0;
        this.kdata    = new LinkedList<PhtData>();
        this.inserted = new LinkedList<String>();
        this.removed  = new LinkedList<String>();
        this.ci       = ClientInterlocutor.getInstance();
        this.maxKeys  = Configuration.getInt(prefix + ".max");

        this.nbResponses = this.maxKeys;

        this.ci.setClient(this);

        for (int i = 0; (i < maxKeys) && (i < keys.size()); i++) {
            kdata.add( new PhtData( keys.get(i), Integer.parseInt(keys.get(i), 2)) );
        }

        System.out.printf("[MSPClientEx] kdata size: %d\n", kdata.size());
    }

    /**
     * This method is the where requests are spread into the network.
     * @return true to stop the simulation, false otherwise
     */
    @Override
    public boolean execute() {
        PhtData keydata;

        if (CommonState.getPhase() == CommonState.POST_SIMULATION) {
            if (! this.statsDone) {
                simulation();
            }
            return true;
        } else if (this.next == this.kdata.size()) {

            if (this.nbResponses == 0) {
                this.next        = 0;
                this.nbResponses = this.maxKeys;
                this.nextOp++;

                if (this.nextOp == 2) {
                    Stats.getInstance().curr().start();
                    Stats.getInstance().newPhase();

                    List<String> keys = new LinkedList<String>();
                    for (PhtData kd: this.kdata) {
                        keys.add(kd.getKey());
                    }


//                    PhtUtil.checkTrie(this.kdata, keys, new LinkedList<String>());
//                    PhtUtil.allKeys(keys);
                } else if (this.nextOp > 2) {
                    Stats.getInstance().curr().start();
                }
            }

            return false;
        }

        keydata = this.kdata.get(this.next);
        switch (this.nextOp) {
            case 0:
                this.ci.insertion(keydata.getKey(), keydata.getData());
                System.out.printf("[MSPClientEx] insertion: '%s' <> %s\n",
                        keydata.getKey(), keydata.getData());
                break;

            case 1:
                this.ci.query(keydata.getKey());
                System.out.printf("[MSPClientEx] query: '%s'\n",
                        keydata.getKey());
                break;

            case 2:
                this.ci.suppression(keydata.getKey());
                System.out.printf("[MSPClientEx] suppression: '%s'\n",
                        keydata.getKey());
                break;

            default:
//                PhtNode nd = PhtProtocol.findPhtNode("0000111011000");
//                for (String key: nd.getKeys()) {
//                    System.out.printf("'%s' -> '%s'\n",
//                            nd.getLabel(), key);
//                }

                simulation();

                List<String> keys = new LinkedList<String>();
                for (PhtData kd: this.kdata) {
                    keys.add(kd.getKey());
                }
                PhtUtil.checkTrie(this.kdata, keys, new LinkedList<String>());
//                PhtUtil.checkTrie(this.kdata, new LinkedList<String>(), keys);

                return true;
        }

        if (this.next < this.kdata.size()) {
            this.next++;
        }

        return false;
    }

    @Override
    public void responseOk(long requestId, int res) {
        System.out.printf("[MSPClientEx] responseOk <> requestId: %d <> res: %d\n",
                requestId, res);

        this.nbResponses--;
    }

    @Override
    public void responseValue(long requestId, String key, Object data) {
        int res = 0;

        if (data instanceof  Integer) {
            res = (Integer) data;
        } else {
            System.out.println("::MSPClientEx:: responseValue error: "
                    + data.getClass().getName());
        }

        if (res == PhtUtil.keyToData(key)) {
            System.out.printf("::MSPClientEx:: responseValue correct ! ('%s')\n",
                    key);
            this.nbResponses--;
        } else {
            System.out.printf("::MSPClientEx:: responseValue error: %d != %d\n",
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

    @Override
    public void splitOk() {
        this.nbResponses--;
    }

    @Override
    public void mergeOk () {
    }

    private void simulation () {
        Stats st = Stats.getInstance();

        st.curr().start();
        st.printAll();

        this.statsDone = true;
    }
}
