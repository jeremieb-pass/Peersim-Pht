package peersim.pht.example;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.pht.*;
import peersim.pht.messages.PhtMessage;
import peersim.pht.statistics.Stats;

import java.util.*;

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

    public static final int INSERT = 0;
    public static final int REMOVE = INSERT + 1;
    public static final int QUERY  = REMOVE + 1;
    public static final int RQUERY = QUERY  + 1;

    // End with INSERT/REMOVE/QUERY/RQUERY
    private int endsWith;

    /* ------------------------------- Insert ------------------------------- */

    // List of (key, data) couples for the simulation
    private LinkedList<PhtData> phtData;

    // Inserted (key, data). Useful for dealing with retries.
    private Map<Long, PhtData> inserted;

    // Randomly generated keys
    private List<String> keys;

    /* ------------------------------- Remove ------------------------------- */

    // Removed (key, data). Needed for the verification part.
    private Map<Long, String> removed;

    // Removed keys
    private ArrayList<String> rmKeys;

    /* ---------------------------- Remove/query ---------------------------- */

    // Temporary list of keys
    private Deque<String> tmpDeque;

    // Temporary map of removed/searched keys
    private Map<Long, String> tmpMap;

    /* ---------------------------- Range query ----------------------------- */

    // Store every (key, data) received by a range query
    private Map<Long, List<PhtData>> rQueryMap;

    /* ---------------------------- Information ----------------------------- */

    // Count the number of response received
    private int responseCount;

    // Change operation (insert, remove, query...)
    private int currOp;

    // Maximum number of keys to insert
    private int maxKeys;

    // PhtProtocol layer of the node to communicate with
    private PhtProtocol pht;

    // Prevent from printing the result of the simulation twice.
    private boolean simulation;

    public MSPClientEx(String prefix) {
        // Length of each key (binary value represented as a String)
        int len = Configuration.getInt(prefix + ".len");

        // Bootstrap node to start operation from
        int bootstrap = Configuration.getInt(prefix + ".bootstrap");

        // Shuffle the generated keys ?
        boolean shuffle = Configuration.getBoolean(prefix + ".shuffle");

        // PhtProtocol's id inside PeerSim
        int phtId = Configuration.getPid(prefix + ".phtid");


        endsWith = Configuration.getInt(prefix + ".endsWith");
        maxKeys  = Configuration.getInt(prefix + ".max");
        pht      = (PhtProtocol) Network.get(bootstrap).getProtocol(phtId);

        /* Initialization */

        simulation = false;
        currOp     = 0;
        tmpDeque   = new LinkedList<>();
        phtData    = new LinkedList<>();
        inserted   = new HashMap<>();
        removed    = new HashMap<>();
        tmpMap     = new HashMap<>();
        rQueryMap  = new HashMap<>();
        List<String> genKeys = PhtUtil.genKeys(len, shuffle);
        keys       = new ArrayList<>(maxKeys < genKeys.size() ? maxKeys : genKeys.size());
        for (int i = 0; (i < maxKeys) && (i < genKeys.size()); i++) {
            String key = genKeys.get(i);
            int data   = Integer.parseInt(key, 2);
            phtData.add( new PhtData(key, data) );
            keys.add(key);
            responseCount++;
        }

        // Update the number of keys. Will be used to determine if all the
        // keys have been inserted (for example)
        maxKeys = phtData.size();

        pht.setClient(this);
    }


    /* ---------------------------------------------------------------------- */
    /* ----------------------------- Implements ----------------------------- */
    /* ---------------------------------------------------------------------- */

    /* ------------------------------- Control ------------------------------ */

    /**
     * <p>This is an example.</p>
     * <p>It will only fill in keys and data, and test every operation.</p>
     * @return false, always.
     */
    @Override
    public boolean execute() {
        if ( (CommonState.getPhase() == CommonState.POST_SIMULATION) || (nextOp()) ) {
            simulation();
            return true;
        }

        switch(currOp) {
            case INSERT:
                PhtData data = phtData.peekFirst();
                if (data != null) {
                    long id = pht.insertion(data.getKey(), data.getData());
                    if (id >= 0) {
                        inserted.put(id, data);
                        phtData.removeFirst();
                    }
                    System.out.printf("((MSPClientEx)) {{execute}} (insert) key : %s (%d / %d) <> id: %d",
                            data.getKey(), maxKeys - phtData.size(), maxKeys, id);
                }
                break;

            case REMOVE:
                String key = tmpDeque.peekFirst();
                if (key != null) {
                    System.out.println("((MSPClientEx)) {{execute}} (remove) key : " + key);
                    long id = pht.suppression(key);
                    if (id >= 0) {
                        tmpMap.put(id, key);
                        removed.put(id, key);
                        tmpDeque.removeFirst();
                    }
                }
                break;

            case QUERY:
                key = tmpDeque.peekFirst();
                if (key != null) {
                    System.out.println("((MSPClientEx)) {{execute}} (query) key : " + key + ", rc: " + responseCount);
                    long id = pht.query(key);
                    if (id >= 0) {
                        tmpMap.put(id, key);
                        tmpDeque.removeFirst();
                    }
                }
                break;

            case RQUERY:
                long id;
                String key1 = randomKey();
                String key2 = randomKey();

                responseCount--;
                if (Long.parseLong(key1) < Long.parseLong(key2)) {
                    id = pht.rangeQuery(key1, key2);
                    System.out.printf("((MSPClientEx)) {{execute}} (rquery) keyMin %s, keyMax %s, id %d, rc %d\n",
                            key1, key2, id, responseCount);
                } else {
                    id = pht.rangeQuery(key2, key1);
                    System.out.printf("((MSPClientEx)) {{execute}} (rquery) keyMin %s, keyMax %s, id %d, rc %d\n",
                            key2, key1, id, responseCount);
                }
                break;


            default:
                if (responseCount == 0) {
                    simulation();
                }
                return true;
        }

        return false;
    }

    /* ------------------------------- Client ------------------------------- */

    @Override
    public void responseOk(long requestId, int ok) {
        System.out.println("((MSPClientEx)) {{responseOk}} ok: " + ok + ", rc: " + responseCount);
        if (ok == 0) {
            responseCount--;
        } else if (ok == PhtMessage.RETRY) {
            if (currOp == INSERT) {
                PhtData data = inserted.get(requestId);
                phtData.addFirst(data);
            } else {
                tmpDeque.addLast( tmpMap.get(requestId) );
            }
        }
    }

    @Override
    public void responseValue(long requestId, String key, Object data) {
        if (currOp == QUERY) {
            responseCount--;
            return;
        }

        if (data instanceof Integer) {
            int res = (Integer) data;

            if (res == Integer.parseInt(key, 2)) {
                responseCount--;
                System.out.println("((MSPClientEx)) {{responseValue}} (ok) key : " + key
                        + ", count: " + responseCount);
                return;
            } else {
                tmpDeque.addFirst( tmpMap.get(requestId) );
            }
        }

        System.out.println("((MSPClientEx)) {{responseValue}} (error) key : " + key
                + ", instanceof: " + data.getClass().getName());
    }

    @Override
    public void responseList(long requestId, List<PhtData> resp, boolean end) {
        List<PhtData> tmp = rQueryMap.get(requestId);
        if ( (! resp.isEmpty()) && (tmp != null) ) {
            tmp.addAll(resp);
            rQueryMap.put(requestId, tmp);
        }
    }

    @Override
    public void splitOk() {
        responseCount--;
        System.out.println("((MSPClientEx)) {{splitOk}} rc: " + responseCount);
    }

    @Override
    public void mergeOk() {

    }

    @Override
    public void initOk() {

    }

    /* ---------------------------------------------------------------------- */
    /* -------------------------------- Tools ------------------------------- */
    /* ---------------------------------------------------------------------- */

    /**
     * Increment currOp if no more response left.
     * @return true if the simulation ends now, false otherwise.
     */
    private boolean nextOp() {
        Stats stats = Stats.getInstance();

        if (responseCount == 0) {
            stats.curr().start();
            if (currOp < endsWith) {
                stats.newPhase();
                currOp++;
            } else {
                return true;
            }

            int count = (maxKeys / (2 * currOp)) + 1;
            responseCount = count;
            setRandomKeys(count);
        }

        return false;
    }

    private void simulation () {
        if (! simulation) {
            Stats st = Stats.getInstance();

            st.curr().start();
            st.printAll();

            PhtUtil.checkTrie(keys, rmKeys);
            PhtUtil.allKeys(keys);

            simulation = true;
            pht.flush();
        }
    }

    private int countNbKeysPht() {
        int count = 0;
        List<Map<String, PhtNode>> nds = PhtUtil.getAllNodes();     // all the PhtNodes

        for (Map<String, PhtNode> map: nds) {
            for (PhtNode node : map.values()) {
                if (node.isLeaf()) {
                    count += node.getKeys().size();
                }
            }
        }

        return count;
    }

    /* ------------------------------- Operation ----------------------------- */

    /**
     * Shuffle the randomly generated keys one more time and get the 'number'
     * first elements.
     * @param number Number of keys to return
     */
    private void setRandomKeys(int number) {
        tmpDeque.clear();

        Collection<PhtData> set = inserted.values();
        List<String> notBestWayToDoThis = new LinkedList<>();
        for (PhtData pdata: set) {
            notBestWayToDoThis.add(pdata.getKey());
        }

        if (currOp == REMOVE) {
            rmKeys = new ArrayList<>(number);
        }
        Collections.shuffle(notBestWayToDoThis);
        for (int i = 0; i < number; i++) {
            tmpDeque.add(keys.get(i));

            if (currOp == REMOVE) {
                rmKeys.add(keys.get(i));
            }
        }
    }

    /**
     * Get a random key from the generated List of keys
     * @return binary key as a String
     */
    private String randomKey() {
        Random random = new Random();
        int next = Math.abs(random.nextInt());
        return keys.get( next % keys.size() );
    }
}
