package peersim.pht;

import peersim.pht.messages.PhtMessage;
import peersim.pht.state.PhtNodeState;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>Represents a Pht Node with:</p>
 * <ol>
 *   <li>a) label</li>
 *   <li>b) right and left son (if any)</li>
 *   <li>c) father</li>
 *   <li>d) data: (key, value)</li>
 *   <li>e) leaf (is the node an internal node or a leaf ?)</li>
 *   <li>f) nextLeaf (null if the node is an internal node</li>
 * </ol>
 */
public class PhtNode {
    private boolean  leaf;
    private int      nbKeys;

    /*
     * Father and sons
     */
    private NodeInfo father;
    private NodeInfo rson;
    private NodeInfo lson;

    /*
     * Threaded leaves (used only for the leaf nodes)
     */
    private NodeInfo nextLeaf;
    private NodeInfo prevLeaf;

    private final String label;

    /*
     * A PhtProtocol can have many PhtNode but a PhtNode has one and only
     * one PhtProtocol
     */
    private final PhtProtocol protocol;

    /*
     * (key, value) pairs
     */
    private final Map<String, Object> dkeys;

    /*
     * Statistics
     * Every time this PhtNode is used
     */
    private long usage;

    /*
     * Only when is it used to insert, remove, get a data and/or a key
     * or update a field. Everything except lookups.
     */
    private long usageDest;

    /*
     * Store a PhtMessage in a split or merge operation:
     * the one from the first son who replied, in order to start the next
     * step of the operation once the two sons have replied.
     */
    private PhtMessage message;

    /*
     * A PhtNode has a state. This is used for merges and splits: when a
     * PhtNode is not in a stable state, some operations (e.g. insertion) are
     * not allowed.
     */
    public final PhtNodeState state;

    public PhtNode(String label, PhtProtocol protocol) {
        this.nbKeys     = 0;
        this.label      = label;
        this.lson       = new NodeInfo(this.label + "0");
        this.rson       = new NodeInfo(this.label + "1");
        this.nextLeaf   = new NodeInfo(null);
        this.prevLeaf   = new NodeInfo(null);
        this.dkeys      = new ConcurrentHashMap<String, Object>(PhtProtocol.B, (float)0.75);
        this.state      = new PhtNodeState();
        this.protocol   = protocol;
        this.leaf       = true;

        if (label.equals("")) {
            this.father = new NodeInfo(null);
        } else if (label.length() == 1) {
            this.father = new NodeInfo("");
        } else {
            this.father = new NodeInfo(this.label.substring(0, this.label.length() - 2));
        }
    }


    /* ______________________                           _____________________ */
    /* ______________________ Interface for the hashmap _____________________ */

    /**
     * Normal insert
     *
     * One key, one data, check the number of keys and tell
     * your father you have a new key.
     * @param key Ley to insert
     * @param data PhtData to insert
     * @return Positive value if a split started, 0 if everything went fine,
     * and a negative value otherwise.
     */
    public int insert (String key, Object data) {
        int res;

        try {
            if (this.dkeys.put(key, data) == null) {
                res = 0;
            } else {
                return -1;
            }
        } catch (NullPointerException npe) {
            return -2;
        }

        this.nbKeys++;
        if (this.nbKeys > PhtProtocol.B) {
            split();
            res = 1;
        }

        if (! this.label.equals("")) {
            this.protocol.sendUpdateNbKeys(this.label, this.father, true);
        }

        PhtProtocol.log(String.format("\tPHT NODE (('%s')) insert <> key: '%s'"
                        + ", data: %s, res: %d\n",
                this.label, key, data, res));
        return res;
    }

    /**
     * Insert with no father updates (for split requests)
     *
     * The number of keys is increased
     * @param kdata List of <key, data>
     * @return Everything went fine
     */
    public boolean insert (List<PhtData> kdata) {
        Object res = null;

        try {
            for (PhtData elem : kdata) {
                res = this.dkeys.put(elem.getKey(), elem.getData());
            }
        } catch (NullPointerException npe) {
            return false;
        }

        if (res == null) {
            this.nbKeys += kdata.size();
            return true;
        }

        return false;
    }

    /**
     * Insert with no father updates (for merge requests)
     *
     * This is for split and mergeData operations where there is no new key.
     * @param kdata List of <key, data>
     * @return Everything went fine
     */
    public boolean insertMerge (List<PhtData> kdata) {
        Object res = null;

        try {
            for (PhtData elem : kdata) {
                res = this.dkeys.put(elem.getKey(), elem.getData());
            }
        } catch (NullPointerException npe) {
            return false;
        }

        return res == null;
    }

    /**
     * Update the number of key an internal node has in his subtrees
     * @param i Increment/decrement
     */
    public void updateNbKeys(int i) {
        if (i > 0) {
            this.nbKeys++;
        } else if (i < 0) {
            this.nbKeys--;
        }
    }

    /**
     * Remove the given key from the PhtNode
     * @param key Key to remove with its data
     * @return If everything went fine
     */
    public boolean remove(String key) {
        if (this.dkeys.remove(key) != null) {

            this.nbKeys--;
            if (! this.label.equals("")) {
                this.protocol.sendUpdateNbKeys(this.label, this.father, false);
            }

            PhtProtocol.log(String.format("((PHTNODE)) key '%s' removed\n", key));

            return true;
        }

        PhtProtocol.log(String.format("((PHTNODE)) key '%s' already removed\n", key));
        return false;
    }

    /**
     * Return the result of a get operation to the <key, data> map:
     * if the key does not exist, return null.
     * @param key Key of the object searched
     * @return The object associated with the key
     */
    public Object get(String key) {
        return this.dkeys.get(key);
    }

    /**
     * Access to the keys of this PhtNode
     * @return Set of keys
     */
    public Set<String> getKeys() {
        return this.dkeys.keySet();
    }

    /* ________________________                       _______________________ */
    /* ________________________ Split related methods _______________________ */

    private void split() {
        this.leaf = false;
        if (! this.state.startSplit() ) {
            this.protocol.interrupt();
        }

        this.protocol.sendSplit(this.label, this.lson.getKey());
        this.protocol.sendSplit(this.label, this.rson.getKey());
        this.leaf = false;
    }

    /**
     * List of (key, data) that belongs the the right son.
     * Called during a split process.
     * @return List of PhtData who will go to the right son
     */
    public List<PhtData> splitDataLson() {
        int idx = this.label.length();
        List<PhtData> lson = new LinkedList<PhtData>();

        for (Map.Entry<String, Object> map: this.dkeys.entrySet()) {
            String key   = map.getKey();
            PhtData data = new PhtData(map.getKey(), map.getValue());

            if (idx >= key.length()) {
                idx = key.length()-1;
            }

            if (key.charAt(idx) == '0') {
                lson.add(data);
                this.dkeys.remove(map.getKey());
            }
        }

        return lson;
    }

    /**
     * List of <key, dat> that belongs the the left son.
     * Called during a split process.
     * @return List of PhtData who will go to the right son
     */
    public List<PhtData> splitDataRson() {
        List<PhtData> rson = new LinkedList<PhtData>();

        for (Map.Entry<String, Object> map: this.dkeys.entrySet()) {
            int idx      = this.label.length();
            String key   = map.getKey();
            PhtData data = new PhtData(map.getKey(), map.getValue());

            if (idx >= key.length()) {
                idx = key.length()-1;
            }

            if (key.charAt(idx) == '1') {
                rson.add(data);
                this.dkeys.remove( map.getKey() );
            }
        }

        return rson;
    }

    /**
     * Transform the PhtNode into an internal node.
     */
    public void internal() {
        this.nextLeaf.clear();
        this.prevLeaf.clear();
        this.dkeys.clear();
        this.leaf = false;
    }

    /* ________________________                       _______________________ */
    /* ________________________ Merge related methods _______________________ */

    /**
     * Get the <key, data> of this PhtNode
     * @return All the keys and data of this PhtNode
     */
    public List<PhtData> getDKeys() {
        LinkedList<PhtData> kdata = new LinkedList<PhtData>();

        for (Map.Entry<String, Object> dkey : this.dkeys.entrySet()) {
            kdata.add( new PhtData(dkey.getKey(), dkey.getValue()) );
        }

        return kdata;
    }

    /**
     * Clear the (key, data) map
     */
    public void clear() {
        this.dkeys.clear();
    }

    /**
     * Make this PhtNode a leaf
     */
    public void leaf() {
        this.lson.setNode(null);
        this.rson.setNode(null);
        this.leaf = true;
    }


    /* ____________________                              ____________________ */
    /* ____________________ Store temporary a PhtMessage ____________________ */

    /**
     * Save a message during a merge process
     * @param message Message to be saved
     * @return false if the was already a message stored
     */
    public boolean storeMessage(PhtMessage message) {
        if (this.message != null) {
            return false;
        }
        this.message = message;
        return true;
    }

    /**
     * Return the message stored and set this.message to null
     * @return The message stored by a previous call to storeMessage
     */
    public PhtMessage returnMessage() {
        PhtMessage res = this.message;
        this.message = null;
        return res;
    }

    public boolean canStoreMessage () {
        return this.message == null;
    }

    /* ____________________________              ____________________________ */
    /* ____________________________ Node's state ____________________________ */

    public void use() {
        usage++;
    }

    public void useDest() {
        this.usageDest++;
    }

    /* ___________________________                ___________________________ */
    /* ___________________________ Setter methods ___________________________ */


    public void setNextLeaf(NodeInfo nextLeaf) {
        this.nextLeaf = nextLeaf;
    }

    public void setRson(NodeInfo rson) {
        this.rson = rson;
    }

    public void setLson(NodeInfo lson) {
        this.lson = lson;
    }

    public void setFather(NodeInfo father) {
        this.father = father;
    }

    public void setPrevLeaf(NodeInfo prevLeaf) {
        this.prevLeaf = prevLeaf;
    }

    /* ___________________________                ___________________________ */
    /* ___________________________ Getter methods ___________________________ */

    public String getLabel() {
        return this.label;
    }

    public NodeInfo getNextLeaf() {
        return nextLeaf;
    }

    public NodeInfo getRson() {
        return rson;
    }

    public NodeInfo getLson() {
        return lson;
    }

    public NodeInfo getFather() {
        return father;
    }

    public boolean isLeaf() {
        return leaf;
    }

    public NodeInfo getPrevLeaf() {
        return prevLeaf;
    }

    public int getNbKeys() {
        return this.nbKeys;
    }

    public Set<Map.Entry<String, Object>> getEntrySet() {
        return this.dkeys.entrySet();
    }

    public long getUsage() {
        return usage;
    }

    public long getUsageDest() {
        return this.usageDest;
    }

    @Override
    public String toString() {
        String prev;
        String next;
        StringBuilder sb = new StringBuilder(this.dkeys.size() * PhtProtocol.D);

        prev = (this.prevLeaf.getNode() == null) ? "Nodenull" :
                String.valueOf( this.prevLeaf.getNode().getID() );
        next = (this.nextLeaf.getNode() == null) ? "Nodenull" :
                String.valueOf( this.nextLeaf.getNode().getID() );

        if (this.leaf) {
            sb.append("(prevLeaf: ");
            sb.append(this.prevLeaf.getKey());
            sb.append("[ ").append(prev);
            sb.append("], nextLeaf: ");
            sb.append(this.nextLeaf.getKey());
            sb.append("[ ").append(next).append("]\n");
        }

        for (Map.Entry<String, Object> me: this.dkeys.entrySet()) {
            sb.append("\t\t");
            sb.append(me.getKey());
            sb.append(":");
            sb.append(me.getValue().toString());
            sb.append("\n");
        }

        return sb.toString();
    }


    /* ________________________                   _______________________ */
    /* ________________________ Methods for tests _______________________ */

    public Map<String, Object> getAll() {
        return this.dkeys;
    }
}