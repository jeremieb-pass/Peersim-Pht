package peersim.pht;

import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.pht.dht.DhtInterface;
import peersim.pht.dht.mspastry.MSPClient;
import peersim.pht.dht.mspastry.MSPastryListener;
import peersim.pht.exceptions.*;
//import peersim.pht.tests.EDSimulator;
import peersim.edsim.EDSimulator;
import peersim.pht.messages.*;
import peersim.pht.statistics.Stats;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PhtProtocol is the core of this package.
 *
 * A PhtProtocol contains a list of PhtNodes, has a state indicating what kind
 * of requests he is waiting for, a request id counter, the id of PhtProtocol
 * in the simulation, and data for insertion operations.
 *
 * PhtProtocol sends messages (PhtMessage) and process events, and is the
 * class who communicates with clients: a client asks for an insertion, a
 * suppression, a lookup, or a range query operation, PhtProtocol starts
 * everything and when it can, it responds to the client (the data searched,
 * the result of the operation).
 */
public class PhtProtocol implements EDProtocol {

    /*
     * Protocol constants
     * D: Size of a key in bits
     * B: Maximum number of keys that a leaf should handle
     * MAX: Maximum number of clients with pending requests
     */
    public static int D;
    public static int B;
    public static int MAX;

    private static final int PHT_INIT        = 0;
    private static final int PHT_INSERTION1  = 1;
    private static final int PHT_INSERTION2  = 2;
    private static final int PHT_SUPPRESSION = 3;
    private static final int PHT_LOOKUP      = 4;
    private static final int PHT_RANGE_QUERY = 5;

    private long rqCount = 0;
    private long rqTotal = 0;

    private static String prefix;

    /* __________ Log fields __________ */

    private static boolean logOk;
    private static int logCount;
    private static StringBuffer logBuf;
    private static FileWriter logFile;

    private static BufferedWriter logWriter;
    private static FileWriter fw;

    /* __________ Statistics __________ */

    // Global statistics
    private static Stats stats;

    // Every time this Node (the machine in the network) is beein used
    private long usage;

    // Every time this Node is the destination for an operation
    private long usageDest;

    /* The dht provides the basic operation that we need */
    private DhtInterface dht;

    /* __________ Tests __________ */

    private static boolean init = false;
    private static long nextId  = 0;
    
    /* __________ Delay for messages __________ */
    
    private static short MAX_DELAY;
    private static short delay = 0;

    /* __________ General information __________ */

    private static int phtid;
    private long nid;

    private static int currentLookup;
    private static int currentRangeQuery;

    private Node node;
    private int state;
    private Object currentData;
    private final ConcurrentHashMap<String, PhtNode> nodes;

    /* Map a request id with a client */
    private final Map<Long, Client> clients;

    public PhtProtocol(String prefix) throws IOException {
        String logValue = Configuration.getString(prefix + ".log");
        int dhtid = Configuration.getPid(prefix + "." + "dht");

        PhtProtocol.prefix = prefix;

        PhtProtocol.B = Configuration.getInt(prefix + "." + "B");
        PhtProtocol.D = Configuration.getInt(prefix + "." + "D");
        
        // Maximum delay for messages
        MAX_DELAY = (short) Configuration.getInt(prefix + ".maxdelay");

        // Lookup
        currentLookup = PhtMessage.LIN_LOOKUP;

        // Range query
        if (Configuration.getString(prefix + ".rq").equals("seq")) {
            currentRangeQuery = PhtMessage.SEQ_QUERY;
        } else {
            currentRangeQuery = PhtMessage.PAR_QUERY;
        }

        this.state      = PHT_INIT;
        phtid           = dhtid + 1;
        this.nodes      = new ConcurrentHashMap<String, PhtNode>();
        this.clients    = new TreeMap<Long, Client>();

        // Logs
        logOk = logValue.equals("on");
        if (logOk) {
            File old  = new File("phtprotocol.log.old");
            File path = new File("phtprotocol.log");

            if ( path.exists() ) {
                path.renameTo(old);
            }

            fw = new FileWriter("phtprotocol.log", false);
            logWriter = new BufferedWriter(fw);
        }
    }

    /* _________________________________     ________________________________ */
    /* _________________________________ API ________________________________ */

    /**
     * Store the data to be inserted, change the state of the PhtProtocol to
     * INSERTION1 and starts a lookup.
     * @param key The key of the data we want to insert
     * @param data PhtData to be inserted
     * @param client Client to who PhtProtocol will respond
     * @return The Id of the request
     */
    public long insertion(String key, Object data, Client client) {
        if (this.state != PHT_INIT) {
            return -1;
        } else if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
        this.clients.put(nextId, client);
        this.currentData = data;
        this.state       = PHT_INSERTION1;

        log(String.format("((%d)) insertion (%s, %s)    [%d]\n",
                nextId, key, data, this.node.getID()));

        // Statistics
        stats.curr().incInsert();

        query(key, PhtMessage.INSERTION, nextId);

        return nextId;
    }

    /**
     * Send a suppression request to remove the data associated with the given
     * key from the network
     * @param key Key of the data to remove
     * @param client Client to who PhtProtocol will respond
     * @return The Id of the request
     */
    public long suppression(String key, Client client) {
        if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
        this.clients.put(nextId, client);
        this.state = PHT_SUPPRESSION;

        log( String.format("((%d)) suppression (%s)    [%d]\n",
                nextId, key, this.node.getID()) );

        // Statistics
        stats.curr().incDelete();

        query(key, PhtMessage.SUPRESSION, nextId);

        return nextId;
    }

    /**
     * Launch an exact search for the given key
     * @param key Key to search
     * @param client Client to who respond
     * @return The Id of the request
     */
    public long query(String key, Client client) {
        if (! PhtProtocol.init) {
            return -1;
        } else if (this.state != PHT_INIT) {
            return -1;
        }

        nextId++;
        this.clients.put(nextId, client);
        this.state = PHT_LOOKUP;

        log( String.format("((%d)) query (%s)    [%d]\n",
                nextId, key, this.node.getID()) );

        // Statistics
        stats.curr().incClientLookup(currentLookup);

        query(key, currentLookup, nextId);

        return nextId;
    }

    /**
     * Start a rangeQuery for keys between keyMin and keyMax
     * @param keyMin Lower bound of the range
     * @param keyMax Upper bound of the range
     * @param client Client to respond to
     * @return The Id of the query
     */
    public long rangeQuery(String keyMin, String keyMax, Client client) {
        if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
        this.state   = PHT_RANGE_QUERY;
        this.rqTotal = 0;
        this.rqCount = 0;
        this.clients.put(nextId, client);

        // Statistics
        stats.curr().incClientRangeQuery(currentRangeQuery);

        rangeQuery(keyMin, keyMax, nextId);

        return nextId;
    }

    /* ______________________________         _______________________________ */
    /* ______________________________ Lookup  _______________________________ */

    /**
     * Switch between linear and binary lookup, depending on the current policy.
     * @param key Key to search
     * @param operation If there something to do after the lookup
     * @param id Id of the request
     */
    private void query (String key, int operation, long id) {
        String startKey;
        PhtMessage message;
        PMLookup pml;

        if (currentLookup == PhtMessage.LIN_LOOKUP) {
            startKey = "";
        } else {
            startKey = key.substring(0, PhtProtocol.D/2);
        }

        System.out.println("::query::");

        pml = new PMLookup(key, operation, null, startKey);
        message = new PhtMessage(currentLookup, this.node, null, id, pml);

        dht.send(message, startKey);
    }

    /* ___________________________               ____________________________ */
    /* ___________________________ Range queries ____________________________ */

    /**
     * Switch between sequential and parallel range queries, depending on the
     * policy.
     * @param keyMin Minimum key of the range
     * @param keyMax Maximum key of the range
     */
    private void rangeQuery(String keyMin, String keyMax, long id) {
        String startLookup = "";
        String startKey;
        PhtMessage message;
        PMLookup pml;
        PMRangeQuery pmrq;

        if (currentRangeQuery == PhtMessage.SEQ_QUERY) {
            startKey = keyMin;
        } else {
            startKey = PhtUtil.smallestCommonPrefix(keyMin, keyMax);
        }

        if (currentLookup == PhtMessage.LIN_LOOKUP) {
            startLookup = "";
        }
        System.out.printf("::rquery:: startKey: '%s' <> startLookup: '%s'\n",
                startKey, startLookup);

        pmrq    = new PMRangeQuery(keyMin, keyMax);
        pml     = new PMLookup(startKey, currentRangeQuery, null, startLookup, pmrq);
        message = new PhtMessage(currentLookup, this.node, null, id, pml);

        System.out.println("::rquery:: this node id is " + this.node.getID());

        dht.send(message, startLookup);
    }

    /* _______________________________          _____________________________ */
    /* _______________________________ Initiate _____________________________ */

    public void sendInit(String recipient) {
        PhtMessage message;

        message = new PhtMessage(PhtMessage.INIT, this.node, "", 0, null);
        this.dht.send(message, recipient);
    }

    /* _____________________                              ___________________ */
    /* _____________________ Message demands from PhtNode ___________________ */

    /**
     * Send a split request to the node (peersim) responsible for the 'son' key
     * @param label Father's label
     * @param son Son's label
     */
    public void sendSplit(String label, String son) {
        PhtMessage message;
        PMLookup pml;

        nextId++;
        pml = new PMLookup(label, PhtMessage.SPLIT, null, son);
        message = new PhtMessage(PhtMessage.SPLIT, this.node, label, nextId, pml);

        log( String.format("((%d)) sendSplit [initiator: '%s' on node %d][son: '%s']\n",
                message.getId(), label, this.node.getID(), son) );

        /*
         * Statistics
         *
         * For each split, this method is called twice. So just count the call
         * for the right son
         */
        if (son.endsWith("1")) {
            stats.curr().incSplit();
        }

        dht.send(message, son);
    }

    /**
     * Send a merge message to the node (peersim) responsible for the 'son' key
     * @param label Father's label
     * @param son Son's label
     */
    private void sendMerge(String label, NodeInfo son) {
        PhtMessage message;
        PMLookup pml;

        nextId++;
        pml = new PMLookup(label, PhtMessage.MERGE, null, son.getKey());
        message = new PhtMessage(PhtMessage.MERGE, this.node, label, nextId, pml);

        log(String.format("((%d)) sendMerge from '%s'(%d) to '%s'(%d)\n",
                nextId, label, this.node.getID(), son.getKey(), son.getNode().getID()));

        /*
         * Statistics
         *
         * For each merge, this method is called twice. So just count the call
         * for the right son
         */
        if (son.getKey().endsWith("1")) {
            stats.curr().incMerge();
        }

        
        EDSimulator.add(delay(), message, son.getNode(), phtid);
    }

    /**
     * Send a message to a PhtNode's father telling to increment or decrement
     * (depending on the value of inc) the number of keys his has in his
     * subtrees. This method is a called after an insertion or a suppression
     * @param label Label of the son
     * @param father Label of the father
     * @param inc Increment or Decrement
     */
    public void sendUpdateNbKeys(String label, NodeInfo father, boolean inc) {
        int type;
        PhtMessage message;
        PMLookup pml;

        if (inc) {
            type = PhtMessage.UPDATE_NBKEYS_PLUS;
        } else {
            type = PhtMessage.UPDATE_NBKEYS_MINUS;
        }

        nextId++;
        pml     = new PMLookup(label, type, null, father.getKey(), true);
        message = new PhtMessage(type, this.node, label, nextId, pml);

        if (father.getNode() == null) {
            log("@@@@@ father null in sendMerge @@@@@");
        }

        
        EDSimulator.add(delay(), message, father.getNode(), phtid);
    }

    /**
     * Inform the initiator that he must try again this operation.
     * @param initiator Initiator of the operation
     * @param id Message id
     * @param label Label of the PhtNode who called this method
     */
    private void sendRetry (Node initiator, long id, String label) {
        PhtMessage message;

        message = new PhtMessage(PhtMessage.RETRY, this.node, label, id, null);
        
        EDSimulator.add(delay(), message, initiator, phtid);
    }

    /* __________________________                    _________________________ */
    /* __________________________ Internal behaviour _________________________ */


    /* __________________________                   _________________________ */
    /* __________________________ Message reception _________________________ */

    /**
     * If the node responsible for the key is found: ACK.
     * @param message Query message
     * @throws PhtNodeNotFoundException
     */
    private void processLinLookup (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        NodeInfo next;
        PhtNode node;

        // Get the node
        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            checkNetwork();
            throw new PhtNodeNotFoundException("processLinLookup '"
                    + pml.getDestLabel()
                    + "' <> this node ID is "
                    + this.node.getID());
        }
        node.use();

        if (pml.getOperation() == PhtMessage.PAR_QUERY) {
            if ( (node.isLeaf()) || (node.getLabel().equals(pml.getKey())) ) {
                processParQuery(message, pml);
                return;
            }
        }

        // Continue the lookup is the node is not a leaf
        if (! node.isLeaf()) {
            log( String.format("((%d)) <noLeaf> processLinLookup :: node %d" +
                            " :: node's label: '%s' :: key: '%s' :: op: %d\n",
                    message.getId(), this.node.getID(),
                    node.getLabel(), pml.getKey(), pml.getOperation()) );

            next = forwardLookup(node, pml.getKey());
            pml.setDestLabel(next.getKey());
            pml.setDest(next.getNode());
            System.out.printf("::linquery next is '%s' on node %d::\n",
                    next.getKey(), next.getNode().getID());

            
            EDSimulator.add(delay(), message, pml.getDest(), phtid);
            return;
        }

        if (pml.getOperation() > PhtMessage.LIN_LOOKUP) {
            System.out.printf("::linquery:: type: %d <> op: %d <> label: '%s' <> key: '%s'\n",
                    message.getType(), pml.getOperation(),
                    node.getLabel(), pml.getKey());
        }

        System.out.printf("::linquery '%s' on node %d::\n",
                pml.getDestLabel(), this.node.getID());

        log(String.format("((%d)) <Leaf> processLinLookup :: node %d" +
                        " :: node's label: '%s' :: key: '%s' :: op: %d\n",
                message.getId(), this.node.getID(),
                node.getLabel(), pml.getKey(), pml.getOperation()));

        // If it is a leaf, the action depends on the underlying operation
        switch (pml.getOperation()) {
            case PhtMessage.SUPRESSION:

                /*
                 * If the PhtNode is not in a stable state: send a retry
                 * message to the initiator.
                 */
                if (! node.state.isStable()) {
                    MSPClient.retry( message.getId() );
//                    return;
                }

                message.setMore(node.remove(pml.getKey()));
                message.setType( PhtMessage.ACK_SUPRESSION );
                break;

            case PhtMessage.LIN_LOOKUP:
                pml.setLess( node.get(pml.getKey()) );
                message.setType( PhtMessage.ACK_LIN_LOOKUP );
                break;

            case PhtMessage.INSERTION:

                /*
                 * If the PhtNode is not in a stable state: send a retry
                 * message to the initiator.
                 */
                if (! node.state.isStable()) {
                    MSPClient.retry( message.getId() );
//                    return;
                }
                
                message.setType(PhtMessage.ACK_LIN_LOOKUP);
                break;

            case PhtMessage.SEQ_QUERY:
                processSeqQuery(message, pml);
                return;
        }
        node.useDest();
        this.usageDest++;

        pml.setDest(this.node);
        
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }


    /**
     * Start a sequential query from the PhtNode who is a prefix of keyMin
     * @param message PhtMessage with all the information needed
     * @param pml PMLookup previously extracted from message
     * @throws PhtNodeNotFoundException
     */
    private void processSeqQuery (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        PhtMessage forward;
        PMLookup pmlForward;
        PMRangeQuery pmrqForward;
        PMRangeQuery pmrq;
        PhtNode node;

        node = this.nodes.get( pml.getDestLabel() );
        if (node == null) {
            throw new PhtNodeNotFoundException("processSeqQuery");
        }

        node.use();
        node.useDest();
        this.usageDest++;

        if (pml.getLess() instanceof PMRangeQuery) {
            pmrq = (PMRangeQuery) pml.getLess();
        } else {
            return;
        }

        // One more node on the range query
        pmrq.oneMore();

        log(String.format("((%d)) processSeqQuery \n"
                        + "<> keyMin: %s <> keyMax: %s\n",
                message.getId(),
                pml.getKey(), pmrq.getKeyMax()));

        // Stop the range query here ?
        if (node.getNextLeaf().getNode() != null) {
            if (! PhtUtil.inRangeMax(node.getNextLeaf().getKey(), pmrq.getKeyMax()) ) {
                pmrq.stop();
            }
        } else {
            System.out.printf("::processSeqQuery:: end '%s' to '%s'\n",
                    pmrq.getKeyMin(), pmrq.getKeyMax());
            pmrq.stop();
        }

        // First: get everything
        if (pmrq.getCount() == 0) {
            pmrq.addSupTo(node.getDKeys(), Integer.parseInt(pml.getKey(), 2));
        } else if (pmrq.isEnd()) {
            pmrq.addInfTo(node.getDKeys(), Integer.parseInt(pmrq.getKeyMax(), 2));
        } else {
            pmrq.add(node.getDKeys());
        }

        // Send the keys and data to the initiator of the request
        message.setType(PhtMessage.ACK_SEQ_QUERY);
        
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);

        if (pmrq.isEnd()) {
            System.out.printf("::processSeqQuery:: end '%s' to '%s'\n",
                    pmrq.getKeyMin(), pmrq.getKeyMax());

            // Statistics
            stats.curr().addSeqQuery(message, pmrq);
            return;
        }

        // Other PhtMessage
        pmrqForward = new PMRangeQuery(
                pmrq.getKeyMin(),
                pmrq.getKeyMax(),
                pmrq.getCount()
        );

        pmlForward = new PMLookup(
                pml.getKey(),
                pml.getOperation(),
                pml.getDest(),
                pml.getDestLabel(),
                pmrqForward
        );

        forward = new PhtMessage(
                PhtMessage.SEQ_QUERY,
                message.getInitiator(),
                message.getInitiatorLabel(),
                message.getId(),
                pmlForward
        );

        // Forward the request to the next Leaf
        pmlForward.setDestLabel(node.getNextLeaf().getKey());
        forward.setType(pmlForward.getOperation());
        
        EDSimulator.add(delay(), forward, node.getNextLeaf().getNode(), phtid);
    }


    /**
     * The method is called when the smallest prefix node has been found (or
     * if we have reached a leaf in the lookup).
     * The first step step is to flood the subtrees if the node is internal.
     * Then, when a leaf is reached, send data and keys to the initiator
     * only if the node's prefix is in the range, and send an ACK to the
     * father.
     * @param message Message with information on the initiator node
     * @param pml PMLookup extracted from the PhtMessage, it contains a
     *            PMRangeQuery.
     * @throws PhtNodeNotFoundException
     */
    private void processParQuery (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        NodeInfo next;
        PhtMessage ack;
        PMLookup pmlAck;
        PMRangeQuery pmrq;
        PhtNode node;
        PhtMessage forward;
        PMLookup forwardPml;
        PMRangeQuery forwardPmrq;

        node = this.nodes.get( pml.getDestLabel() );
        if (node == null) {
            throw new PhtNodeNotFoundException("processSeqQuery");
        }

        node.use();
        if (pml.getLess() instanceof PMRangeQuery) {
            pmrq = (PMRangeQuery) pml.getLess();
        } else {
            return;
        }

        // Continue the lookup is the node is not a leaf
        if (! node.isLeaf()) {
            log(String.format("((%d)) <noLeaf> processParQuery :: node %d" +
                            " :: node's label: '%s' :: key: '%s' :: op: %d\n",
                    message.getId(), this.node.getID(),
                    node.getLabel(), pml.getKey(), pml.getOperation()));


            // Forward to the left son
            pml.setDestLabel(node.getLson().getKey());
            pml.setDest(node.getLson().getNode());
            message.setType(PhtMessage.PAR_QUERY);

            
            EDSimulator.add(delay(), message, pml.getDest(), phtid);

            // Forward to the right son (copy the messages)
            forwardPmrq = new PMRangeQuery(
                    pmrq.getKeyMin(),
                    pmrq.getKeyMax()
            );

            forwardPml = new PMLookup(
                    pml.getKey(),
                    pml.getOperation(),
                    node.getRson().getNode(),
                    node.getRson().getKey(),
                    forwardPmrq
            );

            forward = new PhtMessage(
                    PhtMessage.PAR_QUERY,
                    message.getInitiator(),
                    message.getInitiatorLabel(),
                    message.getId(),
                    forwardPml
            );

            
            EDSimulator.add(delay(), forward, forwardPml.getDest(), phtid);
            return;
        }

        // If this leaf is outside the range stop here
        if ( (PhtUtil.inRangeMax(node.getLabel(), pmrq.getKeyMax()))
                && (PhtUtil.inRangeMin(node.getLabel(), pmrq.getKeyMin())) ) {
            System.out.println("test :: outside ::");

            // First: get everything
            pmrq.addRange(
                    node.getDKeys(),
                    Integer.parseInt(pmrq.getKeyMin(), 2),
                    Integer.parseInt(pmrq.getKeyMax(), 2)
            );
        }

        // Send the keys and data to the initiator of the request
        message.setType(PhtMessage.ACK_PAR_QUERY_CLIENT);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
        

        node.useDest();
        this.usageDest++;
        if (node.getLabel().equals("")) {
            next = new NodeInfo("", this.node);
        } else {
            next = new NodeInfo(node.getFather().getKey(), node.getFather().getNode());
        }

        // Other PhtMessage
        pmlAck = new PMLookup(
                pml.getKey(),
                pml.getOperation(),
                next.getNode(),
                next.getKey(),
                1
        );

        ack = new PhtMessage(
                PhtMessage.ACK_PAR_QUERY,
                message.getInitiator(),
                message.getInitiatorLabel(),
                message.getId(),
                pmlAck
        );

        
        if (node.getLabel().equals("")) {
            EDSimulator.add(delay(), ack, this.node, phtid);
        } else {
            EDSimulator.add(delay(), ack, node.getFather().getNode(), phtid);
        }
    }


    /**
     * Insert the data into the right leaf.
     * @param message Message containing the key and the data.
     * @throws NoPMLookupException
     * @throws PhtNodeNotFoundException
     */
    private void processInsertion (PhtMessage message)
            throws NoPMLookupException, PhtNodeNotFoundException {
        int res;
        PhtNode node;
        PMLookup pml;

        pml  = checkLookup(message, "processInsertion");
        node = this.nodes.get(pml.getDestLabel());
        for (PhtNode nd: this.nodes.values()) {
            if (nd.getLabel().equals(pml.getDestLabel())) {
                node = nd;
                break;
            }
        }

        System.out.println("::insertion::");

        log(String.format("((%d)) processInsertion [initiator: '%s'][type: %d] "
                        + "[op: %d][key: '%s']    [%d]\n",
                message.getId(), message.getInitiator().getID(), message.getType(),
                pml.getOperation(), pml.getKey(), this.node.getID()));

        if (node == null) {
            throw new PhtNodeNotFoundException("processInsertion");
        }
        node.use();
        node.useDest();
        this.usageDest++;

        res = node.insert(pml.getKey(), pml.getLess());

        if (message.getInitiator() == null) {
            log("@@@@@ initiator null in processInsertion @@@@@");
        }

        message.setType(PhtMessage.ACK_INSERTION);
        message.setMore(res);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }


    /* :::::::::: SPLIT ::::::::: */

    /**
     * Method called by processLinLookup because we do not search for a
     * PhtNode, but we want to create a new one.
     * @param message Message with the father's node (peersim)
     * @param pml The son's label
     */
    private void processSplit(PhtMessage message, PMLookup pml) throws CantSplitException {
        String label;
        PhtNode node;
        NodeInfo ni;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);

        /* If the node already exists, there is a problem */
        if (node != null) {
            throw new CantSplitException("processSplit <> '" + node.getLabel() + "' ");
        }

        node = new PhtNode(label, this);
        ni   = new NodeInfo(message.getInitiatorLabel(), message.getInitiator());

        node.setFather(ni);
        this.nodes.put(label, node);

        node.use();
        node.useDest();
        this.usageDest++;

        log(String.format("((%d)) processSplit [initiator: '%s' <> '%s'][type: %d] "
                        + "[label: '%s']    [%d]\n",
                message.getId(),
                message.getInitiator().getID(), message.getInitiatorLabel(),
                message.getType(),
                node.getLabel(), this.node.getID()));

        /*
         * A split operation needs more than just two messages, so to enable
         * direct communications, we update the dest field of PMLookup (the
         * son).
         */
        pml.setDest(this.node);
        pml.setLess(true);
        message.setType(PhtMessage.ACK_SPLIT);


        if (message.getInitiator() == null) {
            log("@@@@@ initiator null in processSplit @@@@@");
        }

        
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /**
     * Get new previous and next leaf. Ack to the father to get data.
     * @param message Message with next and previous leaves
     * @throws NoPMLookupException
     * @throws PhtNodeNotFoundException
     * @throws NoDataSplitData
     */
    private void processSplitLeaves(PhtMessage message)
            throws NoPMLookupException,
            PhtNodeNotFoundException,
            NoDataSplitData {
        String label;
        PhtNode node = null;
        PMLookup pml;
        List<NodeInfo> info;

        pml   = checkLookup(message, "processSplitLeaves");
        label = pml.getDestLabel();

        for (PhtNode nd: this.nodes.values()) {
            if (nd.getLabel().equals(label)) {
                node = nd;
                break;
            }
        }

        if (node == null) {
            throw new PhtNodeNotFoundException("processSplitLeaves <> " + label);
        }

        node.use();
        node.useDest();
        this.usageDest++;

        if (pml.getLess() instanceof List) {

            // Retrieve the data an pass it to the son
            info = (List<NodeInfo>) pml.getLess();

            node.setPrevLeaf(info.get(0));
            node.setNextLeaf(info.get(1));

            log(String.format("((%d)) processSplitLeaves [initiator: '%s' <> '%s'][type: %d] "
                            + "[label: '%s'][leaves received: %d]    [%d]\n",
                    message.getId(),
                    message.getInitiator().getID(), message.getInitiatorLabel(),
                    message.getType(),
                    node.getLabel(), info.size(), this.node.getID()));

        } else {
            throw new NoDataSplitData("processSplitLeaves <> "
                    + pml.getLess().getClass().getName()
                    + " <> pml.getDestLabel: '" + pml.getDestLabel()
                    + "' <> pml.getKey: '" + pml.getKey()
                    + "' <> message.getInitiatorLabel: '" + message.getInitiatorLabel() + "'");
        }

        pml.setLess(true);
        message.setType(PhtMessage.ACK_SPLIT_LEAVES);


        if (message.getInitiator() == null) {
            log("@@@@@ initiator null in processSplitLeaves @@@@@");
        }

        
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /**
     * Retrieve the data from the message and insert it into the son
     * @param message Message with all the information
     * @throws NoPMLookupException
     * @throws PhtNodeNotFoundException
     * @throws NoDataSplitData
     */
    private void processSplitData(PhtMessage message)
            throws NoPMLookupException,
            PhtNodeNotFoundException,
            NoDataSplitData {
        String label;
        PhtNode node;
        PMLookup pml;
        List<PhtData> data;

        pml   = checkLookup(message, "processSplitData");
        label = pml.getDestLabel();
        node  = this.nodes.get(label);
        for (PhtNode nd: this.nodes.values()) {
            if (nd.getLabel().equals(label)) {
                node = nd;
                break;
            }
        }

        if (node == null) {
            throw new PhtNodeNotFoundException("processSplitData <> " + label);
        }

        node.use();
        node.useDest();
        this.usageDest++;

        if (pml.getLess() instanceof List) {

            /* Retrieve the data an pass it to the son */
            data = (List<PhtData>) pml.getLess();
            pml.setLess(node.insert(data));

            log(String.format("((%d)) processSplitData [initiator: %d <> '%s'][type: %d] "
                            + "[destLabel: '%s'][keys received: %d]    [%d]\n",
                    message.getId(),
                    message.getInitiator().getID(), message.getInitiatorLabel(),
                    message.getType(),
                    label, data.size(), this.node.getID()));
        } else {
            throw new NoDataSplitData("processSplitData <> "
                    + pml.getLess().getClass().getName()
                    + " <> pml.getDestLabel: '" + pml.getDestLabel()
                    + "' <> pml.getKey: '" + pml.getKey()
                    + "' <> message.getInitiatorLabel: '" + message.getInitiatorLabel() + "'");
        }

        message.setType(PhtMessage.ACK_SPLIT_DATA);


        if (message.getInitiator() == null) {
            log("@@@@@ initiator null in processSplitData @@@@@");
        }

        this.state = PHT_INIT;
        
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);

        /*
         * Tell the previous leaf I am his new next leaf, and my next leaf that
         * I am his new previous leaf
         */

        PhtMessage update;
        PMLookup pmlUpdate;

        if (node.getPrevLeaf().getNode() != null) {
            pmlUpdate = new PMLookup(
                    pml.getKey(),
                    pml.getOperation(),
                    node.getPrevLeaf().getNode(),
                    node.getPrevLeaf().getKey(),
                    pml.getLess());
            update = new PhtMessage(
                    message.getType(),
                    this.node,
                    node.getLabel(),
                    message.getId(),
                    pmlUpdate);

            pmlUpdate.setLess( new NodeInfo(node.getLabel(), this.node) );
            update.setType(PhtMessage.UPDATE_NEXT_LEAF);
            EDSimulator.add(delay(), update, node.getPrevLeaf().getNode(), phtid);
            
        }
        if (node.getNextLeaf().getNode() != null) {
            pmlUpdate = new PMLookup(
                    pml.getKey(),
                    pml.getOperation(),
                    node.getNextLeaf().getNode(),
                    node.getNextLeaf().getKey(),
                    pml.getLess());
            update = new PhtMessage(
                    message.getType(),
                    this.node,
                    node.getLabel(),
                    message.getId(),
                    pmlUpdate);

            pmlUpdate.setLess(new NodeInfo(node.getLabel(), this.node));
            update.setType(PhtMessage.UPDATE_PREV_LEAF);
            EDSimulator.add(delay(), update, node.getNextLeaf().getNode(), phtid);
            
        }
    }


    /* :::::::::: MERGE ::::::::: */

    /**
     * Send a son's keys and data to his father.
     * This is the first step of a merge process (for to son).
     * @param message Message with all the information needed
     * @param pml PMLookup that was inside that message that has already been
     *            extracted in the processEvent method
     * @throws CantSplitException
     */
    private void processMerge(PhtMessage message, PMLookup pml) throws CantSplitException {
        String label;
        PhtNode node;

        label = pml.getDestLabel();

        node = this.nodes.get(label);

        // If the node does not exists, there is a problem
        if (node == null) {
            throw new CantSplitException("processMerge node null\n"
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey()
                    + " <> father's label: '" + message.getInitiatorLabel() + "'\n"
                    + " <> father's label: '" + pml.getKey()
                    + " <> father's node: " + message.getInitiator().getID() + "\n"
                    + " <> this nodes is: " + this.node.getID()
                    + " <> type: " + PhtMessage.SPLIT_DATA + "\n\n"
                    + " <>\nsize:" + this.nodes.size()
                    + "\nvalues: " + this.nodes.values()
                    + "\n" + toString());
        }

        log(String.format("((%d)) processMerge [node: '%s'][initiator: '%s' on %d]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID()));

        if (node.isLeaf()) {
            pml.setLess(true);
        } else {
            pml.setLess(false);
        }

        /*
         * A merge operation needs more than just two messages, so to enable
         * direct communications, we update the dest field of PMLookup (the
         * son).
         */
        pml.setDest(this.node);
        message.setType(PhtMessage.ACK_MERGE);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
        
    }


    /**
     * Send a son's next (if it is a right son) or previous (if it is a left
     * son) to his father. Second step of a merge process (for the son).
     * @param message Request message
     * @param pml PMLookup that was inside that message that has already been
     *            extracted in the processEvent method
     * @throws CantSplitException
     */
    private void processMergeLeaves(PhtMessage message, PMLookup pml) throws CantSplitException {
        String label;
        PhtNode node;
        NodeInfo leaf;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);

        /* If the PhtNode does not exist, there is a problem */
        if (node == null) {
            throw new CantSplitException("processMergeLeaves <> '" + label + "' ");
        }

        if (label.charAt(label.length() - 1) == '0') {
            leaf = new NodeInfo(node.getPrevLeaf().getKey(), node.getPrevLeaf().getNode());
        } else {
            leaf = new NodeInfo(node.getNextLeaf().getKey(), node.getNextLeaf().getNode());
        }

        log(String.format("((%d)) processMergeLeaves [node: '%s'][initiator: '%s' on %d]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID()));

        /*
         * A merge operation needs more than just two messages, so to enable
         * direct communications, we update the dest field of PMLookup (the
         * son).
         */
        pml.setLess(leaf);
        message.setType(PhtMessage.ACK_MERGE_LEAVES);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
        
    }

    private void processMergeData(PhtMessage message, PMLookup pml) throws CantSplitException {
        String label;
        PhtNode node;
        List<PhtData> kdata;

        label = pml.getDestLabel();

        node = this.nodes.get(label);

        /* If the PhtNode does not exist, there is a problem */
        if (node == null) {
            throw new CantSplitException("processMergeData <> '" + label + "' ");
        }

        kdata = node.getDKeys();

        log(String.format("((%d)) processMergeData [node: '%s'][initiator: '%s' on %d]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID()));

        /*
         * A merge operation needs more than just two messages, so to enable
         * direct communications, we update the dest field of PMLookup (the
         * son).
         */
//        pml.setDest(this.node);
        pml.setLess(kdata);
        message.setType(PhtMessage.ACK_MERGE_DATA);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
        
    }


    /**
     * Last step of a merge process (for the son).
     * The son has sent his keys, data, and next/previous leaf, he can now be
     * removed.
     * @param message Request message
     * @param pml PMLookup that was inside that message that has already been
     *            extracted in the processEvent method
     * @throws CantSplitException
     */
    private void processMergeDone(PhtMessage message, PMLookup pml) throws CantSplitException {
        String label;
        PhtNode node;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);

        /* If the node does not exists, there is a problem */
        if (node == null) {
            throw new CantSplitException("processMergeDone node null\n"
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey()
                    + " <> father's label: '" + message.getInitiatorLabel() + "'\n"
                    + " <> father's label: '" + pml.getKey()
                    + " <> father's node: " + message.getInitiator().getID() + "\n"
                    + " <> this nodes is: " + this.node.getID()
                    + " <> type: " + PhtMessage.SPLIT_DATA + "\n\n"
                    + " <>\nsize:" + this.nodes.size()
                    + "\nvalues: " + this.nodes.values()
                    + "\n" + toString());
        }

        log(String.format("((%d)) processMergeDone [node: '%s'][initiator: '%s' on %d]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID()));

        if (this.nodes.remove(pml.getDestLabel()) == null ) {
            interrupt();
        }

        node.clear();
        this.nodes.remove(node.getLabel());

        pml.setLess(true);
        message.setType(PhtMessage.ACK_MERGE_DONE);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
        
    }


    /* :::::::::: UPDATE NUMBER OF KEYS ::::::::: */

    /**
     * Increment or decrement a PhtNode's number of keys.
     * @param message Message with all the information needed
     * @param pml PMLookup already extracted in the processEvent method
     * @throws PhtNodeNotFoundException
     */
    private void processUpdateNbKeys(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        int update;
        boolean ok = false;
        PhtNode father;

        father = this.nodes.get(pml.getDestLabel());

        // If the father who receives this message does not exist there is a
        // problem
        if (father == null) {
            throw new PhtNodeNotFoundException("processUpdateNbKeys "
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: "   + pml.getKey()
                    + " <> contains key: " + this.nodes.containsKey(pml.getDestLabel())
                    + " <> type: "  + message.getType()
                    + "\n");
        }

        father.use();
        father.useDest();
        this.usageDest++;

        update = message.getType() - PhtMessage.UPDATE_NBKEYS;
        father.updateNbKeys(update);

        if (pml.getLess() instanceof Boolean) {
            ok = (Boolean) pml.getLess();
        }

        if (ok && (update < 0)) {
            if (father.getNbKeys() < PhtProtocol.B+1) {
                father.state.startMerge();
                sendMerge(father.getLabel(), father.getLson());
                sendMerge(father.getLabel(), father.getRson());
            } else {
                MSPClient.release();
            }
        } else if (!ok && (update < 0) && (father.getNbKeys() < PhtProtocol.B+1)) {
            stats.curr().incMergeAvoid(message.getId());
        }

        if (! father.getLabel().equals("")) {
            pml.setLess(false);
            pml.setDest(father.getFather().getNode());
            pml.setDestLabel(father.getFather().getKey());
            message.setInitiatorLabel(father.getLabel());
            message.setInitiator(this.node);
            EDSimulator.add(delay(), message, father.getFather().getNode(),phtid);
            
        }
    }

    /**
     * Update a PhtNode's previous leaf
     * @param message PhtMessage with information for the logs
     * @param pml Has the new previous leaf
     * @throws PhtNodeNotFoundException
     */
    private void processUpdatePreviousLeaf(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        NodeInfo leaf;
        PhtNode node;

        node = this.nodes.get(pml.getDestLabel());
//        for (PhtNode nd: this.nodes.values()) {
//            if (nd.getLabel().equals(pml.getDestLabel())) {
//                node = nd;
//                break;
//            }
//        }

        if (node == null) {
            throw new PhtNodeNotFoundException("processUpdatePreviousLeaf \n"
                    + "<> id: " + message.getId() + "\n"
                    + "<> this node is: " + this.node.getID() + "\n"
                    + "<> initiatorLabel: " + message.getInitiatorLabel()
                    + " <> initiator: " + message.getInitiator().getID()
                    + "\n<> destLabel: " + pml.getDestLabel()
                    + " <> dest: " + pml.getDest().getID() + "\n");
        }

        node.use();
        node.useDest();
        this.usageDest++;

        log(String.format("((%d)) [%s] processUpdatePreviousLeaf: Node found !\n",
                message.getId(),
                node.getLabel()));

        if (pml.getLess() instanceof NodeInfo) {
            leaf = (NodeInfo) pml.getLess();
            node.setPrevLeaf(leaf);
        }
    }

    /**
     * Update a PhtNode's next leaf
     * @param message Just for the log
     * @param pml Contains the next leaf
     * @throws PhtNodeNotFoundException
     */
    private void processUpdateNextLeaf(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        NodeInfo leaf;
        PhtNode node;

        node = this.nodes.get(pml.getDestLabel());
//        for (PhtNode nd: this.nodes.values()) {
//            if (nd.getLabel().equals(pml.getDestLabel())) {
//                node = nd;
//                break;
//            }
//        }

        if (node == null) {
            throw new PhtNodeNotFoundException("processUpdateNextLeaf");
        }

        node.use();
        node.useDest();
        this.usageDest++;

        log(String.format("((%d)) [%s] processUpdateNextLeaf: Node found !\n",
                message.getId(),
                node.getLabel()));

        if (pml.getLess() instanceof NodeInfo) {
            leaf = (NodeInfo) pml.getLess();
            node.setNextLeaf(leaf);
        }
    }

    /* ___________________________               ____________________________ */
    /* ___________________________ Ack reception ____________________________ */

    /**
     * Nothing to do after for two time requests (suppression, queries...).
     * If there is something to do after the lookup, start it.
     * @param message Request message
     * @throws NoPMLookupException
     * @throws NotAllowedOperationException
     * @throws WrongStateException
     */
    private void processAck_LinLookup(PhtMessage message)
            throws NoPMLookupException,
            NotAllowedOperationException,
            WrongStateException {
        PMLookup pml;

        pml = checkLookup(message, "processAck_LinLookup");

        log(String.format("((%d)) processAck_LinLookup [initiator: '%s'][type: %d] "
                        + "[op: %d][key: '%s']    [%d]\n",
                message.getId(), message.getInitiator().getID(), message.getType(),
                pml.getOperation(), pml.getKey(), this.node.getID()));

        /* Next step */
        switch (pml.getOperation()) {
            case PhtMessage.INSERTION:
                message.setType(PhtMessage.INSERTION);
                pml.setLess(this.currentData);

                if (this.state != PHT_INSERTION1) {
                    throw new WrongStateException(
                            String.format("(%d) processAck_LinLookup %d",
                                    message.getId(), this.state));
                } else {
                    this.state = PHT_INSERTION2;
                }
                break;

            case PhtMessage.SUPRESSION:
                if (this.state != PHT_SUPPRESSION) {
                    throw new WrongStateException(
                            String.format("(%d) processAck_LinLookup %d",
                                    message.getId(), this.state));
                } else {
                    this.state = PHT_INIT;
                }
                return;

            case PhtMessage.LIN_LOOKUP:
                if (this.state != PHT_LOOKUP) {
                    throw new WrongStateException(
                            String.format("(%d) processAck_LinLookup %d",
                                    message.getId(), this.state));
                } else {
                    long id = message.getId();

                    this.state = PHT_INIT;
                    this.clients.get(id).responseValue(id, pml.getKey(), pml.getLess());

                    MSPClient.release();
                }
                return;

            case PhtMessage.BIN_LOOKUP:
                break;

            case PhtMessage.SEQ_QUERY:
                break;

            case PhtMessage.PAR_QUERY:
                break;

            default:
                throw new NotAllowedOperationException(
                        "processAck_LinLookup " + pml.getOperation()
                );
        }


        if (pml.getDest() == null) {
            log("@@@@@ dest null in processAck_LinLookup @@@@@");
        }

        
        EDSimulator.add(delay(), message, pml.getDest(), phtid);
    }

    /**
     * The initiator of a sequential query calls this method when it receive
     * an ACK from a leaf. The keys and data are passed to the client and the
     * two range query counters are updated:
     *   1) rqCount (number of ACK received)
     *   2) rqTotal (number of leaves in the range query = number of ACK the
     * initiator must received before it can tell the client the query is
     * done)
     * @param message Keys and data send from the leaf
     * @throws NoPMLookupException
     */
    private void processAck_SeqQuery(PhtMessage message)
            throws NoPMLookupException {
        PMLookup pml;
        PMRangeQuery pmrq;

        pml = checkLookup(message, "processAck_SeqQuery");

        if (pml.getLess() instanceof PMRangeQuery) {
            pmrq = (PMRangeQuery) pml.getLess();
        } else {
            log(String.format("((%d)) processAck_SeqQuery pml.getLess() instanceof %s",
                    message.getId(), pml.getLess().getClass().getName()));
            return;
        }

        if (pmrq.isEnd()) {
            log(String.format("::processAck_SeqQuery:: end '%s' to '%s' count: %d " +
                            "total: %d\n",
                    pmrq.getKeyMin(), pmrq.getKeyMax(), this.rqCount, this.rqTotal));
            this.rqTotal = pmrq.getCount();
        }

        this.rqCount++;
        if (this.rqCount == this.rqTotal) {
            log(String.format("::processAck_SeqQuery:: total: %d OK '%s' to '%s'\n",
                    this.rqTotal, pmrq.getKeyMin(), pmrq.getKeyMax()));
            MSPClient.release();
        }

        this.clients.get(message.getId()).responseList(message.getId(), pmrq.getKdata());

        log(String.format("((%d)) processAck_SeqQuery <> %d keys\n",
                message.getId(), pmrq.getCount()));
    }

    /**
     * A leaf send an ACK to its father during a parallel query.
     * This father sends an ACK to his father, until the starting PhtNode is
     * reached (the one with the smallest common prefix who started the
     * parallel query).
     *
     * Since an internal node must wait an ACK from it two
     * sons, the first ACK is stored and restored when the second arrives.
     *
     * The number of leaves is the sum of the number of leaves in his
     * left subtree and its right subtree.
     * @param message Information we need
     * @param pml Contains the counter (number of leaves)
     * @throws PhtNodeNotFoundException
     */
    private void processAck_ParQuery (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        int count;
        NodeInfo next;
        PhtNode node;

        // Get the node
        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_ParQuery"
                    + pml.getDestLabel());
        }

        node.use();
        node.useDest();
        this.usageDest++;

        if (pml.getLess() instanceof Integer) {
            count = (Integer) pml.getLess();
        } else {
            return;
        }

        /*
         * The whole message is stored which is not needed.
         * This could be improved.
         */
        if (! node.storeMessage(message)) {
            PhtMessage storedMessage;
            PMLookup storedPml;
            int storedCount;

            storedMessage = node.returnMessage();
            storedPml     = (PMLookup) storedMessage.getMore();

            if (storedPml.getLess() instanceof Integer) {
                storedCount = (Integer) storedPml.getLess();
            } else {
                System.out.printf("\n\ntest::: not an integer: %s\n\n",
                        storedPml.getLess().getClass().getName());
                return;
            }

            // Special case: range query with only one node in the Pht
            if (node.getLabel().equals("")) {
                next = new NodeInfo("", this.node);
            } else {
                next = new NodeInfo(node.getFather().getKey(), node.getFather().getNode());
            }

            

            // Starting leaf
            if ( node.getLabel().equals(pml.getKey()) ) {
                pml.setLess(storedCount + count);
                message.setType(PhtMessage.ACK_PAR_QUERY_CLIENT_F);
                System.out.printf("\n\nstarting leaf('%s'): count: %d\n\n",
                        node.getLabel(),
                        storedCount + count);
                EDSimulator.add(delay(), message, message.getInitiator(), phtid);
            } else {
                // ACK to the father
                pml.setLess(storedCount + count);
                pml.setDest(next.getNode());
                pml.setDestLabel(next.getKey());
                System.out.printf("\n\nACK father: count: %d\n\n", storedCount + count);
                EDSimulator.add(delay(), message, node.getFather().getNode(), phtid);
            }
        }
    }

    /**
     * ACK send to the initiator during a parallel query.
     * processAck_ParQueryClient sends the keys and the data to the client and
     * inform him that the query has ended if so.
     * @param message Id of the request
     * @param pml Contains the PMRangeQuery with the keys and the data
     */
    private void processAck_ParQueryClient(PhtMessage message, PMLookup pml) {
        PMRangeQuery pmrq;

        if (pml.getLess() instanceof PMRangeQuery) {
            pmrq = (PMRangeQuery) pml.getLess();
        } else {
            log(String.format("((%d)) processAck_ParQuery pml.getLess() instanceof %s",
                    message.getId(), pml.getLess().getClass().getName()));
            return;
        }

        this.clients.get(message.getId()).responseList(message.getId(), pmrq.getKdata());

        this.rqCount++;
        System.out.printf("::processAck_ParQueryClient:: [%d] count: %d (total: %d) <> '%s' to " +
                        "'%s'\n",
                this.node.getID(),
                this.rqCount, this.rqTotal, pmrq.getKeyMin(), pmrq.getKeyMax());

        if (this.rqCount == this.rqTotal) {
            System.out.printf("::processAck_ParQueryClient:: total: %d OK '%s' to '%s'\n",
                    this.rqTotal, pmrq.getKeyMin(), pmrq.getKeyMax());
            MSPClient.release();
        }

        log(String.format("((%d)) processAck_ParQueryClient <> %d keys\n",
                message.getId(), pmrq.getCount()));
    }

    /**
     * ACK send by the the PhtNode who started the parallel query (the
     * smallest common prefix PhtNode) with the number of leaves
     * participating to the parallel query => number of ACK the initiator
     * should receive from leaves.
     * @param pml Contains the number of leaves in this parallel query
     */
    private void processAck_ParQueryClientF(PMLookup pml) {

        if (pml.getLess() instanceof Integer) {
            this.rqTotal = (Integer) pml.getLess();
        }

        if (this.rqTotal == this.rqCount) {
            System.out.printf("::processAck_ParQueryClientF:: [%d] total: %d OK\n",
                    this.node.getID(), this.rqTotal);
            MSPClient.release();
        }

        System.out.printf("::processAck_ParQueryClientF:: [%d] total: %d count: %d\n",
                this.node.getID(), this.rqTotal, this.rqCount);
    }


    /**
     * Last part of an insertion request.
     * Back to initial state, and response to the client.
     * @param message Request message
     * @throws BadAckException
     */
    private void processAck_Insertion(PhtMessage message)
            throws
            BadAckException {
        long id = message.getId();
        int res;

        log(String.format("((%d)) processAck_Insertion [initiator: '%s'][type: %d] "
                        + "[ok: '%s']    [%d]\n",
                message.getId(), message.getInitiator().getID(), message.getType(),
                message.getMore().toString(), this.node.getID()));

        if (message.getMore() instanceof Integer) {
            res = (Integer) message.getMore();
        } else {
            throw new BadAckException("PHT Bad Ack processAck_Insertion");
        }

        this.state = PHT_INIT;
        this.clients.get(id).responseOk(id, res);

        if (res == 0) {
            MSPClient.release();
        }
    }

    /**
     * Tell the client if the suppression has been done.
     * @param message Message to process
     * @throws WrongStateException
     * @throws BadAckException
     */
    private void processAck_Suppression (PhtMessage message)
            throws WrongStateException, BadAckException {
        boolean ok = checkAck(message, "processAck_Suppression", PHT_SUPPRESSION);
        long id = message.getId();
        int res;

        if (ok) {
            res = 0;
        } else {
            res = -1;
        }

        this.state = PHT_INIT;
        this.clients.get(id).responseOk(id, res);
    }


    /* :::::::::: SPLIT ::::::::: */

    /**
     * Tell the father if his son split well.
     * Update his lson/rson field with the son's label and node (peersim)
     * @param message Message to process
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     * @throws NoPMLookupException
     */
    private void processAck_Split(PhtMessage message)
            throws PhtNodeNotFoundException,
            BadAckException,
            NoPMLookupException {
        boolean ok = false;
        PMLookup pml;
        PhtNode node = null;
        String label;
        List<NodeInfo> info;

        pml = checkLookup(message, "processAck_Split");

        // If something went wrong, no need to continue
        if (pml.getLess() instanceof  Boolean) {
            ok = (Boolean) pml.getLess();
        }

        log(String.format("((%d)) processAck_Split [initiator: '%s' <> '%s'][type: %d] "
                        + "[label: '%s'][ok: %s]    [%d]\n",
                message.getId(),
                message.getInitiator().getID(), message.getInitiatorLabel(),
                message.getType(),
                pml.getDestLabel(), pml.getLess().toString(), this.node.getID()));

        // continue or throw a BadAckException
        if (! ok) {
            throw new BadAckException("processAck_Split: pml.getLess() -> false ");
        }

        // Get the father
        for (PhtNode nd: this.nodes.values()) {
            if (nd.getLabel().equals(message.getInitiatorLabel())) {
                node = nd;
                break;
            }
        }
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_Split "
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey()
                    + " <> father's label: '" + message.getInitiatorLabel() + "'\n"
                    + " <> father's label: '" + pml.getKey() + "'\n"
                    + " <> father's node: " + message.getInitiator().getID()
                    + " <> this nodes is: " + this.node.getID()
                    + " <> type: " + PhtMessage.SPLIT_DATA + "\n"
                    + " <> nodes in the network:\n"
            );
        }

        node.use();
        node.useDest();
        this.usageDest++;

        label = node.getLabel();
        info = new LinkedList<NodeInfo>();

        /*
         * If we receive an ACK for a split operation, we must be sure that the
         * father node has started a split operation.
         */

        // Get the leaves for the left and right sons
        if (pml.getDestLabel().charAt(label.length()) == '0') {
            info.add( node.getPrevLeaf() );
            info.add( node.getRson() );

            if (! node.state.ackSplitLson() ) {
                interrupt();
            }
            node.setLson( new NodeInfo(pml.getDestLabel(), pml.getDest()) );
        } else {
            info.add( node.getLson() );
            info.add( node.getNextLeaf() );

            if (! node.state.ackSplitRson() ) {
                interrupt();
            }
            node.setRson( new NodeInfo(pml.getDestLabel(), pml.getDest()) );
        }

        pml.setLess(info);
        message.setType(PhtMessage.SPLIT_LEAVES);
        if (node.state.twoAckSplit()) {
            PhtMessage storedMessage;
            PMLookup storedPml;

            if (pml.getDest() == null) {
                log("@@@@@ dest null in processAck_Split first @@@@@");
            }

            // Restore the message from the first ACK
            storedMessage = node.returnMessage();
            storedPml     = (PMLookup) storedMessage.getMore();

            if (pml.getDest() == null) {
                log("@@@@@ dest null in processAck_Split second @@@@@");
            }

            /*
             * Send to left son first
             */

            if (storedPml.getDestLabel().endsWith("0")) {
                startUpdateLeavesL(storedMessage, storedPml, node);
                startUpdateLeavesR(message, pml, node);
            } else {
                startUpdateLeavesL(message, pml, node);
                startUpdateLeavesR(storedMessage, storedPml, node);
            }

        } else {
            node.storeMessage(message);
        }
    }

    /**
     * Tell the father if the son has been created.
     * Start the splitData operation.
     * @param message Message to process with the label of their father and the
     *                label of the son.
     * @throws BadAckException
     */
    private void processAck_SplitLeaves (PhtMessage message)
            throws BadAckException,
            NoPMLookupException,
            PhtNodeNotFoundException {
        boolean ok = false;
        PMLookup pml;
        PhtNode node = null;
        String label;
        List<PhtData> data;

        pml = checkLookup(message, "processAck_SplitLeaves");

        // If something went wrong, no need to continue
        if (pml.getLess() instanceof  Boolean) {
            ok = (Boolean) pml.getLess();
        }

        log(String.format("((%d)) processAck_SplitLeaves [initiator: '%s' <> '%s'][type: %d] "
                        + "[label: '%s'][ok: %s]    [%d]\n",
                message.getId(),
                message.getInitiator().getID(), message.getInitiatorLabel(),
                message.getType(),
                pml.getDestLabel(), pml.getLess().toString(), this.node.getID()));

        // continue or throw a BadAckException
        if (! ok) {
            throw new BadAckException("processAck_SplitLeaves: pml.getLess() -> false ");
        }

        // Get the father
        for (PhtNode nd: this.nodes.values()) {
            if (nd.getLabel().equals(message.getInitiatorLabel())) {
                node = nd;
                break;
            }
        }
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_SplitLeaves "
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: " + pml.getKey()
                    + " <> father's node: " + message.getInitiator().getID()
                    + " <> type: " + PhtMessage.SPLIT_LEAVES);
        }

        node.use();
        node.useDest();
        this.usageDest++;

        label = node.getLabel();

        // Get the data for the left and right sons
        if (pml.getDestLabel().charAt(label.length()) == '0') {
            data = node.splitDataLson();

            // Statistics
            if (data.size() == node.getNbKeys()) {
                stats.curr().incSplitAvoid();
            }

            if (! node.state.ackSplitLeavesLson() ) {
                interrupt();
            }
            node.setLson( new NodeInfo(pml.getDestLabel(), pml.getDest()) );
        } else {
            data = node.splitDataRson();

            // Statistics
            if (data.size() == node.getNbKeys()) {
                stats.curr().incSplitAvoid();
            }

            if (! node.state.ackSplitLeavesRson() ) {
                interrupt();
            }
            node.setRson(new NodeInfo(pml.getDestLabel(), pml.getDest()));
        }

        /*
         * Before we can start the split_data phase, the node must have
         * received two ACK from the split phase. If not, wait for the other
         * split ACK to arrive before moving forward. This means we add the
         * same event to the simulator.
         */

        pml.setLess(data);
        message.setType(PhtMessage.SPLIT_DATA);


        if (pml.getDest() == null) {
            log("@@@@@ dest null in processAck_SplitLeaves @@@@@");
        }

        if (node.state.twoAckSplitLeaves()) {
            PhtMessage storedMessage;
            PMLookup storedPml;

            
            EDSimulator.add(delay(), message, pml.getDest(), phtid);

            // Restore the message from the first ACK
            storedMessage = node.returnMessage();
            storedPml     = (PMLookup) storedMessage.getMore();

            
            EDSimulator.add(delay(), storedMessage, storedPml.getDest(), phtid);
        } else {
            node.storeMessage(message);
        }
    }

    /**
     * Tell the father that the son got all the keys that he send to him
     * @param message Message with all the information needed
     * @throws BadAckException
     * @throws NoPMLookupException
     * @throws PhtNodeNotFoundException
     */
    private void processAck_SplitData(PhtMessage message)
            throws BadAckException,
            NoPMLookupException,
            PhtNodeNotFoundException {
        boolean ok = false;
        PMLookup pml;
        PhtNode node = null;
        String label;

        pml = checkLookup(message, "processAck_SplitData");

        // If something went wrong, no need to continue
        if (pml.getLess() instanceof  Boolean) {
            ok = (Boolean) pml.getLess();
        }

        log(String.format("((%d)) processAck_SplitData [initiator: '%s' <> '%s'][type: %d] "
                        + "[label: '%s'][ok: %s]    [%d]\n",
                message.getId(),
                message.getInitiator().getID(), message.getInitiatorLabel(),
                message.getType(),
                pml.getDestLabel(), pml.getLess().toString(), this.node.getID()));

        if (! ok) {
            throw new BadAckException("processAck_SplitData: pml.getLess() -> false ");
        }

        // Get the father
        for (PhtNode nd: this.nodes.values()) {
            if (nd.getLabel().equals(message.getInitiatorLabel())) {
                node = nd;
                break;
            }
        }
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_SplitData "
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey()
                    + " <> father's label: '" + message.getInitiatorLabel() + "'\n"
                    + " <> father's label: '" + pml.getKey()
                    + "' (-> " + Integer.parseInt(pml.getKey(), 2) + ")\n"
                    + " <> father's node: " + message.getInitiator().getID() + "\n"
                    + " <> this nodes is: " + this.node.getID()
                    + " <> type: " + PhtMessage.SPLIT_DATA + "\n\n"
                    + " <>\nsize:" + this.nodes.size()
                    + "\nvalues: " + this.nodes.values()
                    + "\n" + toString()
                    + "\n<>\n\n"
                    + " <> nodes in the network:\n"
            );
        }

        node.use();
        node.useDest();
        this.usageDest++;

        label = pml.getDestLabel();

        // Change the node's state
        if ( label.charAt( label.length()-1 ) == '0' ) {
            if (! node.state.ackSDataLson() ) {
                throw new PhtNodeNotFoundException("processAck_SplitData ackSDataLson fail\n"
                        + " <> state: " + this.state
                        + " <> label: " + pml.getDestLabel() + "\n"
                        + " <> key: " + pml.getKey()
                        + " <> father's label: '" + message.getInitiatorLabel() + "'\n"
                        + " <> father's label: '" + pml.getKey()
                        + "' (-> " + Integer.parseInt(pml.getKey(), 2) + ")\n"
                        + " <> father's node: " + message.getInitiator().getID() + "\n"
                        + " <> this nodes is: " + this.node.getID()
                        + " <> type: " + PhtMessage.SPLIT_DATA + "\n\n"
                        + " <>\nsize:" + this.nodes.size()
                        + "\nvalues: " + this.nodes.values()
                        + "\n" + toString()
                        + "\n<>\n\n"
                        + " <> nodes in the network:\n"
                );
            }
        } else {
            if (! node.state.ackSDataRson() ) {
                throw new PhtNodeNotFoundException("processAck_SplitData ackSDataRson fail\n"
                        + " <> state: " + this.state
                        + " <> label: " + pml.getDestLabel() + "\n"
                        + " <> key: " + pml.getKey()
                        + " <> father's label: '" + message.getInitiatorLabel() + "'\n"
                        + " <> father's label: '" + pml.getKey()
                        + " <> father's node: " + message.getInitiator().getID() + "\n"
                        + " <> this nodes is: " + this.node.getID()
                        + " <> type: " + PhtMessage.SPLIT_DATA + "\n\n"
                        + " <>\nsize:" + this.nodes.size()
                        + "\nvalues: " + this.nodes.values()
                        + "\n" + toString()
                        + "\n<>\n\n"
                        + " <> nodes in the network:\n");
            }
        }

        if (node.state.isStable()) {
            node.internal();
       }

        MSPClient.release();
    }

    /* :::::::::: MERGE ::::::::: */

    /**
     * Get the data send from the son and insert them into the initiator
     * PhtNode.
     * @param message Message with all the information needed
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     * @throws NoPMLookupException
     */
    private void processAck_Merge(PhtMessage message)
            throws PhtNodeNotFoundException,
            BadAckException,
            NoPMLookupException {
        boolean continueMerge;
        PMLookup pml;
        PhtNode node;

        pml = checkLookup(message, "processAck_Merge");

        // If something went wrong, no need to continue
        if (pml.getLess() instanceof Boolean) {
            continueMerge = (Boolean) pml.getLess();
        } else {
            throw new BadAckException("processAck_Merge: pml.getLess() -> false ");
        }

        // Get the father
        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            StringBuilder sb = new StringBuilder(this.nodes.size());

            for (PhtNode nd: this.nodes.values()) {
                sb.append( nd.toString() );
            }

            throw new PhtNodeNotFoundException("processAck_Merge "
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: " + pml.getKey()
                    + " <> father's node: " + message.getInitiator().getID()
                    + " <> type: " + PhtMessage.SPLIT_DATA
                    + " <> nodes " + sb.toString());
        }

        if (! continueMerge) {
            node.state.noMerge();

            System.out.println("No merge");

            
            message.setType(PhtMessage.NO_MERGE);
            if (pml.getDestLabel().endsWith("0")) {
                EDSimulator.add(delay(), message, node.getRson().getNode(), phtid);
            } else {
                EDSimulator.add(delay(), message, node.getLson().getNode(), phtid);
            }

            return;
        }

        log(String.format("((%d)) processAck_Merge [node: '%s'][initiator: '%s' on %d] \n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID()));

        message.setType(PhtMessage.MERGE_LEAVES);
        EDSimulator.add(delay(), message, pml.getDest(), phtid);
        
    }


    /**
     * Get the leaf send from the son, if it is from his left son, it is a
     * previous leaf, a next leaf otherwise
     * @param message Message with all the information needed
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     * @throws NoPMLookupException
     */
    private void processAck_MergeLeaves(PhtMessage message)
            throws PhtNodeNotFoundException,
            BadAckException,
            NoPMLookupException {
        PMLookup pml;
        PhtNode node;
        String label;
        NodeInfo leaf;

        pml = checkLookup(message, "processAck_MergeLeaves");

        // If something went wrong, no need to continue
        if (pml.getLess() instanceof NodeInfo) {
            leaf = (NodeInfo) pml.getLess();
        } else {
            throw new BadAckException("processAck_MergeLeaves: pml.getLess() -> false ");
        }

        // Get the father
        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_MergeLeaves "
                    + " <> state: " + this.state
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: " + pml.getKey()
                    + " <> father's node: " + message.getInitiator().getID()
                    + " <> type: " + message.getType());
        }

        label = pml.getDestLabel();
        if (label.charAt( label.length()-1 ) == '0') {
            node.setPrevLeaf(leaf);
        } else {
            node.setNextLeaf(leaf);
        }

        log(String.format("((%d)) processAck_MergeLeaves [node: '%s'][initiator: '%s' on %d] "
                        + "[node's state: %s]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID(),
                node.state));

        pml.setLess(true);
        message.setType(PhtMessage.MERGE_DATA);

        EDSimulator.add(delay(), message, pml.getDest(), phtid);
        
    }

    /**
     * Insert keys and data send by a son during a merge process
     * @param message Message of the request
     * @param pml Contains the keys and data
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     */
    private void processAck_MergeData (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException,
            BadAckException {
        PhtNode father;

        father = this.nodes.get(message.getInitiatorLabel());
        if (father == null) {
            throw new PhtNodeNotFoundException("processAck_MergeData '"
                    + pml.getDestLabel() + "'");
        }

        if (pml.getLess() instanceof List) {
            father.insertMerge((List<PhtData>) pml.getLess());
        } else {
            throw new BadAckException("processAck_MergeData "
                    + pml.getLess().getClass().getName());
        }

        message.setType(PhtMessage.MERGE_DONE);
        EDSimulator.add(delay(), message, pml.getDest(), phtid);
        
    }


    /**
     * Process the ACK send from a son who has ended the his merge process.
     * If the both sons have ended, the whole merge process ends.
     * @param message Message with all the information needed.
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     * @throws NoPMLookupException
     */
    private void processAck_MergeDone(PhtMessage message)
            throws BadAckException,
            NoPMLookupException,
            PhtNodeNotFoundException {
        PMLookup pml;
        PhtNode node;

        pml = checkLookup(message, "processAck_MergeDone");

        // If something went wrong, no need to continue
        if (!(pml.getLess() instanceof Boolean)) {
            throw new BadAckException("processAck_MergeDone: pml.getLess() -> false ");
        }

        // Get the father
        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_MergeDone "
                    + message.getInitiatorLabel());
        }

        log(String.format("((%d)) processAck_MergeDone [node: '%s'][initiator: '%s' on %d] "
                        + "[node's state: %s]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID(),
                node.state));

        if (pml.getDestLabel().endsWith("0")) {
            if(! node.state.mergeDoneLSon() ) {
                interrupt();
            }
        } else if (pml.getDestLabel().endsWith("1")) {
            if(! node.state.mergeDoneRSon() ) {
                interrupt();
            }
        }

        if (node.state.isStable()) {
            node.Leaf();

            // Update for the previous leaf
            startUpdateLeavesMerge(
                    message.getId(),
                    PhtMessage.UPDATE_NEXT_LEAF,
                    node.getLabel(),
                    node.getPrevLeaf()
            );

            // Update for the next leaf
            startUpdateLeavesMerge(
                    message.getId(),
                    PhtMessage.UPDATE_PREV_LEAF,
                    node.getLabel(),
                    node.getNextLeaf()
            );
        }

        MSPClient.release();
    }

    /* ________________________                       _______________________ */
    /* ________________________ Process methods tools _______________________ */

    /**
     * Method to lighten the code of processLin_Lookup: a little switch
     * @param node current PhtNode
     * @param key Key searched
     * @return The left or the right son depending on the key
     */
    private NodeInfo forwardLookup (PhtNode node, String key) {
        if (key.charAt(node.getLabel().length()) == '0') {
            return node.getLson();
        }

        return node.getRson();
    }

    /**
     * Looks if the more field of PhtMessage is a PMLookup
     * @param message Message to process
     * @param info String for exception (if any)
     * @return The PMLookup of the more field
     * @throws NoPMLookupException
     */
    private PMLookup checkLookup (PhtMessage message, String info)
            throws NoPMLookupException {
        if (message.getMore() instanceof PMLookup) {
            return (PMLookup) message.getMore();
        } else {
            throw new NoPMLookupException(info);
        }
    }

    /**
     * @param message Message beeing processed
     * @param info Name of the method calling this method
     * @param st State in which PhtProtocol should be
     * @return True if the request went well
     * @throws WrongStateException
     * @throws BadAckException
     */
    private boolean checkAck (PhtMessage message, String info, int st)
            throws WrongStateException, BadAckException {
        if (this.state != st) {
            throw new WrongStateException(info + this.state);
        }

        boolean ok;

        if (message.getMore() instanceof Boolean) {
            ok = (Boolean) message.getMore();
        } else {
            throw new BadAckException(info + message.getMore().getClass().getName());
        }

        return ok;
    }

    /**
     * Split related method
     *
     * Send to the left son its previous and next leaves
     * @param message PhtMessage of the split request
     * @param pml PMLookup extracted from message
     * @param node Current node (who started the split)
     */
    private void startUpdateLeavesL (PhtMessage message, PMLookup pml, PhtNode node) {
        List<NodeInfo> info;
        PhtMessage update;
        PMLookup pmlUpdate;

        info      = new LinkedList<NodeInfo>();
        pmlUpdate = new PMLookup(
                node.getLabel(),
                message.getType(),
                pml.getDest(),
                pml.getDestLabel()
        );
        update   = new PhtMessage(
                message.getType(),
                message.getInitiator(),
                message.getInitiatorLabel(),
                message.getId(),
                pmlUpdate
        );

        info.add(
                new NodeInfo(
                        node.getPrevLeaf().getKey(),
                        node.getPrevLeaf().getNode()
                )
        );
        info.add(
                new NodeInfo(
                        node.getRson().getKey(),
                        node.getRson().getNode()
                )
        );

        pmlUpdate.setLess(info);
        EDSimulator.add(delay(), update, node.getLson().getNode(), phtid);
        
    }

    /**
     * Split related method. Send to the right son its previous and next leaves
     * @param message PhtMessage of the split request
     * @param pml PMLookup extracted from message
     * @param node Current node (who started the split)
     */
    private void startUpdateLeavesR (PhtMessage message, PMLookup pml, PhtNode node) {
        List<NodeInfo> info;
        PhtMessage update;
        PMLookup pmlUpdate;

        info      = new LinkedList<NodeInfo>();
        pmlUpdate = new PMLookup(
                node.getLabel(),
                message.getType(),
                pml.getDest(),
                pml.getDestLabel()
        );
        update   = new PhtMessage(
                message.getType(),
                message.getInitiator(),
                message.getInitiatorLabel(),
                message.getId(),
                pmlUpdate
        );

        info.add(
                new NodeInfo(
                        node.getLson().getKey(),
                        node.getLson().getNode()
                )
        );
        info.add(
                new NodeInfo(
                        node.getNextLeaf().getKey(),
                        node.getNextLeaf().getNode()
                )
        );

        pmlUpdate.setLess(info);
        EDSimulator.add(delay(), update, node.getRson().getNode(), phtid);
        
    }

    /**
     * Update a leaf's previous or next leaf
     *
     * After a merge the father of the merge process, sends an udpate to its
     * previous and next leaves.
     * @param id Id of the request
     * @param type Type of the message
     * @param label Initiator PhtNode
     * @param dest Recipient
     */
    private void startUpdateLeavesMerge(long id,
                                        int type,
                                        String label,
                                        NodeInfo dest) {
        NodeInfo info;
        PhtMessage update;
        PMLookup pmlUpdate;

        if (dest.getNode() == null) {
            return;
        }

        pmlUpdate = new PMLookup(
                label,
                type,
                dest.getNode(),
                dest.getKey()
        );
        update   = new PhtMessage(
                type,
                this.node,
                label,
                id,
                pmlUpdate
        );
        info = new NodeInfo(
                label,
                this.node
        );

        pmlUpdate.setLess(info);
        EDSimulator.add(delay(), update, dest.getNode(), phtid);
        
    }

    /* _________________________                    _________________________ */
    /* _________________________ Implements methods _________________________ */


    /**
     * processEvent method for PhtProtocol: a big switch with a lot of methods
     * doing the needed job.
     * @param node the local node
     * @param pid the identifier of this protocol
     * @param event the delivered event
     */
    @Override
    public void processEvent(Node node, int pid, Object event) {
        PhtMessage message;
        PMLookup pml = null;

        /* An event must be a PhtMessage */
        if (event instanceof PhtMessage) {
            message = (PhtMessage) event;
        } else {
            return;
        }

        if (message.getMore() instanceof PMLookup) {
            pml = (PMLookup) message.getMore();
        }

        /* Avoid a huge switch with all the operations and there ACK */
        if (message.getType() > PhtMessage.ACK) {
            processAck(message, pml);
            return;
        }

        // A PhtMessage has arrived: this Node has been used
        this.usage++;

        /*
         * Big switch with just one call to the right method to process
         * the request
         */
        try {
            switch (message.getType()) {
                case PhtMessage.INIT:
                    log("PHT Yeeaah !");
                    log(String.format("PHT init message sent by %d [%d]\n",
                            message.getInitiator().getID(), this.node.getID()));
                    initiate();
                    break;

                case PhtMessage.SPLIT:
                    processSplit(message, pml);
                    break;

                case PhtMessage.SPLIT_DATA:
                    processSplitData(message);
                    break;

                case PhtMessage.SPLIT_LEAVES:
                    processSplitLeaves(message);
                    break;

                case PhtMessage.UPDATE_PREV_LEAF:
                    processUpdatePreviousLeaf(message, pml);
                    break;

                case PhtMessage.UPDATE_NEXT_LEAF:
                    processUpdateNextLeaf(message, pml);
                    break;

                case PhtMessage.MERGE:
                    processMerge(message, pml);
                    break;

                case PhtMessage.MERGE_LEAVES:
                    processMergeLeaves(message, pml);
                    break;

                case PhtMessage.MERGE_DATA:
                    processMergeData(message, pml);
                    break;

                case PhtMessage.MERGE_DONE:
                    processMergeDone(message, pml);
                    break;

                case PhtMessage.INSERTION:
                    processInsertion(message);
                    break;

                case PhtMessage.SUPRESSION:
                    break;

                case PhtMessage.UPDATE_NBKEYS_MINUS:
                case PhtMessage.UPDATE_NBKEYS_PLUS:
                    processUpdateNbKeys(message, pml);
                    break;

                case PhtMessage.LIN_LOOKUP:
                    stats.curr().incLookup(currentLookup);
                    processLinLookup(message, pml);
                    break;

                case PhtMessage.BIN_LOOKUP:
                    stats.curr().incLookup(currentLookup);
                    break;

                case PhtMessage.SEQ_QUERY:
                    stats.curr().incRangeQuery(currentRangeQuery);
                    processSeqQuery(message, pml);
                    break;

                case PhtMessage.PAR_QUERY:
                    stats.curr().incRangeQuery(currentRangeQuery);
                    processParQuery(message, pml);
                    break;

                default:
                    System.err.println("@@@@@ PHT processEvent: sorry only insertion for the " +
                            "moment "
                            + message.getType());

            }
        } catch (NoPMLookupException npmle) {
            npmle.printStackTrace();
        } catch (PhtNodeNotFoundException pnfe) {
            pnfe.printStackTrace();
        } catch (CantSplitException cse) {
            cse.printStackTrace();
        } catch (NoDataSplitData ndsde) {
            ndsde.printStackTrace();
        } catch (java.lang.NullPointerException npe) {
            try {
                logWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Called by processEvent to split the job and avoid a huge processEvent
     * method.
     * @param message the delivered message
     */
    private void processAck(PhtMessage message, PMLookup pml) {

        try {
            switch (message.getType()) {

                case PhtMessage.ACK_SPLIT:
                    processAck_Split(message);
                    break;

                case PhtMessage.ACK_SPLIT_DATA:
                    processAck_SplitData(message);
                    break;

                case PhtMessage.ACK_SPLIT_LEAVES:
                    processAck_SplitLeaves(message);
                    break;

                case PhtMessage.ACK_UPDATE_LEAVES:
                    break;

                case PhtMessage.ACK_UPDATE_NEXT_LEAF:
                    break;

                case PhtMessage.ACK_MERGE:
                    processAck_Merge(message);
                    break;

                case PhtMessage.ACK_MERGE_LEAVES:
                    processAck_MergeLeaves(message);
                    break;

                case PhtMessage.ACK_MERGE_DATA:
                    processAck_MergeData(message, pml);
                    break;

                case PhtMessage.ACK_MERGE_DONE:
                    processAck_MergeDone(message);
                    break;

                case PhtMessage.ACK_INSERTION:
                    processAck_Insertion(message);
                    break;

                case PhtMessage.ACK_SUPRESSION:
                    processAck_Suppression(message);
                    break;

                case PhtMessage.ACK_UPDATE_NBKEYS_MINUS:
                    break;

                case PhtMessage.ACK_UPDATE_NBKEYS_PLUS:
                    break;

                case PhtMessage.ACK_LIN_LOOKUP:
                    processAck_LinLookup(message);
                    break;

                case PhtMessage.ACK_BIN_LOOKUP:
                    break;

                case PhtMessage.ACK_SEQ_QUERY:
                    processAck_SeqQuery(message);
                    break;

                case PhtMessage.ACK_PAR_QUERY:
                    processAck_ParQuery(message, pml);
                    break;

                case PhtMessage.ACK_PAR_QUERY_CLIENT:
                    processAck_ParQueryClient(message, pml);
                    break;

                case PhtMessage.ACK_PAR_QUERY_CLIENT_F:
                    processAck_ParQueryClientF(pml);
                    break;

                default:
                    System.err.println("@@@@@ PHT processAck: sorry only ACK for the moment "
                            + message.getType());
            }
        } catch (NoPMLookupException nple) {
            nple.printStackTrace();
        } catch (NotAllowedOperationException naoe) {
            naoe.printStackTrace();
        } catch (PhtNodeNotFoundException pnfe) {
            pnfe.printStackTrace();
        } catch (WrongStateException wse) {
            wse.printStackTrace();
        } catch (BadAckException bae) {
            bae.printStackTrace();
        } catch (java.lang.NullPointerException npe) {
            try {
                logWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Clone method for PeerSim
     * @return a new PhtProtocol with the same configuration (prefix)
     */
    public Object clone() {
        try {
            return new PhtProtocol(prefix);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /*_______________________________            ____________________________ */
    /*_______________________________ Statistics ____________________________ */

    /**
     * Once everything has been initialized, get the PhaseStats instance.
     */
    public void setStats() {
        stats = Stats.getInstance();
    }

    /* ___________________________               ____________________________ */
    /* ___________________________ Tests methods ____________________________ */

    /**
     * toString for PhtProtocol
     * @return all (key, data) from its nodes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder(this.nodes.size() * PhtProtocol.D);

        sb.append("PHT ");
        sb.append(this.dht.getNodeId());
        sb.append(" ");
        sb.append(this.nodes.size());
        sb.append(" nodes.\n");
        for (Map.Entry<String, PhtNode> nd: this.nodes.entrySet()) {
            PhtNode node = nd.getValue();

            sb.append("id: ");
            sb.append(this.dht.getNodeId());
            sb.append(" (");
            sb.append(this.node.getID());
            sb.append(") $ node [label:");
            sb.append(node.getLabel());
            sb.append(" <> nbKeys: ");
            sb.append(node.getNbKeys());
            sb.append(" <> father: '");
            sb.append(node.getFather().getKey());
            sb.append(" <> leaf: ");
            sb.append(node.isLeaf());
            sb.append("']\n");
            sb.append(node.toString());
        }
        sb.append("\n");

        return sb.toString();
    }


    /* ___________________________                ___________________________ */
    /* ___________________________ Getter methods ___________________________ */

    /**
     * @return Return the <label, PhtNode> map of this PhtProtocol
     */
    public Map<String, PhtNode> getNodes() {
        return this.nodes;
    }

    /**
     * Access to the static field phtid
     * @return the protocol id of PhtProtocol in PeerSim
     */
    public static int getPid() {
        return phtid;
    }

    /**
     * Access to the static field nextId
     * @return the id of the most recent request
     */
    public static long getNextId() {
        return nextId;
    }

    /**
     * Get usage
     *
     * @return Number of times this Node (physical machine) has been used
     * during the simulation
     */
    public long getUsage() {
        return this.usage;
    }

    /**
     * Get usageDest
     *
     * @return Number of times this Node (physical machine) has been
     * the destination for an operation.
     */
    public long getUsageDest() {
        return this.usageDest;
    }

    public long getId() {
        return this.nid;
    }

    public int getNbNodes () {
        return this.nodes.size();
    }

   /* ___________________________                ___________________________ */
    /* ___________________________ Setter methods ___________________________ */

    /**
     * Set this.phtid to phtis
     * @param phtid New PhtId
     */
    public void setPhtid(int phtid) {
        PhtProtocol.phtid = phtid;
    }

    /**
     * Change the current dht to 'dht'
     * @param dht New Dht
     */
    public void setDht(DhtInterface dht) {
        this.dht = dht;
        this.node = this.dht.getNode();
    }

    /**
     * Set the nid field to the index of this PhtProtocol in the Network
     * nodes array.
     *
     * @param id new nid for this PhtProtocol
     */
    public void setNodeId(long id) {
        this.nid = id;
    }


    /*_________________________                        ______________________ */
    /*_________________________ Initiation for PeerSim ______________________ */

    /**
     * In PeerSim, the root of the Pht must be inserted in some PhtProtocol.
     * The node that receives an init message calls initiate() and the
     * simulation can begin.
     */
    public void initiate() {
        if (PhtProtocol.init) {
            return;
        }

        PhtProtocol.init = true;
        this.nodes.put("", new PhtNode("", this));
        log("PHT initiate() on node " + this.dht.getNodeId());

        System.out.println("PHT initiate");

        checkNetwork();
    }

    /**
     * Reset the static init field.
     * This is used for tests.
     */
    public void reset() {
        PhtProtocol.init = false;
    }

    /* _________________________________     ________________________________ */
    /* _________________________________ Log ________________________________ */

    /**
     * Log every information of every requests
     * @param info String to write into the log file
     */
    public static void log (String info) {
        try {
            logWriter.write(info);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ Delay _______________________________ */

    private short delay() {
        delay = (short) ((delay + 1) % MAX_DELAY);
        return 0;
    }

    /* ____________________________               ___________________________ */
    /* ____________________________ Debug methods ___________________________ */

    /**
     * A brutal way to stop the test and/or the simulation
     */
    public void interrupt() {
        try {
            throw new Exception("interrupt()");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * PhtProtocol's test method, for debug.
     */
    public void checkNetwork() {
        PhtProtocol pht;
        MSPastryListener lst;

        System.out.println(String.format("PHT B: %d <> D: %d\n", PhtProtocol.B, PhtProtocol.D));
        System.out.println("PHT check for network of size " + Network.size());

        for (int i = 0; i < Network.size(); i++) {
            pht = (PhtProtocol) Network.get(i).getProtocol(phtid);

            if (pht.nodes.size() == 0) {
                continue;
            }

            System.out.println("----------");
            System.out.println("PHT " +
                            Network.get(i).getProtocol(phtid).toString()
            );
            System.out.println("PASTRY "
                    + ((DhtInterface) Network.get(i).getProtocol(phtid - 1)).getNodeId());

            lst = (MSPastryListener) Network.get(i).getProtocol(phtid+1);
            System.out.println("LISTENER "
                    + ((DhtInterface) lst.getNode().getProtocol(phtid - 1)).getNodeId());
            System.out.println("----------");
        }
    }
}
