package peersim.pht;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.pht.dht.DhtInterface;
import peersim.pht.exceptions.*;
import peersim.pht.messages.PMLookup;
import peersim.pht.messages.PMRangeQuery;
import peersim.pht.messages.PhtMessage;
import peersim.pht.state.PhtNodeState;
import peersim.pht.statistics.Stats;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * <p>
 *     PhtProtocol is the core of this package.
 * </p>
 *
 * <p>
 *     A PhtProtocol contains a list of PhtNodes, has a state indicating what kind
 *     of requests he is waiting for, a request id counter, the id of PhtProtocol
 *     in the simulation, and data for insertion operations.
 * </p>
 *
 * <p>
 *     PhtProtocol sends messages (PhtMessage) and process events, and is the
 *     class who communicates with clients: a client asks for an insertion, a
 *     suppression, a lookup, or a range query operation, PhtProtocol starts
 *     everything and when it can, it responds to the client (the data searched,
 *     the result of the operation).
 * </p>
 */
public class PhtProtocol implements EDProtocol {

    /*
     * Protocol constants
     * D: Size of a key in bits
     * B: Maximum number of keys that a leaf should handle
     */
    public static int D;
    public static int B;

    /*
     * Different steps and operation.
     * Only one insertion can happen at a time, since the data to be inserted
     * is stored.
     */
    private static final int PHT_INIT        = 0;
    private static final int PHT_INSERTION1  = 1;
    private static final int PHT_INSERTION2  = 2;

    // PhtProtocol's state
    private int state;

    // Count the number of response received during a sequential range query
    private long rqCount = 0;

    // Number of response for a range query
    private long rqTotal = 0;

    // Prefix used by PeerSim
    private static String prefix;

    /* __________ Log __________ */

    /*
     * Every important steps of the simulation is logged.
     * This can helpful to understand what happened.
     * Note that choices must be made on what to log, otherwise the log file
     * will be really huge.
     */
    private static BufferedWriter logWriter;

    /* __________ Statistics __________ */

    // Global statistics
    private static Stats stats;

    // Every time this Node (the machine in the network) is being used
    private long usage;

    // Every time this Node is the destination for an operation
    private long usageDest;

    // The dht provides the basic operation that we need
    private DhtInterface dht;

    /* __________ Exception handler __________ */

    /**
     * Handle Pht exceptions.
     */
    public interface PhtExceptionHandler {
        void handle(PhtException pe);
    }

    /*
     * Set the exception handler in the configuration file.
     */
    private PhtExceptionHandler pehandler;

    /* __________ Tests __________ */

    /*
     * Contains every existing PhtNode on every PhtProtocol.
     * This is used to check whether or not a dht send has routed correctly
     * the message: if the destination does not contain a PhtNode which exists,
     * there is a problem.
     * This will arrive sooner or later when using the mspastry package as a
     * dht.
     * For more information, please checkout Pht's documentation.
     */
    private static Map<String, Long> allPhtNodes;

    private static boolean stopOnRouteFail;

    /* __________ Delay for messages __________ */

    private static short RETRY_FACTOR;
    private static short MAX_DELAY;
    private static short delay = 0;

    /* __________ Client __________ */

    /*
     * A client wants to insert, search and remove data. There is no need for
     * more than one Client: the clients can change from a PhtProtocol to
     * another.
     */
    private static Client client;

    /* __________ General information __________ */

    // Has every PhtProtocol been initialized (basic security) ?
    private static boolean init = false;

    // Every message exchanged has an id, starting from 0 to...a big number
    private static long nextId = 0;

    // PhtProtocol's protocol id for PeerSim
    private static int phtid;

    /*
     * PhtProtocol's Node Id (the index inside the Node array from the
     * {@link PeerSim.core.NetWork} class)
     */
    private long nid;

    // Set the default lookup and range query
    private static int currentLookup;
    private static int currentRangeQuery;

    // Node (PeerSim) on which this PhtProtocol is located
    private Node node;

    /*
     * Temporary store the data to insert, since an insert operation
     * always starts with a lookup.
     */
    private Object currentData;

    /*
     * A PhtProtocol has 0 or many PhtNodes, which can be accessed with
     * their label.
     */
    private final Map<String, PhtNode> nodes;

    /**
     * Constructor used by PeerSim
     * @param prefix used to get String, int, boolean, etc. from the
     *               {@link peersim.config.Configuration} class
     * @throws IOException
     */
    public PhtProtocol(String prefix) throws IOException {
        String logValue = Configuration.getString(prefix + ".log");
        PhtProtocol.prefix = prefix;

        PhtProtocol.B = Configuration.getInt(prefix + "." + "B");
        PhtProtocol.D = Configuration.getInt(prefix + "." + "D");

        // Maximum delay for messages
        MAX_DELAY = (short) Configuration.getInt(prefix + ".maxdelay");
        RETRY_FACTOR = (short) Configuration.getInt(prefix + ".factor");

        // Lookup
        currentLookup = PhtMessage.LIN_LOOKUP;

        // Range query
        if (Configuration.getString(prefix + ".rq").equals("seq")) {
            currentRangeQuery = PhtMessage.SEQ_QUERY;
        } else {
            currentRangeQuery = PhtMessage.PAR_QUERY;
        }

        this.state = PHT_INIT;
        phtid      = CommonState.getPid();
        this.nodes = new HashMap<>();

        // Tests
        allPhtNodes = new HashMap<>();

        // Stop when a dht routing has occurred ?
        stopOnRouteFail = Configuration.getBoolean(prefix + ".routeFail");

        // Logs
        if (logValue.equals("on")) {
            logWriter = new BufferedWriter(new FileWriter("phtprotocol.log", false));
        }
    }

    /* _________________________________     ________________________________ */
    /* _________________________________ API ________________________________ */

    /**
     * Store the data to be inserted, change the state of the PhtProtocol to
     * INSERTION1 and starts a lookup.
     * @param key The key of the data we want to insert
     * @param data PhtData to be inserted
     * @return The Id of the request
     */
    public long insertion(String key, Object data) {
        if (this.state != PHT_INIT) {
            return -(this.state);
        } else if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
        this.currentData = data;
        this.state       = PHT_INSERTION1;

        log(String.format("((%d)) insertion (%s, %s)    [%d]\n",
                nextId, key, data, this.node.getID()));

        query(key, PhtMessage.INSERTION, nextId);
        return nextId;
    }

    /**
     * Send a suppression request to remove the data associated with the given
     * key from the network
     * @param key Key of the data to remove
     * @return The Id of the request
     */
    public long suppression(String key) {
        if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
        log( String.format("((%d)) suppression (%s)    [%d]\n",
                nextId, key, this.node.getID()) );

        query(key, PhtMessage.SUPRESSION, nextId);
        return nextId;
    }

    /**
     * Launch an exact search for the given key
     * @param key Key to search
     * @return The Id of the request
     */
    public long query(String key) {
        if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
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
     * @return The Id of the query
     */
    public long rangeQuery(String keyMin, String keyMax) {
        if (! PhtProtocol.init) {
            return -1;
        }

        nextId++;
        this.rqTotal = 0;
        this.rqCount = 0;

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

        pml     = new PMLookup(key, operation, null, startKey);
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

        pmrq    = new PMRangeQuery(keyMin, keyMax);
        pml     = new PMLookup(startKey, currentRangeQuery, null, startLookup, pmrq);
        message = new PhtMessage(currentLookup, this.node, null, id, pml);

        dht.send(message, startLookup);
    }

    /* _______________________________          _____________________________ */
    /* _______________________________ Initiate _____________________________ */

    /**
     * Call sendInit during PeerSim's initialization phase to create the
     * PhtNode root.
     */
    public void sendInit(String recipient) {
        PhtMessage message = new PhtMessage(PhtMessage.INIT, this.node, "", 0, null);
        this.dht.send(message, recipient);
    }

    /* _____________________                              ___________________ */
    /* _____________________ Message demands from PhtNode ___________________ */

    /**
     * Send a split request to the node (PeerSim) responsible for the 'son' key
     * @param label Father's label
     * @param son Son's label
     */
    public void sendSplit(String label, String son) {
        PhtMessage message;
        PMLookup pml;

        nextId++;
        pml = new PMLookup(label, PhtMessage.SPLIT, null, son);
        message = new PhtMessage(PhtMessage.SPLIT, this.node, label, nextId, pml);

        log(String.format("((%d)) sendSplit [initiator: '%s' on node %d][son: '%s']\n",
                message.getId(), label, this.node.getID(), son));

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
     * Send a merge message to the node (PeerSim) responsible for the 'son' key
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
     * Send a no merge message to a son, informing him that the merge process
     * does not continue.
     * @param dest son
     * @param father father's label
     */
    private void sendNoMerge (NodeInfo dest, String father) {
        PhtMessage message;

        nextId++;
        message = new PhtMessage(PhtMessage.NO_MERGE, this.node, father, nextId, null);

        log(String.format("((%d)) sendNoMerge from '%s'(%d) to '%s'(%d)\n",
                nextId, father, this.node.getID(), dest.getKey(), dest.getNode().getID()));

        EDSimulator.add(delay(), message, dest.getNode(), phtid);
    }

    /**
     * Send a message to a PhtNode's father telling to increment or decrement
     * (depending on the value of inc) the number of keys his has in his
     * subtrees. This method is a called after an insertion or a suppression
     * @param label Label of the son
     * @param father Label of the father
     * @param inc Increment or Decrement
     * @throws InitiatorException
     */
    public void sendUpdateNbKeys(String label, NodeInfo father, boolean inc) throws InitiatorException {
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
            throw new InitiatorException(message.getId() + " sendUpdateNbKeys");
        }


        EDSimulator.add(delay(), message, father.getNode(), phtid);
    }

    /**
     * Inform the initiator that he must try again this operation.
     * @param initiator Initiator of the operation
     * @param id Message id
     * @param label Label of the PhtNode who called this method
     * @param state state of the PhtNode which rejected the operation
     * @param isLeaf rejected by a leaf
     * @param op Operation to retry
     */
    private void sendRetry (Node initiator, long id, String label,
                            PhtNodeState state, boolean isLeaf, int op) {
        PhtMessage message;

        log( String.format("\n[sendRetry] initiator: %d <> id: %d <> label: '%s'"
                        + "[node's state: %s][isLeaf: %b][op: %d]\n\n",
                initiator.getID(), id, label, state, isLeaf, op) );

        message = new PhtMessage(PhtMessage.RETRY, this.node, label, id, op);
        EDSimulator.add(MAX_DELAY * RETRY_FACTOR, message, initiator, phtid);
    }

    /* __________________________                   _________________________ */
    /* __________________________ Message reception _________________________ */

    /**
     * Inform the client that a non stable Node or PhtNode has been met.
     * @param message Message with the id the client needs.
     */
    private void processRetry (PhtMessage message) {
        int op = 0;

        log( String.format("((%d)) retry\n", message.getId()) );
        System.out.println(String.format("((%d)) retry\n", message.getId()));

        if (message.getMore() instanceof Integer) {
            op = (Integer) message.getMore();
        }

        this.state = PHT_INIT;
        stats.curr().incRetry(op);

        client.responseOk(message.getId(), PhtMessage.RETRY);
    }

    /**
     * If the node responsible for the key is found: ACK.
     * First step: get the PhtNode. Unless the operation is a range query,
     * keep searching while the PhtNode is not a leaf (using the lookup
     * information inside the two parameters).
     * @param message Query message
     * @param pml extra information
     * @throws PhtNodeNotFoundException
     */
    private void processLinLookup (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException, InitiatorException {
        NodeInfo next;
        PhtNode node;

        // Get the node
        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            log( String.format("((%d)) processLinLookup node null :: label: '%s' [%d]\n",
                    message.getId(), pml.getDestLabel(), this.node.getID()) );

            if (! pml.getDestLabel().equals("")) {
                String father = PhtUtil.father(pml.getDestLabel());

                message.setType(PhtMessage.BACK_LIN_LOOKUP);
                pml.setDestLabel(father);
                EDSimulator.add(MAX_DELAY * RETRY_FACTOR, message, this.node, phtid);
                return;
            } else {
                throw new PhtNodeNotFoundException(
                        String.format("((%d)) processLinLookup, label: '%s'\n",
                                message.getId(), pml.getDestLabel())
                );
            }

        }

        // Statistics
        node.use();

        /*
         * If the operation is an insertion or a suppression and 'node' is
         * not in a stable state do not continue.
         */
        if ( (! node.state.isStable()) &&
                ((pml.getOperation() == PhtMessage.INSERTION) ||
                        (pml.getOperation() == PhtMessage.SUPRESSION)) ) {
            sendRetry(message.getInitiator(), message.getId(), node.getLabel(),
                    node.state, node.isLeaf(), pml.getOperation());
            return;
        }

        // Process parallel range queries in a different method.
        if (pml.getOperation() == PhtMessage.PAR_QUERY) {
            if ( (node.isLeaf()) || (node.getLabel().equals(pml.getKey())) ) {
                processParQuery(message, pml);
                return;
            }
        }

        // Continue the lookup if the node is not a leaf
        if (! node.isLeaf()) {
            next = forwardLookup(node, pml.getKey());
            pml.setDestLabel(next.getKey());
            pml.setDest(next.getNode());

            EDSimulator.add(delay(), message, pml.getDest(), phtid);
            return;
        }

        log(String.format("((%d)) <leaf> processLinLookup :: node %d" +
                        " :: node's label: '%s' :: key: '%s' :: op: %d\n",
                message.getId(), this.node.getID(),
                node.getLabel(), pml.getKey(), pml.getOperation()));

        // If it is a leaf, the action depends on the underlying operation
        switch (pml.getOperation()) {
            case PhtMessage.SUPRESSION:
                message.setMore(node.remove(pml.getKey()));
                message.setType( PhtMessage.ACK_SUPRESSION );
                break;

            case PhtMessage.LIN_LOOKUP:
                pml.setLess( node.get(pml.getKey()) );
                message.setType( PhtMessage.ACK_LIN_LOOKUP );
                break;

            case PhtMessage.INSERTION:
                message.setType(PhtMessage.ACK_LIN_LOOKUP);
                break;

            case PhtMessage.SEQ_QUERY:
                processSeqQuery(message, pml);
                return;
        }

        // Statistics
        node.useDest();
        this.usageDest++;
        stats.curr().incLookup(currentLookup);

        pml.setDest(this.node);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /**
     * Useful if a whole merge process happens during the transmission of a
     * lookup from an internal node (which became a leaf) and one of its sons
     * (which disappeared). This should be very rare.
     * The methods only tries to find a leaf and then continues like a regular
     * linear lookup (call to processLinLookup).
     * @param message message
     * @param pml Label
     * @throws PhtNodeNotFoundException
     */
    private void processBackwardLinLookup (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        String label;
        PhtNode node;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);
        if (node == null) {
            // Test if the node do exist and we landed on the wrong
            // PhtProtocol (because of an incorrect dht routing)
            testNullNode(pml.getDestLabel(), message);

            if (! label.equals("")) {
                String father = PhtUtil.father(label);
                if (father == null) {
                    throw new PhtNodeNotFoundException(
                            String.format("((%d)) processBackwardLinLookup, "
                                            + "label: '%s' <> father null\n",
                                    message.getId(), pml.getDestLabel())
                    );
                }
                pml.setDestLabel(father);
                this.dht.send(message, father);

            } else {
                throw new PhtNodeNotFoundException(
                        String.format("((%d)) processBackwardLinLookup, label: '%s' [%d]\n",
                                message.getId(), pml.getDestLabel(), this.node.getID())
                );
            }

        } else {
            message.setType(PhtMessage.LIN_LOOKUP);
            EDSimulator.add(MAX_DELAY * RETRY_FACTOR, message, this.node, phtid);
        }
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

        log(String.format("((%d)) processSeqQuery <> keyMin: %s <> keyMax: %s <> end: %s\n",
                message.getId(), pml.getKey(), pmrq.getKeyMax(), pmrq.isEnd()));

        // Stop the range query here ?
        if (node.getNextLeaf().getNode() != null) {
            if (! PhtUtil.inRangeMax(node.getNextLeaf().getKey(), pmrq.getKeyMax()) ) {
                pmrq.stop();
            }
        } else {
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

        // Forward the request to the next leaf
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

        // Statistics
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
     * Insert the data into the leaf.
     * @param message Message containing the key and the data.
     * @param pml
     * @throws PhtNodeNotFoundException, InitiatorException
     */
    private void processInsertion(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException, InitiatorException {
        int res;
        PhtNode node;

        log(String.format("((%d)) processInsertion [initiator: '%s'][type: %d] "
                        + "[op: %d][key: '%s']    [%d]\n",
                message.getId(), message.getInitiator().getID(), message.getType(),
                pml.getOperation(), pml.getKey(), this.node.getID()));
        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processInsertion");
        }

        // Statistics
        node.use();
        node.useDest();
        this.usageDest++;

        res = node.insert(pml.getKey(), pml.getLess());

        if (message.getInitiator() == null) {
            throw new InitiatorException(message.getId() + " processInsertion");
        }

        message.setType(PhtMessage.ACK_INSERTION);
        message.setMore(res);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }


    /* :::::::::: SPLIT ::::::::: */

    /**
     * Method called by processLinLookup because we are not searching for a
     * PhtNode, we want to create a new one.
     * @param message Message with the father's node (PeerSim)
     * @param pml The son's label
     */
    private void processSplit(PhtMessage message, PMLookup pml) throws SplitException, InitiatorException {
        String label;
        PhtNode node;
        NodeInfo ni;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);

        if (node != null) {
            throw new SplitException( String.format("((%d)) processSplit <> '%s'\n",
                    message.getId(), node.getLabel()) );
        }

        node = new PhtNode(label, this);
        ni   = new NodeInfo(message.getInitiatorLabel(), message.getInitiator());

        node.setFather(ni);
        node.state.unstable();
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
            throw new InitiatorException(message.getId() + " processSplit");
        }

        /*
         * Used for tests: every PhtNode is registered into a static Map.
         * Using this Map we are able to detect a wrong routing from the
         * underlying Dht.
         */
        allPhtNodes.put(label, this.node.getID());

        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /**
     * Get new previous and next leaf. Ack to the father to get data.
     * @param message Message with next and previous leaves
     * @throws PhtNodeNotFoundException,
     * @throws SplitException
     * @throws InitiatorException
     */
    private void processSplitLeaves(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException, SplitException, InitiatorException {
        String label;
        PhtNode node = null;
        NodeInfo[]  info;

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

        if (pml.getLess() instanceof NodeInfo[]) {

            // Retrieve the data an pass it to the son
            info = (NodeInfo[]) pml.getLess();

            /*
             * Updates may have arrived before this message.
             *
             * For example the previous leaf could have send an update message
             * to this node's father (it does not known that a split is going
             * on) which has forwarded the update to this leaf. Therefore,
             * this node should not accept the previous leaf sent from his
             * father, because it would override the correct one.
             */
            if (node.getPrevLeaf().getNode() == null) {
                node.setPrevLeaf(info[NodeInfo.ARRAY_PREVLEAF]);
            }
            if (node.getNextLeaf().getNode() == null) {
                node.setNextLeaf(info[NodeInfo.ARRAY_NEXTLEAF]);
            }

            log(String.format("((%d)) processSplitLeaves [initiator: '%s' <> '%s'][type: %d] [label: '%s']\n",
                    message.getId(), message.getInitiator().getID(), message.getInitiatorLabel(),
                    message.getType(), node.getLabel()));

        } else {
            throw new SplitException("processSplitLeaves <> " + pml.getLess().getClass().getName()
                    + "' <> Initiator's label: '" + message.getInitiatorLabel() + "'");
        }

        pml.setLess(true);
        message.setType(PhtMessage.ACK_SPLIT_LEAVES);

        if (message.getInitiator() == null) {
            throw new InitiatorException(message.getId() + " processSplitLeaves");
        }

        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /**
     * Retrieve the data from the message and insert it into the son
     * @param message Message with all the information
     * @throws PhtNodeNotFoundException
     * @throws PhtNodeNotFoundException, SplitException
     */
    private void processSplitData(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException, SplitException, InitiatorException {
        String label;
        PhtNode node;
        List<PhtData> data;

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

            // Retrieve the data an pass it to the son
            data = (List<PhtData>) pml.getLess();
            pml.setLess(node.insert(data));

            log(String.format("((%d)) processSplitData [initiator: %d <> '%s'][type: %d] "
                            + "[destLabel: '%s'][keys received: %d]    [%d]\n",
                    message.getId(),
                    message.getInitiator().getID(), message.getInitiatorLabel(),
                    message.getType(),
                    label, data.size(), this.node.getID()));
        } else {
            throw new SplitException("processSplitData <> "
                    + pml.getLess().getClass().getName()
                    + " <> pml.getDestLabel: '" + pml.getDestLabel()
                    + "' <> pml.getKey: '" + pml.getKey()
                    + "' <> message.getInitiatorLabel: '" + message.getInitiatorLabel() + "'");
        }

        message.setType(PhtMessage.ACK_SPLIT_DATA);

        if (message.getInitiator() == null) {
            throw new InitiatorException(message.getId() + " ProcessSplitData");
        }

        EDSimulator.add(delay(), message, message.getInitiator(), phtid);

        /*
         * Tell the previous leaf I am his new next leaf, and my next leaf that
         * I am his new previous leaf.
         */
        node.state.startThreaded();
        if (node.getLabel().endsWith("0")) {
            if (node.getPrevLeaf().getNode() != null) {
                startUpdateLeavesMerge(
                        message.getId(),
                        PhtMessage.UPDATE_NEXT_LEAF,
                        node.getLabel(),
                        node.getPrevLeaf()
                );
            } else {
                node.state.ackPrevLeaf();
            }

            node.state.ackNextLeaf();
        } else {
            if (node.getNextLeaf().getNode() != null) {
                startUpdateLeavesMerge(
                        message.getId(),
                        PhtMessage.UPDATE_PREV_LEAF,
                        node.getLabel(),
                        node.getNextLeaf()
                );
            } else {
                node.state.ackNextLeaf();
            }

            node.state.ackPrevLeaf();
        }

        node.state.reset();
    }


    /* :::::::::: MERGE ::::::::: */

    /**
     * Send a son's keys and data to his father.
     * This is the first step of a merge process (for to son).
     * @param message Message with all the information needed
     * @param pml PMLookup that was inside that message that has already been
     *            extracted in the processEvent method
     * @throws SplitException
     */
    private void processMerge(PhtMessage message, PMLookup pml) throws SplitException {
        String label;
        PhtNode node;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);
        if (node == null) {
            throw new SplitException("processMerge node null\n"
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey());
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
     * @throws SplitException
     */
    private void processMergeLeaves(PhtMessage message, PMLookup pml) throws SplitException {
        String label;
        PhtNode node;
        NodeInfo leaf;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);
        if (node == null) {
            throw new SplitException("processMergeLeaves <> '" + label + "' ");
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

    private void processMergeData(PhtMessage message, PMLookup pml) throws SplitException {
        String label;
        PhtNode node;
        List<PhtData> kdata;

        label = pml.getDestLabel();
        node = this.nodes.get(label);
        if (node == null) {
            throw new SplitException("processMergeData <> '" + label + "' ");
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
     * @throws SplitException
     */
    private void processMergeDone(PhtMessage message, PMLookup pml) throws SplitException {
        String label;
        PhtNode node;

        label = pml.getDestLabel();
        node  = this.nodes.get(label);

        /* If the node does not exists, there is a problem */
        if (node == null) {
            System.out.println(String.format("((%d)) processMergeDone node null [initiator: '%s' on %d]\n",
                    message.getId(),
                    message.getInitiatorLabel(), message.getInitiator().getID()));
            throw new SplitException("processMergeDone node null\n"
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey());
        }

        log(String.format("((%d)) processMergeDone [node: '%s'][initiator: '%s' on %d]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID()));

        if (this.nodes.remove(pml.getDestLabel()) == null ) {
            log( String.format("((%d)) processMergeDone PhtNode '%s' remove -> null\n",
                    message.getId(), node.getLabel()) );
        }
        node.clear();

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
        PhtNode node;

        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processUpdateNbKeys "
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: "   + pml.getKey());
        }

        node.use();
        node.useDest();
        this.usageDest++;

        update = message.getType() - PhtMessage.UPDATE_NBKEYS;
        node.updateNbKeys(update);

        if (pml.getLess() instanceof Boolean) {
            ok = (Boolean) pml.getLess();
        }

        /*
         * ok: the update number of keys message is received for the first
         * time by a PhtNode.
         * update < 0: decrement the number of keys in the subtree.
         */
        if (ok && (update < 0)) {

            /*
             * Condition to trigger a merge operation:
             * 1/ less than B+1 keys inside the subtree,
             * 2/ the PhtNode receiving this message must be an internal one,
             * 3/ this PhtNode must be stable.
             *
             * (the two 'if' could be merged into one, but it might be a little
             * bit harder to read)
             */
            if ( (node.getNbKeys() < PhtProtocol.B+1)
                    && (node.state.isStable())
                    && (! node.isLeaf()) ) {
                log( String.format("((%d)) updateNbkeys from '%s' to '%s' and '%s'\n",
                        message.getId(),
                        node.getLabel(),
                        node.getLson().getKey(),
                        node.getRson().getKey()) );
                node.state.startMerge();
                sendMerge(node.getLabel(), node.getLson());
                sendMerge(node.getLabel(), node.getRson());
            }
        } else if (!ok && (update < 0) && (node.getNbKeys() < PhtProtocol.B+1)) {
            // Statistics: count the number of merge operation avoided
            stats.curr().incMergeAvoid(message.getId());
        }

        // Forward the message to the father
        if (! node.getLabel().equals("")) {
            pml.setLess(false);
            pml.setDest(node.getFather().getNode());
            pml.setDestLabel(node.getFather().getKey());
            message.setInitiatorLabel(node.getLabel());
            message.setInitiator(this.node);
            EDSimulator.add(delay(), message, node.getFather().getNode(),phtid);
        }
    }

    /**
     * Update a PhtNode's previous leaf
     * @param message PhtMessage with information for the logs
     * @param pml Has the new previous leaf
     * @throws PhtNodeNotFoundException
     */
    private void processUpdatePreviousLeaf(PhtMessage message, PMLookup pml) {
        NodeInfo leaf;
        PhtNode node;

        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            /*
             * The destination leaf can have merged, but the sender of the
             * update may not have received the information when it send it.
             * Therefore, we must route the message to the father of the merged
             * PhtNode.
             */
            updateLeavesToFather(message, pml);
            return;
        } else if ( (! node.isLeaf())
                && (node.state.getState() < PhtNodeState.ACK_MERGE_LEAVES) ) {
            /*
             * Same idea as the previous comment but with a split. This
             * scenario is better in a way because it does not involve a dht
             * routing (which takes more time than a direct communication).
             */
            updateLeavesToSon(message, pml, node.getLson(), "0");
        }

        node.use();
        node.useDest();
        this.usageDest++;

        log(String.format("((%d)) [%s] processUpdatePreviousLeaf: Node found !"
                        + " (initiator: '%s')\n",
                message.getId(),
                node.getLabel(),
                message.getInitiatorLabel()));

        if (pml.getLess() instanceof NodeInfo) {
            leaf = (NodeInfo) pml.getLess();
            node.setPrevLeaf(leaf);
        }

        pml.setDest(this.node);
        pml.setDestLabel(node.getLabel());
        message.setType(PhtMessage.ACK_UPDATE_PREV_LEAF);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /**
     * Update a PhtNode's next leaf
     * @param message Just for the log
     * @param pml Contains the next leaf
     * TODO: update comments
     */
    private void processUpdateNextLeaf(PhtMessage message, PMLookup pml) {
        NodeInfo leaf;
        PhtNode node;

        node = this.nodes.get(pml.getDestLabel());
        if (node == null) {
            /*
             * The destination leaf can have merged, but the sender of the
             * update may not have received the information when it send it.
             * Therefore, we must route the message to the father of the merged
             * PhtNode.
             */
            updateLeavesToFather(message, pml);
            return;
        } else if ( (! node.isLeaf())
                && (node.state.getState() < PhtNodeState.ACK_MERGE_LEAVES) ) {
            /*
             * Same idea as the previous comment but with a split. This
             * scenario is better in a way because it does not involve a dht
             * routing (which takes more time than a direct communication).
             */
            updateLeavesToSon(message, pml, node.getRson(), "1");
        }

        node.use();
        node.useDest();
        this.usageDest++;

        log(String.format("((%d)) [%s] processUpdateNextLeaf: Node found ! (initiator: '%s')\n",
                message.getId(), node.getLabel(), message.getInitiatorLabel()));

        if (pml.getLess() instanceof NodeInfo) {
            leaf = (NodeInfo) pml.getLess();
            node.setNextLeaf(leaf);
        }

        pml.setDest(this.node);
        pml.setDestLabel(node.getLabel());
        message.setType(PhtMessage.ACK_UPDATE_NEXT_LEAF);
        EDSimulator.add(delay(), message, message.getInitiator(), phtid);
    }

    /* ___________________________               ____________________________ */
    /* ___________________________ Ack reception ____________________________ */

    /**
     * Nothing to do after for two time requests (suppression, queries...).
     * If there is something to do after the lookup, start it.
     * @param message Request message
     * @param pml More information (extracted from message)
     * @throws WrongStateException
     */
    private void processAck_LinLookup(PhtMessage message, PMLookup pml)
            throws
            WrongStateException {

        log(String.format("((%d)) processAck_LinLookup [initiator: '%s'][type: %d] "
                        + "[op: %d][key: '%s']    [%d]\n",
                message.getId(), message.getInitiator().getID(), message.getType(),
                pml.getOperation(), pml.getKey(), this.node.getID()));

        // Next step
        switch (pml.getOperation()) {
            case PhtMessage.INSERTION:
                message.setType(PhtMessage.INSERTION);
                pml.setLess(this.currentData);

                if (this.state != PHT_INSERTION1) {
                    throw new WrongStateException(
                            String.format("((%d)) processAck_LinLookup %d on node %d :: state: %d",
                                    message.getId(), this.state, this.node.getID(), this.state));
                } else {
                    this.state = PHT_INSERTION2;
                }
                break;

            case PhtMessage.LIN_LOOKUP:
                long id = message.getId();
                client.responseValue(id, pml.getKey(), pml.getLess());
                return;

            default:
                break;
        }

        if (pml.getDest() == null) {
            log("@@@@@ dest null in processAck_LinLookup @@@@@");
        }

        EDSimulator.add(delay(), message, pml.getDest(), phtid);
    }

    /**
     * <p>The initiator of a sequential query calls this method when it receive
     * an ACK from a leaf. The keys and data are passed to the client and the
     * two range query counters are updated:</p>
     * <ol>
     *   <li>rqCount (number of ACK received)</li>
     *   <li>rqTotal (number of leaves in the range query = number of ACK the
     * initiator must received before it can tell the client the query is
     * done)</li>
     *   </ol>
     * @param message Keys and data send from the leaf
     * @param pml More information (extracted from message)
     */
    private void processAck_SeqQuery(PhtMessage message, PMLookup pml) {
        PMRangeQuery pmrq;

        if (pml.getLess() instanceof PMRangeQuery) {
            pmrq = (PMRangeQuery) pml.getLess();
        } else {
            log(String.format("((%d)) processAck_SeqQuery pml.getLess() instanceof %s",
                    message.getId(), pml.getLess().getClass().getName()));
            return;
        }

        if (pmrq.isEnd()) {
            this.rqTotal = pmrq.getCount();
        }

        this.rqCount++;
        if (this.rqCount == this.rqTotal) {
            log(String.format("::processAck_SeqQuery:: total: %d OK '%s' to '%s'\n",
                    this.rqTotal, pmrq.getKeyMin(), pmrq.getKeyMax()));

        }

        client.responseList(message.getId(), pmrq.getKdata(), this.rqCount == this.rqTotal);
        log(String.format("((%d)) processAck_SeqQuery <> %d keys\n",
                message.getId(), pmrq.getCount()));
    }

    /**
     * <p>A leaf send an ACK to its father during a parallel query.
     * This father sends an ACK to his father, until the starting PhtNode is
     * reached (the one with the smallest common prefix who started the
     * parallel query).</p>
     *
     * <p>Since an internal node must wait an ACK from it two
     * sons, the first ACK is stored and restored when the second arrives.</p>
     *
     * <p>The number of leaves is the sum of the number of leaves in his
     * left subtree and its right subtree.</p>
     * @param message Information we need
     * @param pml Contains the counter (number of leaves)
     * @throws PhtNodeNotFoundException
     */
    private void processAck_ParQuery (PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException {
        int count;
        NodeInfo next;
        PhtNode node;

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
         * (this could be improved)
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
                EDSimulator.add(delay(), message, message.getInitiator(), phtid);
            } else {
                // ACK to the father
                pml.setLess(storedCount + count);
                pml.setDest(next.getNode());
                pml.setDestLabel(next.getKey());
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

        client.responseList(message.getId(), pmrq.getKdata(), pmrq.isEnd());

        this.rqCount++;
        if (this.rqCount == this.rqTotal) {
            log(String.format("::processAck_ParQueryClient:: total: %d OK '%s' to '%s'\n",
                    this.rqTotal, pmrq.getKeyMin(), pmrq.getKeyMax()));

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

        // Statistics
        stats.curr().incInsert();

        this.state = PHT_INIT;
        client.responseOk(id, res);
    }

    /**
     * Tell the client if the suppression has been done.
     * @param message Message to process
     * @throws BadAckException
     */
    private void processAck_Suppression (PhtMessage message)
            throws BadAckException {
        boolean ok;
        long id;
        int res;

        id = message.getId();

        if (message.getMore() instanceof Boolean) {
            ok = (Boolean) message.getMore();
        } else {
            throw new BadAckException("processAck_Suppression" + message.getMore().getClass().getName());
        }

        if (ok) {
            res = 0;
        } else {
            res = -1;
        }

        // Statistics
        stats.curr().incDelete();

        client.responseOk(id, res);
        System.out.println("Ack_Suppression res: " + res);
    }

    /* :::::::::: SPLIT ::::::::: */

    /**
     * Tell the father if his son split well.
     * Update his lson/rson field with the son's label and node (PeerSim)
     * @param message Message to process
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     */
    private void processAck_Split(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException,
            BadAckException {
        boolean ok = false;
        PhtNode node;

        if (pml.getLess() instanceof  Boolean) {
            ok = (Boolean) pml.getLess();
        }

        log(String.format("((%d)) processAck_Split [initiator: '%s' <> '%s'][type: %d] "
                        + "[label: '%s'][ok: %s]    [%d]\n",
                message.getId(),
                message.getInitiator().getID(), message.getInitiatorLabel(),
                message.getType(),
                pml.getDestLabel(), pml.getLess().toString(), this.node.getID()));

        if (! ok) {
            throw new BadAckException("processAck_Split: pml.getLess() -> false ");
        }

        // Get the father
        node = nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_Split <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey());
        }

        node.use();
        node.useDest();
        this.usageDest++;

        String label = node.getLabel();
        NodeInfo[] info = new NodeInfo[2];

        // Get the leaves for the left and right sons
        if (pml.getDestLabel().charAt(label.length()) == '0') {
            info[NodeInfo.ARRAY_PREVLEAF] = ( node.getPrevLeaf() );
            info[NodeInfo.ARRAY_NEXTLEAF] = ( node.getRson() );

            if (! node.state.ackSplitLson() ) {
                interrupt();
            }
            node.setLson( new NodeInfo(pml.getDestLabel(), pml.getDest()) );
        } else {
            info[NodeInfo.ARRAY_PREVLEAF] = ( node.getLson() );
            info[NodeInfo.ARRAY_NEXTLEAF] = ( node.getNextLeaf() );

            if (! node.state.ackSplitRson() ) {
                interrupt();
            }
            node.setRson( new NodeInfo(pml.getDestLabel(), pml.getDest()) );
        }

        pml.setLess(info);
        message.setType(PhtMessage.SPLIT_LEAVES);

        // Store the first ACK and restore it when the second arrives.
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

            // Send to the left son first
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
    private void processAck_SplitLeaves (PhtMessage message, PMLookup pml)
            throws BadAckException,
            PhtNodeNotFoundException {
        boolean ok = false;
        if (pml.getLess() instanceof  Boolean) {
            ok = (Boolean) pml.getLess();
        }

        log(String.format("((%d)) processAck_SplitLeaves [initiator: '%s' <> '%s'][type: %d] "
                        + "[label: '%s'][ok: %s]    [%d]\n",
                message.getId(),
                message.getInitiator().getID(), message.getInitiatorLabel(),
                message.getType(),
                pml.getDestLabel(), pml.getLess().toString(), this.node.getID()));

        if (! ok) {
            throw new BadAckException("processAck_SplitLeaves: pml.getLess() -> false ");
        }

        PhtNode node =nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_SplitLeaves "
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: " + pml.getKey());
        }

        node.use();
        node.useDest();
        this.usageDest++;

        String label = node.getLabel();
        List<PhtData> data;

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

        pml.setLess(data);
        message.setType(PhtMessage.SPLIT_DATA);
        if (pml.getDest() == null) {
            log("@@@@@ dest null in processAck_SplitLeaves @@@@@");
        }

        /*
         * Before we can start the split_data phase, the node must have
         * received two ACK from the split phase. If not, wait for the other
         * split ACK to arrive before moving forward. This means we add the
         * same event to the simulator.
         */
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
     * @throws PhtNodeNotFoundException
     */
    private void processAck_SplitData(PhtMessage message, PMLookup pml)
            throws BadAckException,
            PhtNodeNotFoundException {
        boolean ok = false;
        PhtNode node;

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

        node = nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_SplitData "
                    + " <> label: " + pml.getDestLabel() + "\n"
                    + " <> key: " + pml.getKey());
        }

        node.use();
        node.useDest();
        this.usageDest++;

        String label = pml.getDestLabel();

        // Change the node's state
        if ( label.charAt( label.length()-1 ) == '0' ) {
            if (! node.state.ackSDataLson() ) {
                throw new PhtNodeNotFoundException("processAck_SplitData ackSDataLson fail\n"
                        + " <> label: " + pml.getDestLabel() + "\n"
                        + " <> key: " + pml.getKey());
            }
        } else {
            if (! node.state.ackSDataRson() ) {
                throw new PhtNodeNotFoundException("processAck_SplitData ackSDataRson fail\n"
                        + " <> label: " + pml.getDestLabel() + "\n"
                        + " <> key: " + pml.getKey());
            }
        }

        if (node.state.isStable()) {
            node.internal();
            System.out.println(">splitok");
            log( String.format("splitok ((%d)) '%s' [%d] :: lson: '%s' [%d] :: rson: '%s' [%d]\n",
                    message.getId(),
                    node.getLabel(), this.node.getID(),
                    node.getLson().getKey(), node.getLson().getNode().getID(),
                    node.getRson().getKey(), node.getRson().getNode().getID()) );

            client.splitOk();
       } else {
            System.out.println(">splitNotOk");
            log( String.format("((%d)) not stable : %s\n", message.getId(), node.state.toString()) );
        }
    }

    /* :::::::::: MERGE ::::::::: */

    /**
     * Get the data send from the son and insert them into the initiator
     * PhtNode.
     * @param message Message with all the information needed
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     */
    private void processAck_Merge(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException,
            BadAckException {
        boolean continueMerge;
        PhtNode node;
        NodeInfo ni;

        if (pml.getLess() instanceof Boolean) {
            continueMerge = (Boolean) pml.getLess();
        } else {
            throw new BadAckException("processAck_Merge: pml.getLess() -> false ");
        }

        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            StringBuilder sb = new StringBuilder(this.nodes.size());

            for (PhtNode nd: this.nodes.values()) {
                sb.append( nd.toString() );
            }

            throw new PhtNodeNotFoundException("processAck_Merge "
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: " + pml.getKey());
        }

        log(String.format("((%d)) processAck_Merge [node: '%s'][initiator: '%s' on %d] "
                        + "[continueMerge: %b]\n",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID(),
                continueMerge));

        if (continueMerge) {
            if (pml.getDestLabel().endsWith("0")) {
                node.state.ackMergeLSon();
            } else {
                node.state.ackMergeRSon();
            }
        } else {
            if (pml.getDestLabel().endsWith("0")) {
                node.state.noMergeLson();
            } else {
                node.state.noMergeRson();
            }
        }

        /*
         * Wait for the two ACK to arrive before moving to the next step
         * (MERGE_LEAVES).
         */
        if (node.state.ackMerge()) {
            PhtMessage restoredMessage = node.returnMessage();
            PMLookup restoredPml;

            if (restoredMessage == null) {
                return;
            } else {
                if (restoredMessage.getMore() instanceof PMLookup) {
                    restoredPml = (PMLookup) restoredMessage.getMore();
                } else {
                    return;
                }
            }

            log(String.format("((%d)) processAck_Merge [initiator: '%s' on %d] "
                            + "[destLabel: '%s']\n",
                    message.getId(),
                    node.getLabel(), this.node.getID(),
                    pml.getDestLabel()));

            restoredMessage.setType(PhtMessage.MERGE_LEAVES);
            message.setType(PhtMessage.MERGE_LEAVES);

            EDSimulator.add(delay(), message, pml.getDest(), phtid);
            EDSimulator.add(delay(), restoredMessage, restoredPml.getDest(), phtid);

        } else if (node.state.noMerge()) {

            if (continueMerge) {
                if (node.getLabel().endsWith("0")) {
                    ni = node.getLson();
                } else {
                    ni = node.getRson();
                }
            } else {
                if (node.getLabel().endsWith("0")) {
                    ni = node.getRson();
                } else {
                    ni = node.getLson();
                }
            }

            sendNoMerge (ni, node.getLabel());

            log(String.format("((%d)) processAck_Merge [node: '%s'][initiator: '%s' on %d] "
                            + "[send no merge to: '%s']\n",
                    message.getId(), node.getLabel(),
                    message.getInitiatorLabel(), message.getInitiator().getID(),
                    ni.getKey()));

            /*
             * If a message has been stored before (eg. this message arrives
             * after an ACK_MERGE with continueMerge true), remove it: it
             * could cause trouble for the following operations.
             */
            node.returnMessage();
            node.state.stopMerge();

        } else {
            if (! node.storeMessage(message) ) {
                System.out.printf("((%d)) processAck_Merge :: storeMessage false\n",
                        message.getId());
            }
        }
    }


    /**
     * Get the leaf send from the son, if it is from his left son, it is a
     * previous leaf, a next leaf otherwise
     * @param message Message with all the information needed
     * @throws PhtNodeNotFoundException
     * @throws BadAckException
     */
    private void processAck_MergeLeaves(PhtMessage message, PMLookup pml)
            throws PhtNodeNotFoundException,
            BadAckException {
        PhtNode node;
        String label;
        NodeInfo leaf;

        if (pml.getLess() instanceof NodeInfo) {
            leaf = (NodeInfo) pml.getLess();
        } else {
            throw new BadAckException("processAck_MergeLeaves: pml.getLess() -> false ");
        }

        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_MergeLeaves "
                    + " <> label: " + pml.getDestLabel()
                    + " <> key: " + pml.getKey());
        }

        /*
         * Updates may have arrived before this message.
         *
         * For example the previous leaf could have send an update message
         * to this node's father (it does not known that a split is going
         * on) which has forwarded the update to this leaf. Therefore,
         * this node should not accept the previous leaf send from his
         * father, because it would override the correct one.
         */
        label = pml.getDestLabel();
        if (label.charAt( label.length()-1 ) == '0') {
            if (node.getPrevLeaf().getNode() == null) {
                node.setPrevLeaf(leaf);
            }
            node.state.ackMergeLeavesLson();
        } else {
            if (node.getNextLeaf().getNode() == null) {
                node.setNextLeaf(leaf);
            }
            node.state.ackMergeLeavesRson();
        }

        log(String.format("((%d)) processAck_MergeLeaves [node: '%s'][initiator: '%s' on %d] "
                        + "[node's state: %s] ",
                message.getId(), node.getLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID(),
                node.state));

        if ( (node.getPrevLeaf().getNode() != null)
                && (node.getNextLeaf().getNode() != null) ) {
            log(String.format("prev: '%s' <> next: '%s'\n",
                    node.getPrevLeaf().getKey(),
                    node.getNextLeaf().getKey()));
        }

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

        log(String.format("((%d)) processAck_MergeData [node: '%s'][initiator: '%s' on %d] "
                        + "[node's state: %s]\n",
                message.getId(), pml.getDestLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID(),
                father.state));

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
     */
    private void processAck_MergeDone(PhtMessage message, PMLookup pml)
            throws BadAckException,
            PhtNodeNotFoundException {
        PhtNode node;

        if (!(pml.getLess() instanceof Boolean)) {
            throw new BadAckException("processAck_MergeDone: pml.getLess() -> false ");
        }

        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            throw new PhtNodeNotFoundException("processAck_MergeDone "
                    + message.getInitiatorLabel());
        }

        if (pml.getDestLabel().endsWith("0")) {
            if(! node.state.mergeDoneLSon() ) {
                log(String.format("((%d)) processAck_MergeDone fail lson"
                                + "[node: '%s'][initiator: '%s' on %d] "
                                + "[label: '%s'][node state: %s] '",
                        message.getId(), pml.getDestLabel(),
                        message.getInitiatorLabel(), message.getInitiator().getID(),
                        pml.getDestLabel(), node.state.toString()));
                interrupt();
            }
        } else if (pml.getDestLabel().endsWith("1")) {
            if(! node.state.mergeDoneRSon() ) {
                log(String.format("((%d)) processAck_MergeDone fail rson"
                                + "[node: '%s'][initiator: '%s' on %d] "
                                + "[label: '%s'][node state: %s] '",
                        message.getId(), pml.getDestLabel(),
                        message.getInitiatorLabel(), message.getInitiator().getID(),
                        pml.getDestLabel(), node.state.toString()));
                interrupt();
            }
        }

        log(String.format("((%d)) processAck_MergeDone [node: '%s'][initiator: '%s' on %d] "
                        + "[node's state: %s]\n",
                message.getId(), pml.getDestLabel(),
                message.getInitiatorLabel(), message.getInitiator().getID(),
                node.state));

        if (node.state.mergeDone()) {
            node.state.startThreaded();

            // Update for the previous leaf
            if (node.getPrevLeaf().getNode() != null) {
                startUpdateLeavesMerge(
                        message.getId(),
                        PhtMessage.UPDATE_NEXT_LEAF,
                        node.getLabel(),
                        node.getPrevLeaf()
                );
            } else {
                node.state.ackPrevLeaf();
            }

            // Update for the next leaf
            if (node.getNextLeaf().getNode() != null) {
                startUpdateLeavesMerge(
                        message.getId(),
                        PhtMessage.UPDATE_PREV_LEAF,
                        node.getLabel(),
                        node.getNextLeaf()
                );
            } else {
                node.state.ackNextLeaf();

                if (node.state.isStable()) {
                    node.leaf();
                    client.mergeOk();
                }
            }

            // Inform the client
            client.mergeOk();

            log(String.format("((%d)) processAck_MergeDone [node: '%s'][initiator: '%s' on %d] "
                            + "[node's state: %s]\n",
                    message.getId(), pml.getDestLabel(),
                    message.getInitiatorLabel(), message.getInitiator().getID(),
                    node.state));
        }
    }

    /**
     * ACK for an update from the next leaf (threaded leaves), which updated
     * its previous leaf.
     * @param message Useful for the id
     * @param pml For the label
     */
    private void processAck_UpdatePrevLeaf (PhtMessage message, PMLookup pml) {
        boolean merge = false;
        PhtNode node;

        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            testNullNode(message.getInitiatorLabel(), message);
            log( String.format("((%d)) processAck_UpdatePrevLeaf node null :: "
                    + "label: '%s' [%d]\n",
                    message.getId(), message.getInitiatorLabel(), this.node.getID()) );
            return;
        }

        node.use();
        node.useDest();

        if (! node.state.ackNextLeaf() ) {
            log ( String.format("((%d)) processAck_UpdatePrevLeaf error node '%s' "
                            + "ack from '%s' :: tleaves: %s \n",
                    message.getId(), node.getLabel(), pml.getDestLabel(),
                    node.state.toString()));
        } else {
            log(String.format("((%d)) processAck_UpdatePrevLeaf node '%s' ack from '%s' "
                            + "tleaves: %s \n",
                    message.getId(), node.getLabel(), pml.getDestLabel(),
                    node.state.toString()));
        }

        if (node.state.mergeDone()) {
            merge = true;
        }

        node.setNextLeaf(new NodeInfo(pml.getDestLabel(), pml.getDest()));

        if (node.state.isStable()) {
            node.leaf();
            if (merge) {
                client.mergeOk();
            }
        }
    }

    /**
     * ACK for an update from the previous leaf (threaded leaves), which
     * updated its next leaf.
     * @param message Useful for the id
     * @param pml For the label
     */
    private void processAck_UpdateNextLeaf (PhtMessage message, PMLookup pml) {
        boolean merge = false;
        PhtNode node;

        node = this.nodes.get(message.getInitiatorLabel());
        if (node == null) {
            testNullNode(message.getInitiatorLabel(), message);
            log(String.format("((%d)) processAck_UpdateNextLeaf node null :: "
                            + "label: '%s' [%d]\n",
                    message.getId(), message.getInitiatorLabel(), this.node.getID()));
            return;
        }

        node.use();
        node.useDest();

        if (! node.state.ackPrevLeaf() ) {
            log ( String.format("((%d)) processAck_UpdateNextLeaf error node '%s' tleaves: %s \n",
                    message.getId(), node.getLabel(), node.state.toString()));
        } else {
            log(String.format("((%d)) processAck_UpdateNextLeaf node '%s' ack from '%s'"
                            + "tleaves: %s \n",
                    message.getId(), node.getLabel(), pml.getDestLabel(),
                    node.state.toString()));
        }

        if (node.state.mergeDone()) {
            merge = true;
        }

        node.setPrevLeaf(new NodeInfo(pml.getDestLabel(), pml.getDest()));

        if (node.state.isStable()) {
            node.leaf();
            if (merge) {
                client.mergeOk();
            }
        }
    }

    /* ________________________                       _______________________ */
    /* ________________________ Process methods tools _______________________ */

    /**
     * Check if we can route the message to the PhtNode' sfather and do so if
     * possible.
     * (Used by processUpdateNextLeaf and processUpdatePreviousLeaf.)
     * @param message information
     * @param pml extracted from message
     */
    private void updateLeavesToFather(PhtMessage message, PMLookup pml) {
        testNullNode(pml.getDestLabel(), message);

        String father = PhtUtil.father(pml.getDestLabel());
        pml.setDestLabel(father);
        this.dht.send(message, father);
    }

    /**
     * Forward a leaf update message to a PhtNode's son. If the son exists,
     * direct communication is used, otherwise the message is route to the
     * son's label.
     * @param message general information
     * @param pml destination label
     * @param son information about the son
     * @param label '0' if left son or '1' if right son
     */
    private void updateLeavesToSon(PhtMessage message, PMLookup pml, NodeInfo son, String label) {
        if (son.getNode() != null) {
            pml.setDestLabel(son.getKey());
            EDSimulator.add(delay(), message, son.getNode(), phtid);
        } else {
            pml.setDestLabel(pml.getDestLabel() + label);
            this.dht.send(message, pml.getDestLabel());
        }
        return;
    }

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
     * Split related method
     *
     * Send to the left son its previous and next leaves
     * @param message PhtMessage of the split request
     * @param pml PMLookup extracted from message
     * @param node Current node (who started the split)
     */
    private void startUpdateLeavesL (PhtMessage message, PMLookup pml, PhtNode node) {
        NodeInfo[] info;
        PhtMessage update;
        PMLookup pmlUpdate;

        info      = new NodeInfo[2];
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

        info[NodeInfo.ARRAY_PREVLEAF] = new NodeInfo(node.getPrevLeaf().getKey(), node.getPrevLeaf().getNode());
        info[NodeInfo.ARRAY_NEXTLEAF] = new NodeInfo(node.getRson().getKey(), node.getRson().getNode());

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
        NodeInfo[] info;
        PhtMessage update;
        PMLookup pmlUpdate;

        info      = new NodeInfo[2];
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

        info[NodeInfo.ARRAY_PREVLEAF] = new NodeInfo(node.getLson().getKey(), node.getLson().getNode());
        info[NodeInfo.ARRAY_NEXTLEAF] = new NodeInfo(node.getNextLeaf().getKey(), node.getNextLeaf().getNode());

        pmlUpdate.setLess(info);
        EDSimulator.add(delay(), update, node.getRson().getNode(), phtid);
    }

    /**
     * Update a leaf's previous or next leaf
     *
     * After a merge the father of the merge process, sends an update to its
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

        // An event must be a PhtMessage
        if (event instanceof PhtMessage) {
            message = (PhtMessage) event;
        } else {
            return;
        }

        if (message.getMore() instanceof PMLookup) {
            pml = (PMLookup) message.getMore();
        }

        // Avoid a huge switch with all the operations and their ACK
        if (message.getType() > PhtMessage.ACK) {
            processAck(message, pml);
            return;
        }

        // A PhtMessage has arrived: this Node has been used
        this.usage++;

        try {
            switch (message.getType()) {
                case PhtMessage.RETRY:
                    processRetry(message);
                    break;

                case PhtMessage.INIT:
                    log(String.format("PHT init message sent by %d [%d]\n",
                            message.getInitiator().getID(), this.node.getID()));
                    initiate();
                    break;

                case PhtMessage.SPLIT:
                    processSplit(message, pml);
                    break;

                case PhtMessage.SPLIT_DATA:
                    processSplitData(message, pml);
                    break;

                case PhtMessage.SPLIT_LEAVES:
                    processSplitLeaves(message, pml);
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
                    processInsertion(message, pml);
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

                case PhtMessage.BACK_LIN_LOOKUP:
                    processBackwardLinLookup(message, pml);
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

                case PhtMessage.NO_MERGE:
                    break;
            }
        } catch (PhtException pe) {
            handleException(pe);
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
                    processAck_Split(message, pml);
                    break;

                case PhtMessage.ACK_SPLIT_DATA:
                    processAck_SplitData(message, pml);
                    break;

                case PhtMessage.ACK_SPLIT_LEAVES:
                    processAck_SplitLeaves(message, pml);
                    break;

                case PhtMessage.ACK_UPDATE_PREV_LEAF:
                    processAck_UpdatePrevLeaf(message, pml);
                    break;

                case PhtMessage.ACK_UPDATE_NEXT_LEAF:
                    processAck_UpdateNextLeaf(message, pml);
                    break;

                case PhtMessage.ACK_MERGE:
                    processAck_Merge(message, pml);
                    break;

                case PhtMessage.ACK_MERGE_LEAVES:
                    processAck_MergeLeaves(message, pml);
                    break;

                case PhtMessage.ACK_MERGE_DATA:
                    processAck_MergeData(message, pml);
                    break;

                case PhtMessage.ACK_MERGE_DONE:
                    processAck_MergeDone(message, pml);
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
                    processAck_LinLookup(message, pml);
                    break;

                case PhtMessage.ACK_BIN_LOOKUP:
                    break;

                case PhtMessage.ACK_SEQ_QUERY:
                    processAck_SeqQuery(message, pml);
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
            }
        } catch (PhtException pe) {
            handleException(pe);
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
     * Test if the PhtNode exists or if we have reached the Pht root.
     * This method is used by methods when they have not found the destination
     * PhtNode. If there has been a routing error, the message might have been
     * arrived on the wrong Node.
     * @param label destination label
     * @param message message with basic information for the logs
     */
    private void testNullNode(String label, PhtMessage message) {
        if ( (label.equals("")) || (allPhtNodes.get(label) != this.node.getID()) ) {
            System.err.printf("((%d)) [type %d] [dest %s] fatal: PhtNode '%s' exists on node %d (currently on node %d)\n",
                    message.getId(), message.getType(), message.getInitiatorLabel(),
                    label, allPhtNodes.get(label), this.node.getID());

            if (stopOnRouteFail) {
                stats.end();
                stats.printAll();
                interrupt();
            }
        }
    }

    /**
     * toString for PhtProtocol
     * @return all (key, data) from its nodes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder(this.nodes.size() * PhtProtocol.D);

        sb.append("PHT ");
        sb.append(this.node.getID());
        sb.append(" ");
        sb.append(this.nodes.size());
        sb.append(" nodes.\n");
        for (Map.Entry<String, PhtNode> nd: this.nodes.entrySet()) {
            PhtNode node = nd.getValue();

            sb.append("id: ");
            sb.append(this.node.getID());
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
        this.dht  = dht;
    }

    public void setNode (Node nd) {
        this.node = nd;
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

    public void setClient(Client client) {
        PhtProtocol.client = client;
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
        client.initOk();
        log("PHT initiate() on node " + this.node.getID());

        System.out.println("PHT initiate");
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

    public void flush() {
        try {
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /* ________________________________       _______________________________ */
    /* ________________________________ Delay _______________________________ */

    /**
     * We do not need a real pseudo-random delay. Just a changing one.
     * @return next delay.
     */
    private short delay() {
        delay = (short) ((delay + 1) % MAX_DELAY);
        return 0;

    /* ______________________________            ____________________________ */
    /* ______________________________ Exceptions ____________________________ */

    /**
     * Handle a PhtException.
     * If a handler has been set, call its handle method. Otherwise, the
     * default behaviour is print the stack trace and exit.
     * @param pe Exception thrown
     */
    public void handleException(PhtException pe) {
        flush();
        if (this.pehandler != null) {
            this.pehandler.handle(pe);
        } else {
            pe.printStackTrace();
            interrupt();
        }
    }

    /* ____________________________               ___________________________ */
    /* ____________________________ Debug methods ___________________________ */

    /**
     * Stop the test and/or the simulation
     */
    public void interrupt() {
        try {
            logWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.exit(-1);
    }

    /**
     * Find a PhtNode.
     * @param label PhtNode's label
     * @return The PhtNode, or null if it does not exists.
     */
    public static PhtNode findPhtNode (String label) {
        long ppId = allPhtNodes.get(label);
        return ((PhtProtocol)(Network.get((int)ppId))).nodes.get(label);
    }
}
