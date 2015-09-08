package peersim.pht;

import peersim.core.Control;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *     ClientInterlocutor is an interface class between the real client and
 *     PhtProtocol. It is a singleton class, there is no need for more than
 *     one ClientInterlocutor.
 * </p>
 * <p>
 *     Its goal is to make simpler every operation for the client. The most
 *     important part is to retry every operation that has not succeeded
 *     (especially insertion).
 * </p>
 * <p>
 *     This way, the client only has to tell the ClientInterlocutor what he
 *     wants to do and let the latter handle everything.
 * </p>
 *
 */
public class ClientInterlocutor implements Client, Control {

    /*
     * Singleton
     */
    private static ClientInterlocutor ci;

    /*
     * Every operation that has been asked by the Client.
     */
    private Map<Long, Info> ops;

    /*
     * List of every operation that has been rejected.
     */
    private LinkedList<Info> rejected;

    /*
     * ClientInterlocutor communicates with a Client and a PhtProtocol.
     */
    private PhtProtocol pht;
    private Client client;

    private static final int CI_INSERTION   = 1;
    private static final int CI_SUPPRESSION = 2;
    private static final int CI_QUERY       = 3;
    private static final int CI_RANGEQUERY  = 4;

    /* _____________________________           ______________________________ */
    /* _____________________________ Singleton ______________________________ */

    private ClientInterlocutor () {
        this.ops      = new LinkedHashMap<Long, Info>();
        this.rejected = new LinkedList<Info>();
    }

    /**
     * Get the singleton.
     * @return The instance of ClientInterlocutor
     */
    public synchronized static ClientInterlocutor getInstance () {
        if (ci == null) {
            ci = new ClientInterlocutor();
        }

        return ci;
    }

    /* ______________________________         _______________________________ */
    /* ______________________________ Setters _______________________________ */

    public void setPht (PhtProtocol pht) {
        this.pht = pht;
    }

    public void setClient (Client client) {
        this.client = client;
    }

    /* ______________________________         _______________________________ */
    /* ______________________________ Pht API _______________________________ */

    /**
     * Deliver the insertion to PhtProtocol.
     * @param key Key to insert.
     * @param data Data to insert.
     */
    public void insertion(String key, Object data) {
        Long id;
        Info info;

        id = this.pht.insertion(key, data);
        info = new Info(
                CI_INSERTION,
                key,
                null,
                data
        );

        if (id >= 0) {
            this.ops.put(id, info);
        } else {
            if (! this.rejected.add(info) ) {
                error ("insertion", "rejected.add false");
            }
        }
    }

    public void suppression(String key) {
        Long id;
        Info info;

        id = this.pht.suppression(key);
        info = new Info(
                CI_SUPPRESSION,
                key,
                null,
                null
        );

        if (id >= 0) {
            this.ops.put(id, info);
        } else {
            if (! this.rejected.add(info) ) {
                error ("suppression", "rejected.add false");
                return;
            }
        }
    }

    public void query(String key) {
        Long id;
        Info info;

        id = this.pht.query(key);
        info = new Info(
                CI_QUERY,
                key,
                null,
                null
        );

        if (id >= 0) {
            this.ops.put(id, info);
        } else {
            if (! this.rejected.add(info) ) {
                error ("query", "rejected.add false");
            }
        }
    }

    public void rangeQuery(String keyMin, String keyMax) {

    }

    /* _________________________                    _________________________ */
    /* _________________________ Implements methods _________________________ */

    /* ______________________________ Client ________________________________ */

    /**
     * Remove the operation 'requestId' if everything went well (ok == 0),
     * otherwise, add the operation into the rejected list.
     * @param requestId Id of the operation.
     * @param ok Error code.
     */
    @Override
    public void responseOk(long requestId, int ok) {
        Info info;

        if (ok >= 0) {

            if (ok == 0) {
                this.client.responseOk(requestId, ok);
            }
        } else {

            info = this.ops.get(requestId);
            if (info == null) {
                error("responseOK", "info null");
            } else if (! this.rejected.add(info)) {
                error("responseOK", "rejected.add false");
            }
        }

        this.ops.remove(requestId);
    }

    @Override
    public void responseValue(long requestId, String key, Object data) {
        this.client.responseValue(requestId, key, data);
    }

    @Override
    public void responseList(long requestId, List<PhtData> resp) {

    }

    @Override
    public void splitOk () {
        this.client.splitOk();
        execute();
    }

    @Override
    public void mergeOk () {
        this.client.mergeOk();
        execute();
    }

    /* _____________________________ Control ________________________________ */

    @Override
    public boolean execute() {
        long id;
        Info info;

        info = this.rejected.poll();
        if (info == null) {
            this.pht.flush();
            return false;
        }

        switch (info.getType()) {

            case CI_INSERTION:
                id = this.pht.insertion(info.getKey(), info.getData());
                if (id >= 0) {
                    this.ops.put(id, info);
                } else {
                    this.rejected.add(info);
                }

                this.pht.flush();
                break;

            case CI_QUERY:
                id = this.pht.query(info.getKey());
                if (id >= 0) {
                    this.ops.put(id, info);
                } else {
                    this.rejected.add(info);
                }

                this.pht.flush();
                break;

            case CI_SUPPRESSION:
                id = this.pht.suppression(info.getKey());

                if (id >= 0) {
                    this.ops.put(id, info);
                } else {
                    this.rejected.add(info);
                }

                this.pht.flush();
                break;
        }

        return false;
    }

    /* _______________________________       ________________________________ */
    /* _______________________________ Utils ________________________________ */

    /**
     * Print a standard message onto the error stream with some indication.
     * @param method Name of the method who called.
     * @param message Message to print.
     */
    private void error (String method, String message) {
        System.out.printf("[ClientInterlocutor][%s][ERROR] %s\n",
                method, message);
    }

    public boolean queueEmpty () {
        return this.rejected.isEmpty();
    }

    /* ____________________________             _____________________________ */
    /* ____________________________ Inner class _____________________________ */

    /**
     * Inner class used to hold some information for a ClientInterlocutor.
     */
    class Info {
        private int type;
        private String key;
        private String keyMax;
        private Object data;

        public Info(int type, String key, String keyMax, Object data) {
            this.type   = type;
            this.key    = key;
            this.keyMax = keyMax;
            this.data   = data;
        }

        /* ____________________________ Getters _____________________________ */

        public int getType() {
            return type;
        }

        public String getKey() {
            return key;
        }

        public String getKeyMax() {
            return keyMax;
        }

        public Object getData() {
            return this.data;
        }

        /* ____________________________ Updates _____________________________ */

        public void addData (Object data) {
            if (this.data == null) {
                this.data = data;
            }
        }
    }
}
