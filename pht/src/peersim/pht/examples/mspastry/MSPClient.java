package peersim.pht.examples.mspastry;

import peersim.config.Configuration;
import peersim.core.Control;
import peersim.core.Network;
import peersim.pht.*;
import peersim.pht.Client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * MSPClient is a basic client for a simulation using MSPastry.
 * It contains keys and data to insert, search or remove. By default it will
 * start every request from the same Node the bootstrap.
 */
public class MSPClient implements Control, Client {
    private static final int BOOTSTRAP = 0;

    private boolean exe  = false;
    private boolean stop = false;

    private int next;
    private LinkedList<PhtData> kdata;
    private List<String> inserted;
    private List<String> removed;
    private final PhtProtocol pht;

    private BufferedWriter logWriter;

    private int nextOp = 0;

    public MSPClient(String prefix) {
        boolean shuffle = Configuration.getBoolean(prefix + ".shuffle");
        int phtid       = Configuration.getPid(prefix + ".phtid");
        int len         = Configuration.getInt(prefix + ".len");
        int maxKeys     = Configuration.getInt(prefix + ".max");

        List<String> keys = PhtUtil.genKeys(len, shuffle);

        System.out.println("MSPClient");

        kdata    = new LinkedList<PhtData>();
        next     = 0;
        exe      = true;
        this.pht = (PhtProtocol) Network.get(BOOTSTRAP).getProtocol(phtid);
        inserted = new LinkedList<String>();
        removed  = new LinkedList<String>();

        for (int i = 0; (i < maxKeys) && (i < keys.size()); i++) {
            kdata.add( new PhtData( keys.get(i), Integer.parseInt(keys.get(i), 2)) );
        }

        try {
            logWriter = new BufferedWriter( new FileWriter("mspclient.log") );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method is the where requests are spread into the network.
     * @return true to stop the simulation, false otherwise
     */
    @Override
    public boolean execute() {
        if (! exe) {
            return false;
        } else if (stop) {
            return true;
        }

        PhtData data;

        if (next >= kdata.size()) {
            next = 0;
            nextOp++;
            System.out.printf("[MSPClient] nextOp: %d\n", nextOp);
            PhtUtil.checkTrie(kdata, inserted, removed);
            PhtUtil.allKeys(inserted);
        }

        data = kdata.get(next);
        switch (nextOp) {
            case 0:
                if ( this.pht.insertion( data.getKey(), data.getData(), this) >= 0) {
                    lock();
                    try {
                        this.logWriter.write( String.format("[MSPClient] insertion: %s\n",
                                kdata.get(next).getKey()) );
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    inserted.add(kdata.get(next).getKey());
                    next++;
                }
                break;

            case 1:
                if (this.pht.query(data.getKey(), this) >= 0) {
                    lock();
                    try {
                        this.logWriter.write( String.format(
                                "[MSPClient] query %s\n",
                                data.getKey()) );
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    next++;
                }
                break;

            case 2:
                if (this.pht.suppression(data.getKey(), this) >= 0) {
                    lock();
                    inserted.remove(kdata.get(next).getKey());
                    removed.add(kdata.get(next).getKey());
                    next += kdata.size()/PhtProtocol.B;
                }
                break;

            case 3:
                if (this.pht.rangeQuery(
                        kdata.get(next).getKey(),
                        kdata.get(kdata.size()-1).getKey(),
                        this) >= 0) {
                    lock();
                    try {
                        this.logWriter.write(String.format(
                                "[MSPClient] rangeQuery '%s' to '%s'\n",
                                kdata.get(next).getKey(),
                                kdata.get(kdata.size() - 1).getKey()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    next += kdata.size() / 4;
                }
                break;

            default:
                try {
                    PhtUtil.phtStats();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return true;
        }

        return false;
    }

    public void lock() {
        exe = false;
    }

    public void release() {
        exe = true;
    }

    @Override
    public void stop() {
        this.stop = true;
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

        if (res != PhtUtil.keyToData(key)) {
            System.out.printf("::MSPClient:: responseValue error: %d != %d\n",
                    res, PhtUtil.keyToData(key));
        }
    }

    @Override
    public void responseList(long requestId, List<PhtData> resp) {
    }

}
