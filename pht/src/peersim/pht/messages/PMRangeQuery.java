package peersim.pht.messages;

import peersim.pht.PhtData;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * A PMRangeQuery is a class for range queries inside a PMLookup message.
 * It contains the maximum key of the range (the minimum is the key field of
 * PMLookup), and the node (peersim) from where the range query started: this
 * node will receive one or two acquittal (one for a sequential lookup, two
 * for a parallel lookup) and send a general acquittal to the initiator node.
 */
public class PMRangeQuery {
    private int count;
    private boolean end;
    private final String keyMin;
    private final String keyMax;
    private final List<PhtData> kdata;

    public PMRangeQuery(String keyMin, String keyMax) {
        this(keyMin, keyMax, 0);
    }

    public PMRangeQuery(String keyMin, String keyMax, int count) {
        this.keyMin = keyMin;
        this.keyMax = keyMax;
        this.count  = count;
        this.end    = false;
        this.kdata  = new LinkedList<PhtData>();
    }

    public void add(Collection<? extends  PhtData> kd) {
        this.kdata.addAll(kd);
    }

    public void addSupTo(Collection<? extends PhtData> kd, int min) {
        for (PhtData kdata: kd) {
            if ( Integer.parseInt(kdata.getKey(), 2) >= min) {
                this.kdata.add(kdata);
            }
        }
    }

    public void addInfTo(Collection<? extends PhtData> kd, int max) {
        for (PhtData kdata: kd) {
            if ( Integer.parseInt(kdata.getKey(), 2) <= max) {
                this.kdata.add(kdata);
            }
        }
    }

    public void addRange(Collection<? extends PhtData>kd, int min, int max) {
        for (PhtData kdata: kd) {
            if ( (Integer.parseInt(kdata.getKey(), 2) <= max)
                    && (Integer.parseInt(kdata.getKey(), 2) >= min) ) {
                this.kdata.add(kdata);
            }
        }
    }

    /* Getter */

    public String getKeyMin() {
        return keyMin;
    }

    public String getKeyMax() {
        return keyMax;
    }

    public int getCount() {
        return this.count;
    }

    public boolean isEnd() {
        return this.end;
    }

    public List<PhtData> getKdata() {
        return kdata;
    }

   /* Setter */

    public void stop() {
        this.end = true;
    }

    public void oneMore() {
        this.count++;
    }
}
