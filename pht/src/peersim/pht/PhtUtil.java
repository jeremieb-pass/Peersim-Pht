package peersim.pht;

import org.apache.commons.codec.digest.DigestUtils;
import peersim.core.Network;
import peersim.core.Node;
import peersim.pastry.MSPastryProtocol;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 * Static methods to generate Objects or check properties.
 * </p>
 *
 * <p>
 * The goal of this class is to provide some useful methods to other class
 * of the peersim.pht.* packages from generating a list of keys to statistics.
 * </p>
 */
public class PhtUtil {

    /**
     * Does @param key starts with @param with ?
     * @param key The key
     * @param with The prefix
     * @return true if @param key starts with @param with
     */
    private static boolean startsWith(String key, String with) {
        if (key.length() < with.length()) {
            System.err.printf("'%s' (len: %d) does not start with '%s' (len: %d)\n",
                    key, key.length(), with, with.length());
            return false;
        }

        for (int i = 0; i < with.length(); i++) {
            if (key.charAt(i) != with.charAt(i)) {
                System.err.printf("((%d)) '%s' [%c] does not start with '%s' [%c]\n",
                        i, key, key.charAt(i), with, with.charAt(i));
                return false;
            }
        }

        return true;
    }

    public static String smallestCommonPrefix (String keyL, String keyH) {
        int i;
        int minLen = keyL.length();
        String res = "";

        if (keyL.length() > keyH.length()) {
            minLen = keyH.length();
        }

        for (i = 0; i < minLen; i++) {
            if (keyL.charAt(i) != keyH.charAt(i)) {
                break;
            }
            res = res + keyL.charAt(i);
        }

       return res;
    }


    /**
     * Check if s1 is a key/label inferior to s2 (max of the range query)
     * @param s1 label to compare
     * @param s2 max range
     * @return true if s1 <= s2
     */
    public static boolean inRangeMax (String s1, String s2) {
        int max;

        max = (s1.length() < s2.length()) ? s1.length() : s2.length();
        for (int i = 0; i < max; i++) {
            if (s1.charAt(i) > s2.charAt(i)) {
                return false;
            } else if (s1.charAt(i) < s2.charAt(i)) {
                return true;
            }
        }

        return true;
    }

    /**
     * Check if s1 is a key/label superior to s2 (min of the range query)
     * @param s1 label to compare
     * @param s2 max range
     * @return true if s1 >= s2
     */
    public static boolean inRangeMin (String s1, String s2) {
        int max;

        max = (s1.length() < s2.length()) ? s1.length() : s2.length();
        for (int i = 0; i < max; i++) {
            if (s1.charAt(i) < s2.charAt(i)) {
                return false;
            } else if (s1.charAt(i) > s2.charAt(i)) {
                return true;
            }
        }

        return true;
    }

    public static byte[] hashMe(String str) {
        String res;

        if (str.equals("")) {
            res = "";
        } else {
            res = str;
        }
        return DigestUtils.sha1(res);
    }

    /**
     * Generates a random ArrayLists of keys (size: PhtProtocol.D)
     * @param len Number of bits of each String
     * @return the generated ArrayList
     */
    public static ArrayList<String> genKeys (int len, boolean shuffle) {
        int max = (int) Math.pow(2, len);
        ArrayList<String> res;

        res = new ArrayList<String>(max);
        for (int i = 0; i < max; i++) {
            StringBuilder sb = new StringBuilder();
            String val = Integer.toBinaryString(i);

            for (int j = val.length(); j < len; j++) {
                sb.append("0");
            }
            sb.append(val);

            res.add(sb.toString());
        }

        if (shuffle) {
            Collections.shuffle(res, new Random(PhtProtocol.D));
        }

        return res;
    }

    public static int keyToData (String key) {
        return Integer.parseInt(key, 2);
    }

    /* _____________________________            _____________________________ */
    /* _____________________________ Statistics _____________________________ */

    public static void phtStats() throws IOException {
        int maxStats      = 10;
        long nbNodes      = 0;
        long nbLeaves     = 0;
        long nbKeys       = 0;
        int nbMoreB       = 0;
        int nbKeysMoreB   = 0;
        int maxHeightLeaf = 0;
        int minHeightLeaf = PhtProtocol.D;
        double average;
        TreeSet<PhtNode> tsUsage      = new TreeSet<PhtNode>(new PNUsageComp());
        TreeSet<PhtNode> tsKeysLeaves = new TreeSet<PhtNode>(new PNKeysComp());
        FileWriter fw = new FileWriter("pht_stats.log");

        for (int i = 0; i < Network.size(); i++) {
            PhtProtocol pht;

            pht = (PhtProtocol) Network.get(i).getProtocol(PhtProtocol.getPid());
            if (pht == null) {
                fw.close();
                return;
            }
            for (PhtNode nd: pht.getNodes().values()) {
                tsUsage.add(nd);

                nbNodes++;

                if (nd.isLeaf()) {
                    nbLeaves++;

                    tsKeysLeaves.add(nd);
                    nbKeys += nd.getNbKeys();

                    if (nd.getNbKeys() > PhtProtocol.B) {
                        nbMoreB++;
                        nbKeysMoreB += nd.getNbKeys();
                    }

                    if (nd.getLabel().length() < minHeightLeaf) {
                        minHeightLeaf = nd.getLabel().length();
                    } else if (nd.getLabel().length() > maxHeightLeaf) {
                        maxHeightLeaf = nd.getLabel().length();
                    }
                }
            }
        }

        // Write the results
        fw.write("Number of keys in the Pht  : " + nbKeys   + "\n");
        fw.write("Number of nodes in the Pht : " + nbNodes  + "\n");
        fw.write("Number of leaves in the Pht: " + nbLeaves + "\n");
        fw.write("Number of requests         : " + PhtProtocol.getNextId() + "\n\n");

        fw.write("PhtProtocol.B: " + PhtProtocol.B + "\n");
        fw.write("PhtProtocol.D: " + PhtProtocol.D + "\n\n");

        average = ((double)nbKeysMoreB) / ((double) nbMoreB);
        fw.write( String.format("Number of leaves with more than B keys: %d"
                + " with %.2f keys on average\n\n", nbMoreB, average) );

        fw.write("Trie height: " + maxHeightLeaf + "\n");
        fw.write("Minimum height for a leaf: " + minHeightLeaf + "\n\n");

        fw.write("\n---------- Usage ----------\n\n");
        for (int i = 1; i <= maxStats; i++) {
            PhtNode node = tsUsage.pollLast();
            if (node == null) {
                fw.close();
                return;
            }

            fw.write(i + ". " + node.getLabel()
                    + " has been used " + node.getUsage() + " times (all type of requests) "
                    + " and really used " + node.getUsageDest() + " times\n");
        }

        fw.write("\n");
        for (int i = 1; i <= maxStats; i++) {
            PhtNode node = tsUsage.pollFirst();
            if (node == null) {
                fw.close();
                return;
            }

            fw.write(i + ". " + node.getLabel()
                    + " has been used " + node.getUsage() + " times (all type of requests) "
                    + " and really used " + node.getUsageDest() + " times\n");
        }

        fw.write("\n\n\n---------- Keys ----------\n\n");
        for (int i = 1; i <= maxStats; i++) {
            PhtNode leaf = tsKeysLeaves.pollLast();
            if (leaf == null) {
                fw.close();
                return;
            }

            fw.write(i + ". " + leaf.getLabel()
                    + " has " + leaf.getNbKeys() + " keys\n");
        }

        fw.write("\n");
        for (int i = 1; i <= maxStats; i++) {
            PhtNode leaf = tsKeysLeaves.pollFirst();
            if (leaf == null) {
                fw.close();
                return;
            }

            fw.write(i + ". " + leaf.getLabel()
                    + " has " + leaf.getNbKeys() + " keys\n");
        }

        fw.close();
    }

    private static class PNUsageComp implements Comparator<PhtNode> {

        @Override
        public int compare(PhtNode phtNode, PhtNode t1) {
            return (int) (phtNode.getUsage() - t1.getUsage());
        }
    }

    private static class PNKeysComp implements Comparator<PhtNode> {

        @Override
        public int compare(PhtNode phtNode, PhtNode t1) {
            if (phtNode.getNbKeys() != t1.getNbKeys()) {
                return phtNode.getNbKeys() - t1.getNbKeys();
            }

            return -1;
        }
    }

    /* _______________________________       ________________________________ */
    /* _______________________________ Tests ________________________________ */


    /**
     * Get all the PhtNode from all the nodes in the simulation and check if:
     *
     * 1/ keys in leaf node starts with the node(s label
     * 2/ internal nodes has no keys
     */
    public static void checkTrie(List<PhtData> keys,
                                 List<String> inserted,
                                 List<String> removed) {
        int nullPrev = 0;
        int nullNext = 0;
        int cpt      = 0;
        int nbKeys   = keys.size();
        String startLeaf = null;
        List<Map<String, PhtNode>> nds = getAllNodes();
        ConcurrentHashMap<String, PhtNode> nodes  = new ConcurrentHashMap<String, PhtNode>(nbKeys);
        ConcurrentHashMap<String, PhtNode> leaves = new ConcurrentHashMap<String, PhtNode>(nbKeys);

        for (Map<String, PhtNode> map: nds) {
            for (PhtNode node: map.values()) {

                // Add the node to the nodes map
                assert ! nodes.containsKey(node.getLabel());
                nodes.put(node.getLabel(), node);

                // Add the leaf the leaves map
                if (node.isLeaf()) {
                    assert ! leaves.containsKey(node.getLabel());
                    leaves.put(node.getLabel(), node);

                    // Count the number of null links in the links between
                    // nodes
                    if (node.getPrevLeaf().getNode() == null) {
                        nullPrev++;
                        startLeaf = node.getLabel();
                    }
                    if (node.getNextLeaf().getNode() == null) {
                        nullNext++;
                    }
                }

                for (String key: node.getKeys()) {

                    // An internal node has no keys
                    if (! node.isLeaf()) {
                        assert node.getEntrySet().size() == 0;
                        continue;
                    } else {
                        assert startsWith(key, node.getLabel());
                        assert node.getLson().getNode() == null;
                        assert node.getRson().getNode() == null;
                    }

                    cpt++;
                }

            }

        }

        checkLeaves(leaves, startLeaf);
        if (nodes.size() > 1) {
            checkNodes("", nodes);
        }

        assert nullPrev == 1;
        assert nullNext == 1;

        List<String> keysGen = new LinkedList<String>();
        for (PhtData data: keys) {
            keysGen.add(data.getKey());
        }
        for (String key: removed) {
            keysGen.remove(key);
        }

        for (String key: inserted) {
            if (! keysGen.contains(key)) {
                System.out.printf("1. missing key is '%s'\n", key);
            }
        }

        for (String key: keysGen) {
            if (! inserted.contains(key)) {
                System.out.printf("2. missing key is '%s'\n", key);
            }
        }

        assert inserted.containsAll(keysGen);
        assert keysGen.containsAll(inserted);

        System.out.printf("nbKeys: %d <> cpt: %d\n", nbKeys, cpt);
        assert (nbKeys - removed.size()) == cpt;
    }

    /**
     * Check that all links between leaves are OK
     *
     * A leaf with a next leaf must be the previous leaf of this leaf.
     * The route from the most left leaf to the most right leaf must pass
     * through every leaf.
     * @param leaves Every leaves in the Pht
     * @param startLeaf Label of the most left leaf
     */
    private static void checkLeaves(Map<String, PhtNode> leaves, String startLeaf) {
        String next;
        List<String> check = new LinkedList<String>();

        assert startLeaf != null;
        assert leaves.size() >= 1;

        next = startLeaf;

        while (true) {
            PhtNode nxtNode;
            PhtNode curr;

            curr = leaves.get(next);
            assert curr != null;

            check.add(curr.getLabel());

            if (curr.getNextLeaf().getKey() != null) {
                nxtNode = leaves.get( curr.getNextLeaf().getKey() );
                assert leaves.get(nxtNode.getPrevLeaf().getKey()) == curr;
            } else {
                break;
            }

            next = nxtNode.getLabel();
        }

        assert leaves.keySet().containsAll(check);
        assert check.containsAll(leaves.keySet());
    }

    /**
     * Check the relationship between PhtNodes
     *
     * Check if every internal node has the right number of keys (sum of its
     * left and right sons number of keys), and that is it the father of its
     * two sons.
     * @param label Father's label
     * @param nodes All the PhtNodes in the Pht
     */
    private static void checkNodes(String label, Map<String, PhtNode> nodes) {
        PhtNode nd = nodes.get(label);
        PhtNode lson;
        PhtNode rson;

        if (nd == null) {
            return;
        }
        if (! nd.isLeaf()) {
            lson = nodes.get( nd.getLson().getKey() );
            assert lson != null;

            rson = nodes.get( nd.getRson().getKey() );
            assert rson != null;

            // Number of nodes
            assert nd.getNbKeys() == (lson.getNbKeys() + rson.getNbKeys());

            // nd is the father of his left and right sons
            assert nd.getLabel().equals(lson.getFather().getKey());
            assert nd.getLabel().equals(rson.getFather().getKey());

            checkNodes(lson.getLabel(), nodes);
            checkNodes(rson.getLabel(), nodes);
        }
    }

    /**
     * All keys that have been inserted must be in the Pht (except those who
     * have been removed)
     * @param keys Keys that have been inserted (and not removed)
     */
    public static void allKeys(List<String> keys) {
        List<Map<String, PhtNode>> nds = getAllNodes();
        List<String> phtKeys = new ArrayList<String>(keys.size());

        for (Map<String, PhtNode> map: nds) {
            for (PhtNode node: map.values()) {
                phtKeys.addAll(node.getKeys());
            }
        }

        assert phtKeys.containsAll(keys);
        assert keys.containsAll(phtKeys);
    }

    /**
     * Return all the PhtNodes across the Network (PeerSim)
     * @return List of maps with all the PhtNodes
     */
    private static List<Map<String, PhtNode>> getAllNodes() {
        List<Map<String, PhtNode>> res = new LinkedList<Map<String, PhtNode>>();

        for (int i = 0; i < Network.size(); i++) {
            PhtProtocol pht = (PhtProtocol) Network.get(i).getProtocol(PhtProtocol.getPid());
            res.add(pht.getNodes());
        }

        return res;
    }
}
