package peersim.pht;

import org.apache.commons.codec.digest.DigestUtils;
import peersim.core.Network;
import peersim.core.Node;
import peersim.pastry.MSPastryProtocol;

import java.math.BigInteger;
import java.util.*;

/**
 * <p>Static methods to generate Objects or check properties.</p>
 *
 * <p>The goal of this class is to provide some useful methods to other class
 * of the peersim.pht.* packages from generating a list of keys to statistics.</p>
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
     * Is key strictly inferior to min ? First character strictly inferior to
     * min's one means true.
     * @param key Key to test
     * @param min Current minimum key
     * @return key < min
     */
    public static boolean infTo (String key, String min) {
        if (key == null) {
            return true;
        } else if (min == null) {
            return false;
        }

        int minLen;

        minLen = min.length() < key.length() ? min.length() : key.length();
        for (int i = 0; i < minLen; i++) {
            if (key.charAt(i) < min.charAt(i)) {
                return true;
            }
        }

        if (key.length() > min.length()) {
            return key.charAt(minLen) == '0';
        } else if (min.length() > key.length()) {
            return min.charAt(minLen) == '1';
        }

        return false;
    }

    /**
     * Is key strictly superior to min ? First character strictly superior to
     * min's one means true.
     * @param key Key to test
     * @param max Current maximum key
     * @return key > max
     */
    public static boolean supTo (String key, String max) {
        if (key == null) {
            return false;
        } else if (max == null) {
            return true;
        }

        int minLen;

        minLen = max.length() < key.length() ? max.length() : key.length();
        for (int i = 0; i < minLen; i++) {
            if (key.charAt(i) > max.charAt(i)) {
                return true;
            }
        }

        if (key.length() > max.length()) {
            return key.charAt(minLen) == '1';
        } else if (max.length() > key.length()) {
            return max.charAt(minLen) == '0';
        }

        return false;
    }

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

    public static String father (String label) {
        if (label == null) {
            return null;
        } else if (label.equals("")) {
            return null;
        }

        System.out.printf("[[]] PhtUtil.father :: father of '%s' is '%s'\n",
                label, label.substring(0, label.length()-1));
        return label.substring(0, label.length()-1);
    }

    /* _____________________________            _____________________________ */
    /* _____________________________ Statistics _____________________________ */


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
        List<Map<String, PhtNode>> nds  = getAllNodes();
        HashMap<String, PhtNode> nodes  = new HashMap<String, PhtNode>(nbKeys);
        HashMap<String, PhtNode> leaves = new HashMap<String, PhtNode>(nbKeys);

        for (Map<String, PhtNode> map: nds) {
            for (PhtNode node: map.values()) {

                // Add the node to the nodes map
                assert ! nodes.containsKey(node.getLabel());
                nodes.put(node.getLabel(), node);

                if (node.isLeaf()) {

                    System.out.printf("\n[PhtUtil] '%s' isLeaf\n", node.getLabel());

                    // Add the leaf to the leaves map
                    assert ! leaves.containsKey(node.getLabel());
                    leaves.put(node.getLabel(), node);

                    // Null links
                    if (node.getPrevLeaf().getNode() == null) {
                        nullPrev++;
                        startLeaf = node.getLabel();
                    }
                    if (node.getNextLeaf().getNode() == null) {
                        nullNext++;
                    }

                    // A leaf has no sons
                    assert node.getLson().getNode() == null;
                    assert node.getRson().getNode() == null;

                    // A leaf's label is a prefix for every of its keys
                    for (String key: node.getKeys()) {
                        assert startsWith(key, node.getLabel());
                        cpt++;

//                        System.out.printf("\ncheckTrie :: PhtNode '%s' with key '%s'\n",
//                                node.getLabel(), key);
                    }
                } else {
                    // An internal node has no keys
                    assert node.getDKeys().size() == 0;
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

        System.out.printf("nbKeys: %d <> removed.size: %d <> cpt: %d\n",
                nbKeys, removed.size(), cpt);
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

        int cptOk = 0;

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

                cptOk++;

                nxtNode = leaves.get( curr.getNextLeaf().getKey() );
                if( nxtNode == null ) {
                    PhtNode nd = PhtProtocol.findPhtNode(curr.getNextLeaf().getKey());

                    System.out.printf("\ncheckLeaves :: cptOk: %d :: %d leaves"
                                    + "\n curr -> '%s'"
                                    + "\n curr.nextLeaf -> '%s'\n",
                            cptOk, leaves.size(),
                            curr.getLabel(),
                            curr.getNextLeaf().getKey());
                    if (nd != null) {
                        System.out.printf("nextLeaf is -----> %s\n\n", nd.toString());
                    } else {
                        System.out.printf("nextLeaf does not exist\n\n");
                    }
                }

                if( leaves.get(nxtNode.getPrevLeaf().getKey()) != curr ) {
                    System.out.printf("\ncheckLeaves :: cptOk: %d :: %d leaves"
                                    + "\n curr -> '%s' :: nxt -> '%s'"
                                    + "\n :: curr.nextLeaf -> '%s' :: nxt.PrevLeaf -> '%s'\n\n",
                            cptOk, leaves.size(),
                            curr.getLabel(), nxtNode.getLabel(),
                            curr.getNextLeaf().getKey(), nxtNode.getPrevLeaf().getKey());
                }

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
