package peersim.pht;

import peersim.core.Node;

/**
 * This goal of this class is just to have an easy access to information a
 * PhtNode maintains on other PhtNodes: there key and and there Node (peersim).
 */
public class NodeInfo {
    private final String key;
    private Node node;

    public NodeInfo(String key) {
        this.key = key;
    }

    public NodeInfo(String key, Node node) {
        this.key = key;
        this.node = node;
    }

    public String getKey() {
        return key;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }
}
