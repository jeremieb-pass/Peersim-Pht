# Peersim-pht

Prefix Hash Tree Implementation for PeerSim as described in https://www.eecs.berkeley.edu/~sylvia/papers/pht.pdf

## Pht Operations 

1. Linear lookup

   a top-down traversal of the prefix tree (trie) which returns the the unique leaf node whose 
   label is a prefix of the given key.

2. Binary lookup 

   Try different prefix lengths until the required leaf is reached. (not yet implemented).

3. Insertion of a key with related split operation.
4. Deletion of a key with the related merge operation.
5. Sequential range query: return all keys in a range using the links between the leaves.
6. Parallel range query: return all keys in a range starting from the smallest common prefix node
 of the trie.
 
 ## PeerSim integration
 
 This Pht implementation can be used with any Distributed hash table as long as an interface is 
 provided to enable communication between the concrete implementation of the Dht and Pht. 
 
 ### Dht interface
 
The peersim.pht.DhtInterface interface contains the three methods needed for an integration with 
a Dht:

1. `void send(PhtMessage message, String dest)` Send a message to another node of the trie using
 the Dht. 
2. `Object getNodeId()` Id of the current Node (PeerSim node: a physical machine). This id is 
set by the Dht, not PeerSim. This method is used for debug and logs rather than for real operations.
3. `Node getNode()` Current node (PeerSim node). This method is needed for the beginning of the 
simulation. This information will be used to enable direct communications between PeerSim nodes, 
thus avoiding extra Dht routing.

#### Interface with MSPastry

PeerSim has additional packages, one of them being the MSPastry protocol. 
We provide some classes in the peersim.dht package to run simulations with this Dht.

### PeerSim configuration file

You will find five different parameters for the PhtProtocol in the configuration file:

1. Key size (bits).
2. Maximum amount of keys a leaf can have.
3. Dht interface.
4. Logs enabled. Possible values: "on" or anything else to disable this.
5. Type of range query for the simulation ("seq" for sequential, everything else for parallel).

## Project status

Every operation is implemented except for the binary lookup (it will be implemented very quickly 
!). For now, it is only possible to start a new operation when the previous is completely finished. 
For example: an insertion that triggered a split is finished when the split is finished, not when 
the key is inserted.

The next step is to set a thinner granularity and allow operations to start sooner.

## Statistics
 
At the end of any simulation, the `phtStats()` method is launched. It provides information 
about:
 
+ Number of keys in the Pht
+ Number of nodes in the Pht
+ Number of leaves in the requests
+ Number of requests
+ Number of leaves with more than B keys
+ Pht height
+ Minimum height
+ Ten most used nodes
+ Ten least used nodes
+ Ten most loaded leaves
+ Ten least loaded leaves
