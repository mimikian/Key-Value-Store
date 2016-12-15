# Key-Value-Store
- A key-value store supporting CRUD operations (Create, Read, Update, Delete).
- Load-balancing (via a consistent hashing ring to hash both servers and keys).
- Fault-tolerance up to two failures (by replicating each key three times to three successive nodes in the ring, starting from the first node at or to the clockwise of the hashed key).
- Quorum consistency level for both reads and writes (at least two replicas).
- Stabilization after failure (recreate three replicas after failure).
- A Gossip-style heartbeating membership protocol for thousand cluster nodes simulated by EmulNet.
- The protocol satisfies Completeness where it detects every node join, failure, and leave. Also it satisfies Accuracy of Failure Detection when there are no message losses and message delays are small. 

# System Architecture
 A three-layer implementation framework that allows to run multiple copies of peers within one process running a single-threaded simulation engine. The three layers are 1) the lower EmulNet (network), 2) middle layer including: MP1Node (membership protocol) and the MP2Node (the key-value store), and 3) the application layer.
 
Each node in the P2P layer is logically divided in two components: MP1Node and MP2Node. MP1Node runs a membership protocol. MP2Node supports all the KV Store functionalities. 
 
At each node, the key-value store talks to the membership protocol and receives from it the membership list. It then uses this to maintain its view of the virtual ring. Periodically, each node engages in the membership protocol to try to bring its membership list up to date.

![Homepage](https://raw.githubusercontent.com/mimikian/Key-Value-Store/master/imgs/structure.png)

## How to test it?
```bash
$ make clean
$ make
```
```bash
$ ./Application ./testcases/create.conf
```
or 
```bash
$ ./Application ./testcases/delete.conf
```
or
```bash
$ ./Application ./testcases/read.conf
```
or
```bash
$ ./Application ./testcases/update.conf
```
###### NOTES
This is the programming assignment from Coursera [Cloud Computing course 2](https://www.coursera.org/learn/cloud-computing-2).
