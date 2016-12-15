/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
  this->memberNode = memberNode;
  this->par = par;
  this->emulNet = emulNet;
  this->log = log;
  ht = new HashTable();
  this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
  delete ht;
  delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 *        1) Gets the current membership list from the Membership Protocol (MP1Node)
 *           The membership list is returned as a vector of Nodes. See Node class in Node.h
 *        2) Constructs the ring based on the membership list
 *        3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
  /*
   * Implement this. Parts of it are already implemented
   */
  vector<Node> curMemList;
  bool change = false;

  /*
   *  Step 1. Get the current membership list from Membership Protocol / MP1
   */

  curMemList = getMembershipList();
  curMemList = checkRing(curMemList);
  /*
   * Step 2: Construct the ring
   */
  // Sort the list based on the hashCode
  sort(curMemList.begin(), curMemList.end());

  oldRing = ring;
  ring = checkRing(curMemList);
  
  /*
   * Step 3: Run the stabilization protocol IF REQUIRED
   */
  // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
  if(ht->hashTable.size() > 0){
    stabilizationProtocol();
  }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 *        i) generates the hash code for each member
 *        ii) populates the ring member in MP2Node class
 *        It returns a vector of Nodes. Each element in the vector contain the following fields:
 *        a) Address of the node
 *        b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
  unsigned int i;
  vector<Node> curMemList;
  for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
    Address addressOfThisMember;
    int id = this->memberNode->memberList.at(i).getid();
    short port = this->memberNode->memberList.at(i).getport();
    memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
    memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
    curMemList.emplace_back(Node(addressOfThisMember));
  }
  return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 *        HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
  std::hash<string> hashFunc;
  size_t ret = hashFunc(key);
  return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientf
 *
 * DESCRIPTION: client side CREATE API
 *        The function does the following:
 *        1) Constructs the message
 *        2) Finds the replicas of this key
 *        3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
  g_transID++;
  
  // Constructs the message
  Mp2Message msg = Mp2Message(g_transID, memberNode->addr, CREATE, key, value);
  
  // Sends a message to the replica
  sendMessage(msg);

}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 *        The function does the following:
 *        1) Constructs the message
 *        2) Finds the replicas of this key
 *        3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
  g_transID++;
  
  // Constructs the message
  Mp2Message msg = Mp2Message(g_transID, memberNode->addr, READ, key);
  
  // Sends a message to the replica
  sendMessage(msg);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 *        The function does the following:
 *        1) Constructs the message
 *        2) Finds the replicas of this key
 *        3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
  g_transID++;
  
  // Constructs the message
  Mp2Message msg = Mp2Message(g_transID, memberNode->addr, UPDATE, key, value);
  
  // Sends a message to the replica
  sendMessage(msg);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 *        The function does the following:
 *        1) Constructs the message
 *        2) Finds the replicas of this key
 *        3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
  g_transID++;
  
  // Constructs the message
  Mp2Message msg = Mp2Message(g_transID, memberNode->addr, DELETE, key);
  
  // Sends a message to the replica
  sendMessage(msg);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 *          The function does the following:
 *          1) Inserts key value into the local hash table
 *          2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
  return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 *          This function does the following:
 *          1) Read key from local hash table
 *          2) Return value
 */
string MP2Node::readKey(string key) {
  return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 *        This function does the following:
 *        1) Update the key to the new value in the local hash table
 *        2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
  return ht->update(key,value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 *        This function does the following:
 *        1) Delete the key from the local hash table
 *        2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
  bool success = ht->deleteKey(key);
  return success;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 *        This function does the following:
 *        1) Pops messages from the queue
 *        2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
  /*
   * Implement this. Parts of it are already implemented
   */
  char * data;
  int size;

  /*
   * Declare your local variables here
   */

  // dequeue all messages and handle them
  while ( !memberNode->mp2q.empty() ) {
    /*
     * Pop a message from the queue
     */
    data = (char *)memberNode->mp2q.front().elt;
    size = memberNode->mp2q.front().size;
    memberNode->mp2q.pop();
    string message(data, data + size);
    Mp2Message msg = Mp2Message(message);
    bool success = false;
    // Mp2Message replyMessage = Mp2Message(msg.transID, msg.fromAddr, REPLY, success);
    Mp2Message replyMessage = Mp2Message(msg);
    replyMessage.type = REPLY;
    string keyValue;
    switch(msg.type){
      case CREATE:
        success = createKeyValue(msg.key, msg.value, msg.replica);
        if(success){
          log->logCreateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
        }else{
          log->logCreateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
        }
        replyMessage.key = msg.key;
        replyMessage.value = msg.value;
        replyMessage.fromMessageType = CREATE;
        replyMessage.success = success;
        sendReplyMessage(replyMessage, CREATE);
        break;
      case READ:
        keyValue = readKey(msg.key);
        if(keyValue != ""){
          log->logReadSuccess(&memberNode->addr, false, msg.transID, msg.key, keyValue);
          success = true;
        }else{
          log->logReadFail(&memberNode->addr, false, msg.transID, msg.key);
        }
        replyMessage.key = msg.key;
        replyMessage.value = keyValue;
        replyMessage.success = success;
        sendReplyMessage(replyMessage, READ);
        break;
      case UPDATE:
        success = updateKeyValue(msg.key, msg.value, msg.replica);
        if(success){
          log->logUpdateSuccess(&memberNode->addr, false, msg.transID, msg.key, msg.value);
        }else{
          log->logUpdateFail(&memberNode->addr, false, msg.transID, msg.key, msg.value);
        }
        replyMessage.key = msg.key;
        replyMessage.value = msg.value;
        replyMessage.success = success;
        sendReplyMessage(replyMessage, UPDATE);
        break;
      case DELETE:
        success = deletekey(msg.key);
        if(success){
          log->logDeleteSuccess(&memberNode->addr, false, msg.transID, msg.key);
        }else{
          log->logDeleteFail(&memberNode->addr, false, msg.transID, msg.key);
        }
        replyMessage.fromMessageType = DELETE;
        replyMessage.key = msg.key;
        replyMessage.success = success;
        sendReplyMessage(replyMessage, DELETE);
        break;
      case REPLY:
      case READREPLY:
        quorum[msg.transID].repliesCount++;
        int successCount = quorum[msg.transID].successCount;
        int failsCount = quorum[msg.transID].failCount;
        if(msg.success){
            successCount++;
            quorum[msg.transID].successCount = successCount;
        }else{
            failsCount++;
            quorum[msg.transID].failCount = failsCount;
        }
        int count = max(successCount,failsCount);
        if(count < 2 || quorum[msg.transID].quorumReached ==  true)
          break;
        quorum[msg.transID].quorumReached = true;
        switch(msg.fromMessageType){
            case CREATE:
              if(successCount >= 2){
                log->logCreateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
              }else{
                log->logCreateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value);
              }
              quorum[msg.transID].commited = true;
              break;
            case DELETE:
              if(successCount >= 2){
                log->logDeleteSuccess(&memberNode->addr, true, msg.transID, msg.key);
              }else{
                log->logDeleteFail(&memberNode->addr, true, msg.transID, msg.key);
              }
              quorum[msg.transID].commited = true;
              break;
            case READ:
              if(successCount >= 2){
                log->logReadSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
              }else{
                log->logReadFail(&memberNode->addr, true, msg.transID, msg.key);
              }
              quorum[msg.transID].commited = true;
              break;
            case UPDATE:
              if(successCount >= 2){
                log->logUpdateSuccess(&memberNode->addr, true, msg.transID, msg.key, msg.value);
              }else{
                log->logUpdateFail(&memberNode->addr, true, msg.transID, msg.key, msg.value);
              }
              quorum[msg.transID].commited = true;
              break;
        }
        break;
    }
  }
  /*
   * This function should also ensure all READ and UPDATE operation
   * get QUORUM replies
   */
  checkFailedNodes();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 *        This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key, vector<Node> myRing) {
  size_t pos = hashFunction(key);
  vector<Node> addr_vec;
  if (myRing.size() >= 3) {
    // if pos <= min || pos > max, the leader is the min
    if (pos <= myRing.at(0).getHashCode() || pos > myRing.at(myRing.size()-1).getHashCode()) {
      addr_vec.emplace_back(myRing.at(0));
      addr_vec.emplace_back(myRing.at(1));
      addr_vec.emplace_back(myRing.at(2));
    }
    else {
      for (int i=1; i<myRing.size(); i++){
        Node addr = myRing.at(i);
        if (pos <= addr.getHashCode()) {
          addr_vec.emplace_back(addr);
          addr_vec.emplace_back(myRing.at((i+1)%myRing.size()));
          addr_vec.emplace_back(myRing.at((i+2)%myRing.size()));
          break;
        }
      }
    }
  }
  return addr_vec;
}

vector<Node> MP2Node::findNodes(string key) {
  size_t pos = hashFunction(key);
  vector<Node> addr_vec;
  if (ring.size() >= 3) {
    // if pos <= min || pos > max, the leader is the min
    if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
      addr_vec.emplace_back(ring.at(0));
      addr_vec.emplace_back(ring.at(1));
      addr_vec.emplace_back(ring.at(2));
    }
    else {
      for (int i=1; i<ring.size(); i++){
        Node addr = ring.at(i);
        if (pos <= addr.getHashCode()) {
          addr_vec.emplace_back(addr);
          addr_vec.emplace_back(ring.at((i+1)%ring.size()));
          addr_vec.emplace_back(ring.at((i+2)%ring.size()));
          break;
        }
      }
    }
  }
  return addr_vec;
}

vector<Node> MP2Node::checkRing(vector<Node> membershipList) {
  vector<Node> newRing;
  map<string, bool> found;
  for(int i = 0; i< membershipList.size();i++){
      if(!found[membershipList.at(i).nodeAddress.getAddress()])
        newRing.emplace_back(membershipList.at(i));
      found[membershipList.at(i).nodeAddress.getAddress()] = true;
  }
  return newRing;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
      return false;
    }
    else {
      return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 *        It ensures that there always 3 copies of all keys in the DHT at all times
 *        The function does the following:
 *        1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *        Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */

void MP2Node::stabilizationProtocol() {
  /*
   * Implement this
   */
    //iterator on all keys in my hash table
    //move the keys to another nodes where key belongs
    for (auto e = ht->hashTable.begin(); e != ht->hashTable.end();++e) {
      string key = e->first;
      string value = e->second;
      vector<Node> replicas = findNodes(key, oldRing);
      vector<Node> newReplicas = findNodes(key);
      int myPos = -1;
      for(int i = 0 ;i< 3;i++){
        if(replicas[i].nodeAddress == memberNode->addr)
          myPos = i;
      }
      if (myPos != -1) {
        switch (myPos) {
        case 0:
          {
            if (!isNodeAlive(replicas[1].nodeAddress)) {
              sendReplicationMessage(newReplicas[1].nodeAddress, key, value, SECONDARY);
            }
            if (!isNodeAlive(replicas[2].nodeAddress)) {
              sendReplicationMessage(newReplicas[2].nodeAddress, key, value, TERTIARY);
            }
            break;
          }
        case 1:
          {
            if (!isNodeAlive(replicas[0].nodeAddress)) {
              sendReplicationMessage(newReplicas[0].nodeAddress, key, value, PRIMARY);
            }
            if (!isNodeAlive(replicas[2].nodeAddress)) {
              sendReplicationMessage(newReplicas[2].nodeAddress, key, value, TERTIARY);
            }
            break;
          }
        case 2:
          {
            if (!isNodeAlive(replicas[0].nodeAddress)) {
              sendReplicationMessage(newReplicas[0].nodeAddress, key, value, PRIMARY);
            }
            if (!isNodeAlive(replicas[1].nodeAddress)) {
              sendReplicationMessage(newReplicas[1].nodeAddress, key, value, SECONDARY);
            }
            break;
          }
        }
      } else {
        sendReplicationMessage(newReplicas[0].nodeAddress, key, value, PRIMARY);
        sendReplicationMessage(newReplicas[1].nodeAddress, key, value, SECONDARY);
        sendReplicationMessage(newReplicas[2].nodeAddress, key, value, TERTIARY);
        ht->hashTable.erase(e);
      }
    }
}

void MP2Node::sendReplicationMessage(Address addr, string key, string value, ReplicaType replica) {
    g_transID++;
    //Send replication message
    quorum[g_transID].key = key;
    quorum[g_transID].value = value;
    quorum[g_transID].type = CREATE;

    // transID::fromAddr::CREATE::key::value::ReplicaType
    Mp2Message msg(CREATE);
    msg.transID = g_transID;
    msg.key = key;
    msg.value = value;
    msg.replica = replica;
    msg.fromAddr = memberNode->addr;

    string msg_content = msg.toString();
    const char *a = msg_content.c_str();
    size_t msgsize = sizeof(char) * msg_content.length();
    emulNet->ENsend(&memberNode->addr, &addr, (char *) a, msgsize);
}

bool MP2Node::isNodeAlive(Address adr)
{
    auto memberList = getMembershipList();
    for(auto &&node: memberList)
    {
        if(node.nodeAddress.getAddress() == adr.getAddress())
            return true;
    }
    return false;
}

void MP2Node::checkFailedNodes(){
  for(map<int,Quorum>::iterator itr=quorum.begin(); itr != quorum.end(); ++itr){
    int transID = itr->first;
    Quorum quorum_entry = itr->second;
    if(quorum_entry.type == READ || quorum_entry.type == UPDATE ){
      for(int i=0;i<3;++i){
        if(!quorum_entry.got_reply[i] && !isNodeAlive(quorum_entry.addresses[i]) && quorum_entry.repliesCount <3){
          quorum[transID].got_reply[i] = true;
          quorum[transID].repliesCount++;
          // log->logReadFail(&quorum_entry.addresses[i], false, transID, quorum_entry.key);
        }
      }
      if(quorum[transID].repliesCount == 3 && !quorum[transID].commited){
        if(quorum_entry.type == READ)
          log->logReadFail(&memberNode->addr, true, transID, quorum_entry.key);
        else
          log->logUpdateFail(&memberNode->addr, true, transID, quorum_entry.key, quorum_entry.value);
        quorum[transID].commited = true;
      }
   }
  }
}

void MP2Node::sendMessage(Mp2Message msg){
  quorum[msg.transID].key = msg.key;
  quorum[msg.transID].value = msg.value;
  quorum[msg.transID].type = msg.type;
  
  // Finds the replicas of this key
  vector<Node> replicas = findNodes(msg.key);
  // Send message
  for(int i = 0 ;i<replicas.size();i++){
    quorum[msg.transID].addresses[i] = replicas[i].nodeAddress;
    quorum[msg.transID].got_reply[i] = false;
    msg.replica = (enum ReplicaType)i;
    string msg_content = msg.toString();
    const char *a = msg_content.c_str();
    size_t msgsize = sizeof(char) * msg_content.length();
    emulNet->ENsend(&memberNode->addr, &replicas[i].nodeAddress, (char *) a, msgsize);
  }
}
void MP2Node::sendReplyMessage(Mp2Message reply_msg, MessageType reply_type){
  reply_msg.fromMessageType = reply_type;
  string msg_content = reply_msg.toString();
  const char *a = msg_content.c_str();
  size_t msgsize = sizeof(char) * msg_content.length();
  emulNet->ENsend(&memberNode->addr, &reply_msg.fromAddr, (char *) a, msgsize);
}
