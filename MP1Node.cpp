/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *        Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
  for( int i = 0; i < 6; i++ ) {
    NULLADDR[i] = 0;
  }
  this->memberNode = member;
  this->emulNet = emul;
  this->log = log;
  this->par = params;
  this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *        This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
      return false;
    }
    else {
      return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
  Queue q;
  return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *        All initializations routines for a member.
 *        Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
  /*
   * This function is partially implemented and may require changes
   */
  int id = *(int*)(&memberNode->addr.addr);
  int port = *(short*)(&memberNode->addr.addr[4]);

  memberNode->bFailed = false;
  memberNode->inited = true;
  memberNode->inGroup = false;
    // node is up!
  memberNode->nnb = 0;
  memberNode->heartbeat = 0;
  memberNode->pingCounter = TFAIL;
  memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);
  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
  MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg) + sizeof(MessageHdr) + sizeof(Address), &memberNode->heartbeat, sizeof(long));
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        free(msg);
    }
    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *        Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
      return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
      return;
    }

    // incremeat hearbeat
    memberNode->heartbeat++;

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
      ptr = memberNode->mp1q.front().elt;
      size = memberNode->mp1q.front().size;
      memberNode->mp1q.pop();
      recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */

bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr *msg;
    msg = (MessageHdr *) malloc(size * sizeof(char));
    memcpy(msg, (char *)(data), sizeof(MessageHdr));
    Address *source;
    source = (Address *) malloc(sizeof(Address));    
    memcpy(source, data + sizeof(MessageHdr), sizeof(Address));

    if(msg->msgType == JOINREQ){
        size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = JOINREP;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg) + sizeof(MessageHdr) + sizeof(Address), &memberNode->heartbeat, sizeof(long));
        emulNet->ENsend(&memberNode->addr, source, (char *)msg, msgsize);
        free(msg);
        
        // Adding Membership
        long heartbeat = -1 ;
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(Address) , sizeof(long));
        int id = *(int*)(&source->addr);
        int port = *(short*)(&source->addr[4]);
        MemberListEntry entry = MemberListEntry(id,port,heartbeat,par->getcurrtime());
        bool found = false;
        for(int i = 0 ;i< memberNode->memberList.size();i++){
            MemberListEntry me = memberNode->memberList[i];
            if(me.getid()==id){
                me.settimestamp(par->getcurrtime());
                found = true;
                break;
            }
        }
        if(!found){
            memberNode->memberList.push_back(entry);
            log->logNodeAdd(&memberNode->addr, source);
        }
    }
    else if(msg->msgType == JOINREP){
        memberNode->inGroup = true;
        int id = *(int*)(&source->addr);
        short port = *(short*)(&source->addr[4]);
        long heartbeat = -1 ;
        memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(Address) , sizeof(long));
        MemberListEntry entry = MemberListEntry(id,port, heartbeat, par->getcurrtime());
        bool found = false;
        for(int i = 0 ;i< memberNode->memberList.size();i++){
            MemberListEntry me = memberNode->memberList[i]; 
            if(me.getid()==id){
                found = true;
                break;
            }
        }
        // entry for my self
        int nodeID = *(int*)(&memberNode->addr);
        short nodePort = *(short*)(&source->addr[4]);
        MemberListEntry myEntry = MemberListEntry(nodeID,nodePort, memberNode->heartbeat, par->getcurrtime());
        memberNode->memberList.push_back(myEntry);
        if(!found){
            memberNode->memberList.push_back(entry);
            log->logNodeAdd(&memberNode->addr, source);
        }
}else if(msg->msgType == GOSSIP){
        vector<MemberListEntry> *memberList;
        memberList = (vector<MemberListEntry> *) malloc(sizeof(vector<MemberListEntry>));
        memcpy(memberList, data + sizeof(MessageHdr) + sizeof(Address) , sizeof(vector<MemberListEntry> ));
        int sz = memberList->size();
        for(int i = 0 ;i< sz;i++){
            MemberListEntry me = (*memberList)[i];
            int id = me.getid();
            short port =  me.getport();
            string addres_mimi = to_string(id) + ":" + to_string(port);
            Address mimi = Address(addres_mimi);
            long meHeartbeat = me.getheartbeat();
            bool found = false;
            for(int j = 0; j< memberNode->memberList.size();j++){
                MemberListEntry myEntry = memberNode->memberList[j];
                int myEntryID = memberNode->memberList[j].getid();
                long myEntryHeartbeat = memberNode->memberList[j].getheartbeat();
                if(id == myEntryID){
                    found = true;
                    // hena
                    if(meHeartbeat > myEntryHeartbeat && meHeartbeat<=5000 && myEntryHeartbeat <= 5000){
                        memberNode->memberList[j].setheartbeat(meHeartbeat);
                        memberNode->memberList[j].settimestamp(par->getcurrtime());
                    }
                    break;
                }
                int nodeID = *(int*)(&memberNode->addr);
                if(nodeID == myEntryID){
                    memberNode->memberList[j].settimestamp(par->getcurrtime());
                    myEntry.settimestamp(par->getcurrtime());
                }
            }
            if(!found){
                MemberListEntry myEntry = MemberListEntry(id, port, meHeartbeat, me.gettimestamp());
                if(id<=par->EN_GPSZ && id>0){
                  memberNode->memberList.push_back(myEntry);
                log->logNodeAdd(&memberNode->addr, &mimi);
                }
            }
            found = false;
        }
    }
}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *        the nodes
 *        Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    int nodeID = *(int*)(&memberNode->addr);
    // Check if any node hasn't responded within a timeout period and then delete the nodes
    int sz = memberNode->memberList.size();
    vector<int> to_be_deleted = vector<int>();
    to_be_deleted.clear();
    for(int i = 0 ;i< sz; i++){
        MemberListEntry me = memberNode->memberList[i];
        int id =  me.getid();
        short port = me.getport();
        long heartbeat = me.getheartbeat();
        string addres_string = to_string(id) + ":" + to_string(port);
        Address a = Address(addres_string);
        if(par->getcurrtime()-heartbeat > (TREMOVE)){
            if(nodeID != id && id > 0 && id <=10){
              to_be_deleted.push_back(i);
              log->logNodeRemove(&memberNode->addr, &a);
            }
        }
        if(nodeID == id){
            memberNode->memberList[i].setheartbeat(memberNode->heartbeat);
            memberNode->memberList[i].settimestamp(par->getcurrtime());
        }
    }
    int sz2 = to_be_deleted.size();
    for(int i = 0 ;i< sz2;i++){
        memberNode->memberList.erase(memberNode->memberList.begin()+to_be_deleted[i]-i);
    }
    // Propagate your membership list : Gossping
    for(int i = 0 ;i< memberNode->memberList.size();i++){
        MemberListEntry me = memberNode->memberList[i];
        int id =  me.getid();
        short port = me.getport();
        string addres_string = to_string(id) + ":" + to_string(port);
        Address a = Address(addres_string);
        MessageHdr *msg;
        size_t msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(memberNode->memberList);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        msg->msgType = GOSSIP;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->memberList, sizeof(memberNode->memberList));
        emulNet->ENsend(&memberNode->addr, &a, (char *)msg, msgsize);
        free(msg);
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
  return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
  memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],addr->addr[3], *(short*)&addr->addr[4]) ;    
}
