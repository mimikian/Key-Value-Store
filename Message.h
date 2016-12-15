/**********************************
 * FILE NAME: Message.h
 *
 * DESCRIPTION: Message class header file
 **********************************/
#ifndef MESSAGE_H_
#define MESSAGE_H_

#include "stdincludes.h"
#include "Member.h"
#include "common.h"

/**
 * CLASS NAME: Message
 *
 * DESCRIPTION: This class is used for message passing among nodes
 */
class Message{
public:
	MessageType type;
	ReplicaType replica;
	string key;
	string value;
	Address fromAddr;
	int transID;
	bool success; // success or not 
	// delimiter
	string delimiter;
	// construct a message from a string
	Message(string message);
	Message(const Message& anotherMessage);
	// construct a create or update message
	Message(int _transID, Address _fromAddr, MessageType _type, string _key, string _value);
	Message(int _transID, Address _fromAddr, MessageType _type, string _key, string _value, ReplicaType _replica);
	// construct a read or delete message
	Message(int _transID, Address _fromAddr, MessageType _type, string _key);
	// construct reply message
	Message(int _transID, Address _fromAddr, MessageType _type, bool _success);
	// construct read reply message
	Message(int _transID, Address _fromAddr, string _value);
	Message& operator = (const Message& anotherMessage);
	// serialize to a string
	string toString();
};

/**
 * CLASS NAME: Mp2Message
 *
 * DESCRIPTION: Another variant for message
 */
class Mp2Message{
public:
  Mp2Message();
  Mp2Message(MessageType msg):type(msg) {}
  virtual ~Mp2Message() {}
	MessageType type;
	MessageType fromMessageType;
	ReplicaType replica;
	string key;
	string value;
	Address fromAddr;
	int transID;
  bool got_reply;
	bool success; // success or not 
	// delimiter
	string delimiter = "::";

Mp2Message(string message){
  this->delimiter = "::";
  vector<string> tuple;
  size_t pos = message.find(delimiter);
  size_t start = 0;
  while (pos != string::npos) {
    string field = message.substr(start, pos-start);
    tuple.push_back(field);
    start = pos + 2;
    pos = message.find(delimiter, start);
  }
  tuple.push_back(message.substr(start));

  transID = stoi(tuple.at(0));
  Address addr(tuple.at(1));
  fromAddr = addr;
  type = static_cast<MessageType>(stoi(tuple.at(2)));
  switch(type){
    case CREATE:
    case UPDATE:
      key = tuple.at(3);
      value = tuple.at(4);
      if (tuple.size() > 5)
        replica = static_cast<ReplicaType>(stoi(tuple.at(5)));
      break;
    case READ:
    case DELETE:
      key = tuple.at(3);
      break;
    case REPLY:
      fromMessageType = static_cast<MessageType>(stoi(tuple.at(6)));
      if (tuple.at(3) == "1")
        success = true;
      else
        success = false;
      key = tuple.at(4);
      value = tuple.at(5);
      break;
    case READREPLY:
      value = tuple.at(3);
      break;
  }
}

/**
 * Constructor
 */
// construct a create or update message
Mp2Message(int _transID, Address _fromAddr, MessageType _type, string _key, string _value, ReplicaType _replica){
  this->delimiter = "::";
  transID = _transID;
  fromAddr = _fromAddr;
  type = _type;
  key = _key;
  value = _value;
  replica = _replica;
}

/**
 * Constructor
 */
Mp2Message(const Mp2Message& anotherMessage) {
  this->delimiter = anotherMessage.delimiter;
  this->fromAddr = anotherMessage.fromAddr;
  this->key = anotherMessage.key;
  this->replica = anotherMessage.replica;
  this->success = anotherMessage.success;
  this->transID = anotherMessage.transID;
  this->type = anotherMessage.type;
  this->value = anotherMessage.value;
}

/**
 * Constructor
 */
Mp2Message(int _transID, Address _fromAddr, MessageType _type, string _key, string _value){
  this->delimiter = "::";
  transID = _transID;
  fromAddr = _fromAddr;
  type = _type;
  key = _key;
  value = _value;
}

/**
 * Constructor
 */
// construct a read or delete message
Mp2Message(int _transID, Address _fromAddr, MessageType _type, string _key){
  this->delimiter = "::";
  transID = _transID;
  fromAddr = _fromAddr;
  type = _type;
  key = _key;
}

/**
 * Constructor
 */
// construct reply message
Mp2Message(int _transID, Address _fromAddr, MessageType _type, bool _success){
  this->delimiter = "::";
  transID = _transID;
  fromAddr = _fromAddr;
  type = _type;
  success = _success;
}

/**
 * Constructor
 */
// construct read reply message
Mp2Message(int _transID, Address _fromAddr, string _value){
  this->delimiter = "::";
  transID = _transID;
  fromAddr = _fromAddr;
  type = READREPLY;
  value = _value;
}

/**
 * FUNCTION NAME: toString
 *
 * DESCRIPTION: Serialized Message in string format
 */
string toString(){
  string message = to_string(transID) + delimiter + fromAddr.getAddress() + delimiter + to_string(type) + delimiter;
  switch(type){
    case CREATE:
    case UPDATE:
      message += key + delimiter + value + delimiter + to_string(replica);
      break;
    case READ:
    case DELETE:
      message += key;
      break;
    case REPLY:
      if (success)
        message += "1"+delimiter+key+delimiter+value+delimiter+ to_string(fromMessageType);
      else
        message += "0"+delimiter+key+delimiter+value+delimiter+ to_string(fromMessageType);
      break;
    case READREPLY:
      message += value;
      break;
  }
  return message;
}

/**
 * Assignment operator overloading
 */
Mp2Message& operator =(const Mp2Message& anotherMessage) {
  this->delimiter = anotherMessage.delimiter;
  this->fromAddr = anotherMessage.fromAddr;
  this->key = anotherMessage.key;
  this->replica = anotherMessage.replica;
  this->success = anotherMessage.success;
  this->transID = anotherMessage.transID;
  this->type = anotherMessage.type;
  this->value = anotherMessage.value;
  return *this;
}
};

#endif
