#include <assert.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <bsd/stdlib.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <openssl/sha.h>
#include <string>
#include <thread>
#include <typeinfo>
#include <hiredis/hiredis.h>
#include <bsd/stdlib.h>

#include <jsoncpp/json/json.h>
#include <jsoncpp/json/reader.h>
#include <jsoncpp/json/writer.h>
#include <jsoncpp/json/value.h>

#include <fstream>
#include <string>

using namespace std;



// This method loads a json file, converts it to json obj.
//
// Return true if success, false otherwise.
bool LoadJsonFile(char* filename, Json::Value& root) {
  ifstream file(filename);
  string content;

  // load file content.
  string line;
  while (getline(file, line)) {
    content.append(line);
  }
  printf("file %s content: \n%s\n", filename, content.c_str());

  // convert to json
  Json::Reader reader;
  bool rv = reader.parse(content.c_str(), root);
  printf("parse to json ret %s\n", rv ? "ok" : "failure");

  return rv;
}

// This method writes json obj to redis.
//
// Return true on success, false otherwise.
bool WriteConfig2Redis(Json::Value& root, redisContext* c) {
  Json::FastWriter fastWriter;
  string output = fastWriter.write(root);

  const char *dummy = "test";
  redisReply* reply =
    (redisReply*)redisCommand(c, "hcdsetproxy %s %b", dummy, output.c_str(), output.size());
  if (!reply) {
    printf("cmd failed, c->err = %d(%s)\n", c->err, c->errstr);
  } else {
    printf("got reply\n  type=%d\n  str=%s(%d)\n  integer=%lld\n",
           reply->type, reply->str, reply->len, reply->integer);
    freeReplyObject(reply);
  }

  return true;
}

int main(int argc, char** argv) {
  if (argc < 4) {
    printf("Usage: %s [conf file] [proxy ip] [proxy port]\n", argv[0]);
    return -1;
  }

  Json::Value root;
  if (LoadJsonFile(argv[1], root)) {
    redisContext *c = redisConnect(argv[2], atoi(argv[3]));
    if (c == NULL || c->err) {
      if (c) {
        printf("Failed to connect redis: %s\n", c->errstr);
      } else {
        printf("Can't allocate redis context\n");
      }
      return -1;
    }
    WriteConfig2Redis(root, c);
    redisFree(c);
  }

  return 0;


}
