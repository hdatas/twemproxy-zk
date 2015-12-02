#ifndef _ZK_UTIL_H_
#define _ZK_UTIL_H_

#include <zookeeper.h>
//#include <zookeeper_log.h>
#include <proto.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


#include <sys/time.h>
#include <unistd.h>
#include <sys/select.h>

#include <time.h>
#include <errno.h>
#include <assert.h>


// Context struct for zoo_get().
typedef struct ContextGet_s {
  // fields for get completion.
  // zk path to get
  char *path;
  // buf to store returned value
  char *buf;
  // buf size in bytes
  int buf_len;

  // return code
  int rc;
  // returned value size in bytes
  int ret_buf_len;

  // sync lock
  pthread_mutex_t lock;

  // fields for get watcher.
  // watcher func
  watcher_fn watcher;
  // watcher func context
  void *ctx;

} ContextGet;


// Context struct for zoo_exists().
typedef struct ContextExists_s {
  // znode path
  char *path;

  // watcher func
  watcher_fn watcher;
  // watcher func context
  void *ctx;

} ContextExists;


// Connect to zk ensemble.
zhandle_t* ZKConnect(char* zk_hosts);

// Close connection to zk ensemble.
void ZKClose(zhandle_t* zkh);

// Create a znode.
int ZKCreate(zhandle_t *zkh, char* path, char *value, int value_len, int flags);

// Get a znode value.
int ZKGet(zhandle_t *zkh, char *path, char *buf, int buf_len, int watch, int sync);

// Set an existing znode value.
int ZKSet(zhandle_t *zkh, char* path, char *value, int value_len);

// Delete a znode.
int ZKDelete(zhandle_t *zkh, char* path);

// Check if znode exists.  Return 1 if exists, 0 otherwise.
int ZKExists(zhandle_t *zkh, char* path);

// Set a "get" watcher on a znode.
int ZKSetGetWatch(zhandle_t *zkh, char *path, watcher_fn watcher, void* ctx);

// Set an "existence" watcher on a znode.
int ZKSetExistsWatch(zhandle_t *zkh, char *path, watcher_fn watcher, void* ctx);

void DefaultGetCompletion(int rc,
                      const char *value,
                      int value_len,
                      const struct Stat *stat,
                      const void *data);

void DefaultGetWatcher(zhandle_t *zkh,
                   int type,
                   int state,
                   const char *path,
                   void *ctx);

void DefaultExistsWatcher(zhandle_t *zkh,
                   int type,
                   int state,
                   const char *path,
                   void *ctx);


void GlobalWatcher(zhandle_t *zh,
                   int type,
                   int state,
                   const char *path,
                   void* ctx);

#endif  // _ZK_UTIL_H_
