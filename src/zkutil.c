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

#include "zkutil.h"


static int connected = 0;
static int expired = 0;

// Convert ZK state to string.
const char* State2String(int state){
  if (state == 0)
    return "CLOSED_STATE";
  if (state == ZOO_CONNECTING_STATE)
    return "CONNECTING_STATE";
  if (state == ZOO_ASSOCIATING_STATE)
    return "ASSOCIATING_STATE";
  if (state == ZOO_CONNECTED_STATE)
    return "CONNECTED_STATE";
  if (state == ZOO_EXPIRED_SESSION_STATE)
    return "EXPIRED_SESSION_STATE";
  if (state == ZOO_AUTH_FAILED_STATE)
    return "AUTH_FAILED_STATE";

  return "INVALID_STATE";
}

// Convert ZK event type to string.
const char* Type2String(int type){
  if (type == ZOO_CREATED_EVENT)
    return "CREATED_EVENT";
  if (type == ZOO_DELETED_EVENT)
    return "DELETED_EVENT";
  if (type == ZOO_CHANGED_EVENT)
    return "CHANGED_EVENT";
  if (type == ZOO_CHILD_EVENT)
    return "CHILD_EVENT";
  if (type == ZOO_SESSION_EVENT)
    return "SESSION_EVENT";
  if (type == ZOO_NOTWATCHING_EVENT)
    return "NOTWATCHING_EVENT";

  return "UNKNOWN_EVENT_TYPE";
}


void GlobalWatcher(zhandle_t *zh,
                   int type,
                   int state,
                   const char *path,
                   void* ctx) {
  fprintf(stderr, "global_watcher: event type %s, state = %s",
          Type2String(type), State2String(state));
  if (path && strlen(path) > 0) {
    fprintf(stderr, " for path %s", path);
  }
  fprintf(stderr, "\n");
  if (type == ZOO_SESSION_EVENT) {
    if (state == ZOO_CONNECTED_STATE) {
      connected = 1;
    } else if (state == ZOO_AUTH_FAILED_STATE) {
      zookeeper_close(zh);
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
      connected = 0;
      expired = 1;
      zookeeper_close(zh);
    } else {
      fprintf(stderr, "SESSION event: ignore state: %s\n",
              State2String(state));
    }
  } else {
    fprintf(stderr, "Ignore type %s, state %s\n",
            Type2String(type), State2String(state));
  }
}

void DefaultGetCompletion(int rc,
                          const char *value,
                          int value_len,
                          const struct Stat *stat,
                          const void *data) {
  if (data == NULL) {
    return;
  }
  ContextGet* ctx = (ContextGet*)data;
  ctx->rc = rc;

  switch (rc) {
    case ZOK:
      if (value_len > ctx->buf_len - 1) {
        ctx->ret_buf_len = ctx->buf_len - 1;
      } else {
        ctx->ret_buf_len = value_len;
      }
      memcpy(ctx->buf, value, (size_t)ctx->ret_buf_len);
      ctx->buf[ctx->ret_buf_len] = 0;
      break;

    case ZCONNECTIONLOSS:
    case ZOPERATIONTIMEOUT:
      fprintf(stderr, "Get completion: path %s: error = %d\n",
              ctx->path, rc);
      break;

    default:
      break;
  }

  pthread_mutex_unlock(&ctx->lock);
}

void DefaultExistsWatcher(zhandle_t *zkh,
                          int type,
                          int state,
                          const char *path,
                          void *ctx) {
  ContextExists* ce = (ContextExists*)ctx;

  fprintf(stderr, "exists_watcher: got event %s, state %s at path %s, ctx=%p\n",
          Type2String(type), State2String(state), path, ce);
  if (type == ZOO_CREATED_EVENT) {
  }

  if (type == ZOO_CHANGED_EVENT) {
  }

  if (type == ZOO_DELETED_EVENT) {
  }

  if (ce) {
    ZKSetExistsWatch(zkh, (char*)path, ce->watcher, ce->ctx);
  } else {
    ZKSetExistsWatch(zkh, (char*)path, DefaultExistsWatcher, ctx);
  }
}

// Watcher func for get().
void DefaultGetWatcher(zhandle_t *zkh,
                       int type,
                       int state,
                       const char *path,
                       void *ctx) {
  ContextGet *cg = (ContextGet*)ctx;
  fprintf(stderr, "get_watcher: got event %s, state %s at path %s, ctx=%p\n",
          Type2String(type), State2String(state), path, cg);


  if (type == ZOO_CHANGED_EVENT) {
    // only on zoo_exists and zoo_get
    char buf[4000];
    int watch = 0;
    int sync = 1;
    ZKGet(zkh, path, buf, sizeof(buf), watch, sync);

    // RE-arm watcher.
    if (cg) {
      ZKSetGetWatch(zkh, (char*)path, cg->watcher, cg->ctx);
    } else {
      ZKSetGetWatch(zkh, (char*)path, DefaultGetWatcher, ctx);
    }
  }

  if (type == ZOO_DELETED_EVENT) {
    // only on zoo_exists and zoo_get
    //zoo_awget(zkh, path, DefaultGetWatcher, (void*)path, DefaultGetCompletion, NULL);
  }

  if (type == ZOO_SESSION_EVENT) {
    // This is generated when a client loses contact
    // or reconnects with a server
  }

  if (type == ZOO_NOTWATCHING_EVENT) {
    // when a server will no longer watch a node for a client.
  }

  // only for zoo_get_children or zoo_get_children2
  // ZOO_CHILD_EVENT:
}

int ZKCreate(zhandle_t *zkh, char* path, char *value, int value_len, int flags) {
  int rc = zoo_create(zkh,
                      path,
                      value,
                      value_len,
                      &ZOO_OPEN_ACL_UNSAFE,
                      flags,
                      NULL,
                      0);
  fprintf(stderr, "create znode \"%s\", ret %d\n",
          path, rc);
  return rc;
}

int ZKDelete(zhandle_t *zkh, char* path) {
  int rc = zoo_delete(zkh, path, -1);
  fprintf(stderr, "delete znode \"%s\", ret %d\n",
          path, rc);

  return rc;
}

int ZKSet(zhandle_t *zkh, char* path, char *value, int value_len) {
  int rc = zoo_set(zkh, path, value, value_len, -1);
  fprintf(stderr, "Set znode \"%s\" value \"%s\", ret %d\n",
          path, value, rc);
  return rc;
}

// Read a znode, optionally put watcher on it.
//
// Recommend to pass sync = 0 so that we can use awget() to pass
// a customer-defined watcher func.
// If "sync == 1 and watch == 1",  will use global watcher.
int ZKGet(zhandle_t *zkh,
          const char *path,
          char *buf,
          int buf_len,
          int watch,
          int sync) {

  struct Stat stat;
  ContextGet ctx;
  int ret_buf_len;

  int rc;
  if (sync) {
    ret_buf_len = buf_len;
    rc = zoo_get(zkh, path, watch, buf, &ret_buf_len, &stat);
  } else {
    ctx.path = (char*)path;
    ctx.buf = buf;
    ctx.buf_len = buf_len;
    ctx.ret_buf_len = 0;
    ctx.watcher = DefaultGetWatcher;
    ctx.ctx = &ctx;

    pthread_mutex_init(&ctx.lock, NULL);
    pthread_mutex_lock(&ctx.lock);

    rc = zoo_awget(zkh,
                   path,
                   watch ? DefaultGetWatcher : NULL,
                   watch ? &ctx: NULL,
                   DefaultGetCompletion,
                   &ctx);

    ret_buf_len = ctx.ret_buf_len;
  }
  if (rc != ZOK) {
    return -1;
  }

  if (sync) {
    fprintf(stderr, "ZKGet() sync: path %s: ret value \"%s\"(%d)\n",
            path, buf, ret_buf_len);
  } else {
    pthread_mutex_lock(&ctx.lock);
    fprintf(stderr, "ZKGet() async: path %s: ret value \"%s\"(%d)\n",
            path, buf, ctx.ret_buf_len);
  }

  return ret_buf_len;
}


// Set a "get" watcher to znode
//
// The watcher is triggered by following events:
//   "ZOO_CHANGED_EVENT"
//   "ZOO_DELETED_EVENT"
int ZKSetGetWatch(zhandle_t *zkh, char *path, watcher_fn watcher, void* ctx) {
  int rc = zoo_awget(zkh,
                     path,
                     watcher,
                     ctx,
                     DefaultGetCompletion,
                     NULL);
  return rc;
}


// Set an "exists" watcher.
//
// The watcher is triggered by following events:
//   "ZOO_CREATED_EVENT"
//   "ZOO_DELETED_EVENT"
//   "ZOO_CHANGED_EVENT"
int ZKSetExistsWatch(zhandle_t *zkh, char *path, watcher_fn watcher, void* ctx) {
  struct Stat stat;
  int rc = zoo_wexists(zkh,
                       path,
                       watcher,
                       ctx,
                       &stat);
  return rc;
}

// Connect to zookeeper.
//
// Return: a zk handler.
zhandle_t* ZKConnect(char* zk_hosts) {
  if (connected) {
    fprintf(stderr, "zk %s already connected\n", zk_hosts);
    return NULL;
  }

  zhandle_t* zkh = NULL;
  zkh = zookeeper_init(zk_hosts,
                       GlobalWatcher,
                       15000,
                       NULL,
                       0,
                       0);
  // Wait for zk connection to established.
  while (!connected) {
    sleep(1);
  }

  return zkh;
}


// Close connection to zk.
void ZKClose(zhandle_t* zkh) {
  zookeeper_close(zkh);
  connected = 0;
  fprintf(stderr, "have closed zk...\n");
}


// Check if znode exists.
//
// Return:  1 if exists, 0 otherwise.
int ZKExists(zhandle_t *zkh, char* path) {
  struct Stat stat;
  int watch = 0;
  int rc = zoo_exists(zkh, path, watch, &stat);
  if (rc == ZOK) {
    return 1;
  } else {
    return 0;
  }
}

