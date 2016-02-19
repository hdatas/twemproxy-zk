/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdlib.h>
#include <unistd.h>

#include <nc_core.h>
#include <nc_server.h>
#include <nc_conf.h>

static struct shard*
get_shard_from_key(struct server_pool *pool, uint8_t *key, uint32_t keylen);

static void
server_resolve(struct server *server, struct conn *conn)
{
    rstatus_t status;

    status = nc_resolve(&server->addrstr, server->port, &server->info);
    if (status != NC_OK) {
        conn->err = EHOSTDOWN;
        conn->done = 1;
        return;
    }

    conn->family = server->info.family;
    conn->addrlen = server->info.addrlen;
    conn->addr = (struct sockaddr *)&server->info.addr;
}

void
server_ref(struct conn *conn, void *owner)
{
    struct server *server = owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner == NULL);

    server_resolve(server, conn);

    server->ns_conn_q++;
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, server,
              server->pname.len, server->pname.data);
}

void
server_unref(struct conn *conn)
{
    struct server *server;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->owner != NULL);

    server = conn->owner;
    conn->owner = NULL;

    ASSERT(server->ns_conn_q != 0);
    server->ns_conn_q--;
    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);

    log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
              server->pname.len, server->pname.data);
}

int
server_timeout(struct conn *conn)
{
    struct server *server;
    struct server_pool *pool;

    ASSERT(!conn->client && !conn->proxy);

    server = conn->owner;
    pool = server->owner;

    return pool->timeout;
}

bool
server_active(struct conn *conn)
{
    ASSERT(!conn->client && !conn->proxy);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

    return false;
}

static rstatus_t
server_each_set_owner(void *elem, void *data)
{
    struct server *s = elem;
    struct server_pool *sp = data;

    s->owner = sp;

    return NC_OK;
}

rstatus_t
server_init(struct array *server, struct array *conf_server,
            struct server_pool *sp)
{
    rstatus_t status;
    uint32_t nserver;

    nserver = array_n(conf_server);
    ASSERT(nserver != 0);
    ASSERT(array_n(server) == 0);

    status = array_init(server, nserver, sizeof(struct server));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf server to server */
    status = array_each(conf_server, conf_server_each_transform, server);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }
    ASSERT(array_n(server) == nserver);

    /* set server owner */
    status = array_each(server, server_each_set_owner, sp);
    if (status != NC_OK) {
        server_deinit(server);
        return status;
    }

    log_debug(LOG_DEBUG, "init %"PRIu32" servers in pool %"PRIu32" '%.*s'",
              nserver, sp->idx, sp->name.len, sp->name.data);

    return NC_OK;
}

void
server_deinit(struct array *server)
{
    uint32_t i, nserver;

    for (i = 0, nserver = array_n(server); i < nserver; i++) {
        struct server *s;

        s = array_pop(server);
        ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
    }
    array_deinit(server);
}

struct conn *
server_conn(struct server *server)
{
    struct server_pool *pool;
    struct conn *conn;

    pool = server->owner;

    /*
     * FIXME: handle multiple server connections per server and do load
     * balancing on it. Support multiple algorithms for
     * 'server_connections:' > 0 key
     */

    if (server->ns_conn_q < pool->server_connections) {
        return conn_get(server, false, pool->redis);
    }
    ASSERT(server->ns_conn_q == pool->server_connections);

    /*
     * Pick a server connection from the head of the queue and insert
     * it back into the tail of queue to maintain the lru order
     */
    conn = TAILQ_FIRST(&server->s_conn_q);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
    TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

    return conn;
}

static rstatus_t
server_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server *server;
    struct server_pool *pool;
    struct conn *conn;

    server = elem;
    pool = server->owner;

    conn = server_conn(server);
    if (conn == NULL) {
        return NC_ENOMEM;
    }

    status = server_connect(pool->ctx, server, conn);
    if (status != NC_OK) {
        log_warn("connect to server '%.*s' failed, ignored: %s",
                 server->pname.len, server->pname.data, strerror(errno));
        server_close(pool->ctx, conn);
    }

    return NC_OK;
}

static rstatus_t
server_each_disconnect(void *elem, void *data)
{
    struct server *server;
    struct server_pool *pool;

    server = elem;
    pool = server->owner;

    while (!TAILQ_EMPTY(&server->s_conn_q)) {
        struct conn *conn;

        ASSERT(server->ns_conn_q > 0);

        conn = TAILQ_FIRST(&server->s_conn_q);
        conn->close(pool->ctx, conn);
    }

    return NC_OK;
}

static void
server_failure(struct context *ctx, struct server *server)
{
    struct server_pool *pool = server->owner;
    int64_t now, next;
    rstatus_t status;

    if (!pool->auto_eject_hosts) {
        return;
    }

    server->failure_count++;

    log_debug(LOG_VERB, "server '%.*s' failure count %"PRIu32" limit %"PRIu32,
              server->pname.len, server->pname.data, server->failure_count,
              pool->server_failure_limit);

    if (server->failure_count < pool->server_failure_limit) {
        return;
    }

    now = nc_usec_now();
    if (now < 0) {
        return;
    }

    stats_server_set_ts(ctx, server, server_ejected_at, now);

    next = now + pool->server_retry_timeout;

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to delete server '%.*s' "
              "for next %"PRIu32" secs", pool->idx, pool->name.len,
              pool->name.data, server->pname.len, server->pname.data,
              pool->server_retry_timeout / 1000 / 1000);

    stats_pool_incr(ctx, pool, server_ejects);

    server->failure_count = 0;
    server->next_retry = next;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" '%.*s' failed: %s", pool->idx,
                  pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
server_close_stats(struct context *ctx, struct server *server, err_t err,
                   unsigned eof, unsigned connected)
{
    if (connected) {
        stats_server_decr(ctx, server, server_connections);
    }

    if (eof) {
        stats_server_incr(ctx, server, server_eof);
        return;
    }

    switch (err) {
    case ETIMEDOUT:
        stats_server_incr(ctx, server, server_timedout);
        break;
    case EPIPE:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
        stats_server_incr(ctx, server, server_err);
        break;
    }
}

void
server_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */
    struct conn *c_conn;    /* peer client connection */

    ASSERT(!conn->client && !conn->proxy);

    server_close_stats(ctx, conn->owner, conn->err, conn->eof,
                       conn->connected);

    conn->connected = false;

    if (conn->sd < 0) {
        server_failure(ctx, conn->owner);
        conn->unref(conn);
        conn_put(conn);
        return;
    }

    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server inq */
        conn->dequeue_inq(ctx, conn, msg);

        /*
         * Don't send any error response, if
         * 1. request is tagged as noreply or,
         * 2. client has already closed its connection
         */
        if (msg->swallow || msg->noreply) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;

            if (msg->frag_owner != NULL) {
                msg->frag_owner->nfrag_done++;
            }

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from server outq */
        conn->dequeue_outq(ctx, conn, msg);

        if (msg->swallow) {
            log_debug(LOG_INFO, "close s %d swallow req %"PRIu64" len %"PRIu32
                      " type %d", conn->sd, msg->id, msg->mlen, msg->type);
            req_put(msg);
        } else {
            c_conn = msg->owner;
            ASSERT(c_conn->client && !c_conn->proxy);

            msg->done = 1;
            msg->error = 1;
            msg->err = conn->err;
            if (msg->frag_owner != NULL) {
                msg->frag_owner->nfrag_done++;
            }

            if (req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
                event_add_out(ctx->evb, msg->owner);
            }

            log_debug(LOG_INFO, "close s %d schedule error for req %"PRIu64" "
                      "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
                      msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
                      conn->err ? strerror(conn->err): " ");
        }
    }
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(!msg->request);
        ASSERT(msg->peer == NULL);

        rsp_put(msg);

        log_debug(LOG_INFO, "close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                  "in error", conn->sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    server_failure(ctx, conn->owner);

    conn->unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

rstatus_t
server_connect(struct context *ctx, struct server *server, struct conn *conn)
{
    rstatus_t status;

    ASSERT(!conn->client && !conn->proxy);

    if (conn->err) {
      ASSERT(conn->done && conn->sd < 0);
      errno = conn->err;
      return NC_ERROR;
    }

    if (conn->sd > 0) {
        /* already connected on server connection */
        return NC_OK;
    }

    log_debug(LOG_VVERB, "connect to server '%.*s'", server->pname.len,
              server->pname.data);

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("socket for server '%.*s' failed: %s", server->pname.len,
                  server->pname.data, strerror(errno));
        status = NC_ERROR;
        goto error;
    }

    status = nc_set_nonblocking(conn->sd);
    if (status != NC_OK) {
        log_error("set nonblock on s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    if (server->pname.data[0] != '/') {
        status = nc_set_tcpnodelay(conn->sd);
        if (status != NC_OK) {
            log_warn("set tcpnodelay on s %d for server '%.*s' failed, ignored: %s",
                     conn->sd, server->pname.len, server->pname.data,
                     strerror(errno));
        }
    }

    status = event_add_conn(ctx->evb, conn);
    if (status != NC_OK) {
        log_error("event add conn s %d for server '%.*s' failed: %s",
                  conn->sd, server->pname.len, server->pname.data,
                  strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);
    if (status != NC_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "connecting on s %d to server '%.*s'",
                      conn->sd, server->pname.len, server->pname.data);
            return NC_OK;
        }

        log_error("connect on s %d to server '%.*s' failed: %s", conn->sd,
                  server->pname.len, server->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);

    return NC_OK;

error:
    conn->err = errno;
    return status;
}

void
server_connected(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connecting && !conn->connected);

    stats_server_incr(ctx, server, server_connections);

    conn->connecting = 0;
    conn->connected = 1;

    conn->post_connect(ctx, conn, server);

    log_debug(LOG_INFO, "connected on s %d to server '%.*s'", conn->sd,
              server->pname.len, server->pname.data);
}

void
server_ok(struct context *ctx, struct conn *conn)
{
    struct server *server = conn->owner;

    ASSERT(!conn->client && !conn->proxy);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "reset server '%.*s' failure count from %"PRIu32
                  " to 0", server->pname.len, server->pname.data,
                  server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0LL;
    }
}

static rstatus_t
server_pool_update(struct server_pool *pool)
{
    rstatus_t status;
    int64_t now;
    uint32_t pnlive_server; /* prev # live server */

    if (!pool->auto_eject_hosts) {
        return NC_OK;
    }

    if (pool->next_rebuild == 0LL) {
        return NC_OK;
    }

    now = nc_usec_now();
    if (now < 0) {
        return NC_ERROR;
    }

    if (now <= pool->next_rebuild) {
        if (pool->nlive_server == 0) {
            errno = ECONNREFUSED;
            return NC_ERROR;
        }
        return NC_OK;
    }

    pnlive_server = pool->nlive_server;

    status = server_pool_run(pool);
    if (status != NC_OK) {
        log_error("updating pool %"PRIu32" with dist %d failed: %s", pool->idx,
                  pool->dist_type, strerror(errno));
        return status;
    }

    log_debug(LOG_INFO, "update pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
              pool->idx, pool->name.len, pool->name.data,
              pool->nlive_server - pnlive_server);


    return NC_OK;
}

static uint32_t
server_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL);

    if (array_n(&pool->server) == 1) {
        return 0;
    }

    if (keylen == 0) {
        return 0;
    }

    return pool->key_hash((char *)key, keylen);
}

uint32_t
server_pool_idx(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    uint32_t hash, idx;

    ///////////////////
    // At init, a shard's master/slave may be empty, so we allow a pool without
    // any servers. Later on ZK update will add servers.
    // However, a pool must contain some shards from the beginnning.
    //ASSERT(array_n(&pool->server) != 0);
    ASSERT(key != NULL);

    /*
     * If hash_tag: is configured for this server pool, we use the part of
     * the key within the hash tag as an input to the distributor. Otherwise
     * we use the full key
     */
    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = nc_strchr(key, key + keylen, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = nc_strchr(tag_start + 1, key + keylen, tag->data[1]);
            if ((tag_end != NULL) && (tag_end - tag_start > 1)) {
                key = tag_start + 1;
                keylen = (uint32_t)(tag_end - key);
            }
        }
    }

    ///////// distribute key to backend servers based on hash code
    struct shard* sd = get_shard_from_key(pool, key, keylen);
    ASSERT(sd != NULL);
    // always direct traffic to master shard.
    // Returns the maste shard id.
    return sd->idx;

    ////////////////////

    switch (pool->dist_type) {
    case DIST_KETAMA:
        hash = server_pool_hash(pool, key, keylen);
        idx = ketama_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_MODULA:
        hash = server_pool_hash(pool, key, keylen);
        idx = modula_dispatch(pool->continuum, pool->ncontinuum, hash);
        break;

    case DIST_RANDOM:
        idx = random_dispatch(pool->continuum, pool->ncontinuum, 0);
        break;

    default:
        NOT_REACHED();
        return 0;
    }
    ASSERT(idx < array_n(&pool->server));
    return idx;
}

static struct server *
server_pool_server(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    struct server *server;
    uint32_t idx;

    idx = server_pool_idx(pool, key, keylen);
    server = array_get(&pool->server, idx);

    log_debug(LOG_VERB, "key '%.*s' on dist %d maps to server '%.*s'", keylen,
              key, pool->dist_type, server->pname.len, server->pname.data);

    return server;
}

// decide what shard a key belongs to within a server_pool.
static struct shard*
get_shard_from_key(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
    uint32_t nshards = array_n(&pool->shards);

    if (nshards == 0) {
        return NULL;
    }

    if (nshards == 1 || keylen == 0) {
        log_debug(LOG_NOTICE, "case 1: key %s => shard %d", (char*)key, 0);
        return (struct shard*)array_get(&pool->shards, 0);;
    }

    //uint32_t hv = pool->key_hash((char *)key, keylen);
    //uint32_t hv = hash_murmur((char *)key, keylen);
    //uint32_t hv = hash_crc32((char *)key, keylen);
    //uint32_t hv = hash_hsieh((char *)key, keylen);
    uint32_t hv = hash_jenkins((char *)key, keylen);

    hv = hv % pool->shard_range_max;
    if (hv < pool->shard_range_min) {
        hv = pool->shard_range_min;
    }

    // TODO: use a binary search tree to locate a shard.

    for (uint32_t i = 0; i < nshards; i++) {
        struct shard* sd = (struct shard*)array_get(&pool->shards, i);
        if (sd->range_begin <= hv && hv <= sd->range_end) {
            log_debug(LOG_NOTICE, "shard-search: key %s, hv %d, "
                      "%d shards, map to shard %d (idx %d)",
                      (char*)key, hv, nshards, i, sd->idx);
            return sd;
        }
    }
    return NULL;
}

struct conn *
server_pool_conn(struct context *ctx, struct server_pool *pool, uint8_t *key,
                 uint32_t keylen)
{
    rstatus_t status;
    struct server *server;
    struct conn *conn;

    //////////////////////////////////////
    /*status = server_pool_update(pool);
    if (status != NC_OK) {
        return NULL;
    }*/

    /* from a given {key, keylen} pick a server from pool */
    /*server = server_pool_server(pool, key, keylen);
    if (server == NULL) {
        return NULL;
    }*/
    //////////////////////////////////////

    ///////////////
    // hash(key) =>  shard => master server in shard
    struct shard* sd = get_shard_from_key(pool, key, keylen);
    ASSERT(sd != NULL);
    // always direct traffic to master.
    server = sd->master;
    if (!server) {
      return NULL;
    }
    /////////////////

    /* pick a connection to a given server */
    conn = server_conn(server);
    if (conn == NULL) {
        return NULL;
    }

    status = server_connect(ctx, server, conn);
    if (status != NC_OK) {
        server_close(ctx, conn);
        return NULL;
    }

    return conn;
}

static rstatus_t
server_pool_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    if (!sp->preconnect) {
        return NC_OK;
    }

    // Return OK if no server is avail at beginning, so rest of logic and
    // continue.
    if (array_n(&sp->server) == 0) {
      return NC_OK;
    }

    status = array_each(&sp->server, server_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

rstatus_t
server_pool_preconnect(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, server_pool_each_preconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_disconnect(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    status = array_each(&sp->server, server_each_disconnect, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}

void
server_pool_disconnect(struct context *ctx)
{
    array_each(&ctx->pool, server_pool_each_disconnect, NULL);
}

static rstatus_t
server_pool_each_set_owner(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    sp->ctx = ctx;

    return NC_OK;
}

static rstatus_t
server_pool_each_calc_connections(void *elem, void *data)
{
    struct server_pool *sp = elem;
    struct context *ctx = data;

    ctx->max_nsconn += sp->server_connections * array_n(&sp->server);
    ctx->max_nsconn += 1; /* pool listening socket */

    return NC_OK;
}

rstatus_t
server_pool_run(struct server_pool *pool)
{
    //////
    return NC_OK;
    ASSERT(array_n(&pool->shards) > 0 &&
           array_n(&pool->server) > 0);
    return NC_OK;
    //////

    ASSERT(array_n(&pool->server) != 0);

    switch (pool->dist_type) {
    case DIST_KETAMA:
        return ketama_update(pool);

    case DIST_MODULA:
        return modula_update(pool);

    case DIST_RANDOM:
        return random_update(pool);

    default:
        NOT_REACHED();
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
server_pool_each_run(void *elem, void *data)
{
    return server_pool_run(elem);
}

// Add a server to the given shard.
//
// "saddr" is "ip:port" string representing the new server address.
struct server*
AddServerFromAddressString(struct shard *sd, char *saddr)
{
    // Parse the "ip:port" string.
    uint32_t len = (uint32_t)strlen(saddr);
    uint8_t *s_name, *s_port;

    uint8_t *delim = nc_strchr((uint8_t*)saddr,
                               (uint8_t*)(saddr + len),
                               ':');
    ASSERT(delim != NULL);
    s_name = (uint8_t*)saddr;
    s_port = delim + 1;

    // Grab a struct server.
    struct server_pool *sp = sd->owner;
    struct server *srv = array_push(&sp->server);

    ASSERT(srv != NULL);
    memset(srv, 0, sizeof(*srv));

    // Init server fields.
    srv->idx = array_idx(&sp->server, srv);
    srv->owner = sp;
    srv->owner_shard = sd;

    string_copy(&srv->pname, (const uint8_t*)saddr, len);
    string_copy(&srv->name, (const uint8_t*)saddr, len);
    string_copy(&srv->addrstr,
                (const uint8_t*)saddr,
                (uint32_t)(delim - s_name));
    srv->port = (uint16_t)nc_atoi(s_port, (uint32_t)(len - (delim + 1 - s_name)));
    srv->weight = 0;

    rstatus_t status = nc_resolve(&srv->addrstr, srv->port, &srv->info);
    if (status != NC_OK) {
        string_deinit(&srv->pname);
        string_deinit(&srv->name);
        string_deinit(&srv->addrstr);
        array_pop(&sp->server);
        return NULL;
    }

    srv->ns_conn_q = 0;
    TAILQ_INIT(&srv->s_conn_q);

    srv->next_retry = 0LL;
    srv->failure_count = 0;

    return srv;
}

// When a new "struct server" is added to a pool,
// the stats_pool should be updated to include
// an entry for this new server.
void
add_server_to_stats(struct server *srv, struct server_pool *pool)
{
  struct context *ctx = pool->ctx;

  struct stats_pool *stp;
  struct stats *st = ctx->stats;

  if (st->curr_servers >= st->max_allowed_servers) {
    log_error("stats curr servers %d >= max_allowed servers %d, cannot add more",
              st->curr_servers, st->max_allowed_servers);
    return;
  }

  stats_lock(st);
  st->curr_servers++;

  stp = array_get(&st->current, pool->idx);
  add_server_to_stats_pool(stp, srv);
  stp = array_get(&st->shadow, pool->idx);
  add_server_to_stats_pool(stp, srv);
  stp = array_get(&st->sum, pool->idx);
  add_server_to_stats_pool(stp, srv);

  stats_unlock(st);
}

// a shard's master address has changed.
static void
MasterAddressWatcher(zhandle_t *zkh,
                     int type,
                     int state,
                     const char *path,
                     void *ctx)
{
    struct shard *srv_sd = (struct shard *)ctx;
    ASSERT(srv_sd != NULL);
    struct server_pool *pool = srv_sd->owner;
    struct context *pool_ctx = pool->ctx;
    uint32_t pool_idx = array_idx(&pool_ctx->pool, pool);
    ASSERT(pool_idx == pool->idx);

    int buflen = 1000;
    char buf[buflen];
    int rc;

    log_error("Master Address Watcher got event %s, state %s at path %s",
              Type2String(type), State2String(state), path);

    if (type == ZOO_CREATED_EVENT ||
        type == ZOO_CHANGED_EVENT) {
      memset(buf, 0, (size_t)buflen);
      rc = ZKGet(zkh, path, buf, buflen, 0, 1);
      if (rc < 0) {
          log_error("read %s failed!", path);
      } else {
          struct server *currsrv = srv_sd->master;
          if (strlen(buf) != (size_t)currsrv->name.len ||
              strncmp(buf, (void*)currsrv->name.data, strlen(buf)) != 0) {

              // Master address has changed. Switch to a new master.
              struct server *srv = AddServerFromAddressString(srv_sd, buf);
              add_server_to_stats(srv, pool);

              // Add a corresponding stats_server.
              /*
              struct stats_pool *stp;
              stp = array_get(&pool_ctx->stats->current, pool_idx);
              add_server_to_stats_pool(stp, srv);
              stp = array_get(&pool_ctx->stats->shadow, pool_idx);
              add_server_to_stats_pool(stp, srv);
              stp = array_get(&pool_ctx->stats->sum, pool_idx);
              add_server_to_stats_pool(stp, srv);
              */

              // TODO: should use a lock to protect ?

              // TODO: retire original master ?

              // !!! Switch to a new master.
              srv_sd->master = srv;
              if (!srv_sd->can_write) {
                  srv_sd->can_write = 1;
              }

              log_error("Use a new master %s for shard %s",
                        srv->pname.data, path);
          }
      }
    } else if (type == ZOO_DELETED_EVENT) {

    }

    ZKSetExistsWatch(zkh, (char*)path, MasterAddressWatcher, ctx);
}


// a shard's master srv has changed its status.
static void
MasterStatusWatcher(zhandle_t *zkh,
                    int type,
                    int state,
                    const char *path,
                    void *ctx)
{
    struct shard *srv_sd = (struct shard *)ctx;
    ASSERT(srv_sd != NULL);

    int buflen = 1000;
    char buf[buflen];
    int rc;

    log_error("Status Watcher got event %s, state %s at path %s",
              Type2String(type), State2String(state), path);
    if (type == ZOO_CREATED_EVENT ||
        type == ZOO_CHANGED_EVENT) {
      rc = ZKGet(zkh, path, buf, buflen, 0, 1);
      if (rc < 0) {
          log_error("read %s failed!", path);
      } else {
          if (strncmp(buf, "draining", 8) == 0) {
              // should drain this server shard.
              srv_sd->can_write = 0;
              log_error("Turn shard %s to read-only.", path);
          } else if (strncmp(buf, "running", 7) == 0) {
              srv_sd->can_write = 1;
              log_error("Turn shard %s to read-write.", path);
          }
      }
    } else if (type == ZOO_DELETED_EVENT) {

    }

    ZKSetExistsWatch(zkh, (char*)path, MasterStatusWatcher, ctx);
}

// Set zk watcher on each master's "status", "address".
//
// Whenever a shard's master status or address changes,
// watcher is triggered to take action:
// if "status" == draining:  block all writes to the master.
// if "address" changes, replace the master with a new server.
static void
set_watch_on_master_status(struct array *server_pool, struct context *ctx)
{
    int buflen = 5000;
    char zkpath[buflen];
    char buf[buflen];
    int rc;

    for (uint32_t i = 0; i < array_n(server_pool); i++ ) {
        struct server_pool *sp =
          (struct server_pool*)array_get(server_pool, i);

        for (uint32_t j = 0; j < array_n(&sp->shards); j++ ) {
            struct shard *srv_sd = (struct shard*)array_get(&sp->shards, j);
            struct server *srv = srv_sd->master;
            ASSERT(srv->owner_shard == srv_sd);

            //sprintf(zkpath, "%s/pools/%s/shards/%d:%d/master/status",
            //        ZK_BASE,
            //        sp->name.data,
            //        srv_sd->range_begin,
            //        srv_sd->range_end);
            sprintf(zkpath, "%s/hosts/%s/status",
                    ZK_BASE,
                    srv->name.data);

            memset(buf, 0, (size_t)buflen);
            rc = ZKGet(ctx->zkh, zkpath, buf, buflen, 0, 1);
            if (rc <= 0) {
                log_error("master %s status non-exist: %s",
                          srv->name.data, zkpath);
            } else {
                log_debug(LOG_NOTICE, "master %s status znode %s: %s",
                          srv->name.data, zkpath, buf);
            }
            // Set watch on master "status".
            rc = ZKSetExistsWatch(ctx->zkh, zkpath, MasterStatusWatcher, srv_sd);
            log_debug(LOG_NOTICE, "set watch on master %s status: %s",
                      srv->name.data, zkpath);

            // Set watch on master "address".
            sprintf(zkpath, "%s/pools/%s/shards/%d:%d/master/addr",
                    ZK_BASE,
                    sp->name.data,
                    srv_sd->range_begin,
                    srv_sd->range_end);
            rc = ZKSetExistsWatch(ctx->zkh, zkpath, MasterAddressWatcher, srv_sd);
            log_debug(LOG_NOTICE, "set watch on master %s address: %s",
                      srv->name.data, zkpath);
        }
        // Create an ephemeral znode for this pool:proxy
        sprintf(zkpath, "%s/pools/%s/proxies/%s",
                ZK_BASE, sp->name.data, sp->addrstr.data);
        rc = ZKCreate(ctx->zkh, zkpath, NULL, -1, ZOO_EPHEMERAL);
        log_debug(LOG_NOTICE, "created proxy marker znode %s, ret %d",
                  zkpath, rc);
    }
}

rstatus_t
server_pool_init(struct array *server_pool, struct array *conf_pool,
                 struct context *ctx)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(conf_pool);
    ASSERT(npool != 0);
    ASSERT(array_n(server_pool) == 0);

    status = array_init(server_pool, npool, sizeof(struct server_pool));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf pool to server pool */
    status = array_each(conf_pool, conf_pool_each_transform, server_pool);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }
    ASSERT(array_n(server_pool) == npool);

    /* set ctx as the server pool owner */
    status = array_each(server_pool, server_pool_each_set_owner, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* compute max server connections */
    ctx->max_nsconn = 0;
    status = array_each(server_pool, server_pool_each_calc_connections, ctx);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    /* update server pool continuum */
    status = array_each(server_pool, server_pool_each_run, NULL);
    if (status != NC_OK) {
        server_pool_deinit(server_pool);
        return status;
    }

    // Set a watcher to each shard's master server status.
    //set_watch_on_master_status(server_pool, ctx);
    add_watcher_on_conf_pool(ctx);

    log_debug(LOG_DEBUG, "init %"PRIu32" pools", npool);

    return NC_OK;
}

void
server_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->nc_conn_q == 0);

        if (sp->continuum != NULL) {
            nc_free(sp->continuum);
            sp->ncontinuum = 0;
            sp->nserver_continuum = 0;
            sp->nlive_server = 0;
        }

        server_deinit(&sp->server);

        log_debug(LOG_DEBUG, "deinit pool %"PRIu32" '%.*s'", sp->idx,
                  sp->name.len, sp->name.data);
    }

    array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" pools", npool);
}

void
dump_server(struct server *srv)
{
  log_warn("server %s (idx %d)",
           (char*)srv->name.data, srv->idx);
}

void
dump_server_shard(struct shard *sd)
{
  struct server *master = sd->master;
  log_warn("shard %d [%d : %d]: have %d master, %d slaves, "
           "can-read %d, can-write %d",
           sd->idx, sd->range_begin, sd->range_end,
           master ? 1 : 0,
           array_n(&sd->slaves),
           sd->can_read,
           sd->can_write);
  if (master) {
    log_warn("master :");
    dump_server(master);
  }

  for (uint32_t i = 0; i < array_n(&sd->slaves); i++) {
    log_warn("slave %d:", i);
    dump_server(*(struct server**)array_get(&sd->slaves, i));
  }
}

void
dump_server_pool(struct server_pool *sp)
{
  log_warn("");
  log_warn("pool %s (idx %d), have %d shards, %d servers",
           (char*)sp->name.data, sp->idx,
           array_n(&sp->shards), array_n(&sp->server));
  log_warn("proxy addr %s, shard range [%d : %d]",
           (char*)sp->addrstr.data,
           sp->shard_range_min,
           sp->shard_range_max);

  for (uint32_t i = 0; i < array_n(&sp->shards); i++) {
    log_warn("pool \"%s\" shard %d ::", (char*)sp->name.data, i);
    dump_server_shard((struct shard*)array_get(&sp->shards, i));
  }

  log_warn("");

}
