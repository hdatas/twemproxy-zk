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

#include <nc_core.h>
#include <nc_conf.h>
#include <nc_server.h>
#include <proto/nc_proto.h>
#include "parson/parson.h"
#include "zkutil.h"

#define DEFINE_ACTION(_hash, _name) string(#_name),
static struct string hash_strings[] = {
    HASH_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_t hash_algos[] = {
    HASH_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) string(#_name),
static struct string dist_strings[] = {
    DIST_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

static void conf_server_to_conf_listen(struct conf_server* cs,
                                       struct conf_listen* cl);
static rstatus_t string_to_conf_server(const uint8_t* saddr,
                                       struct conf_server* srv);
static rstatus_t conf_shards_to_server_shards(struct array* cf_shards,
                                              struct array* srv_shards,
                                              struct server_pool* sp);
static rstatus_t parse_pool_conf_file(struct conf *cf, char *pool_name);
static rstatus_t conf_json_to_conf_pool(JSON_Object *pobj,
                                        struct conf_pool *pool,
                                        char *pool_name);
static bool sanity_check_pool_conf_json(JSON_Object *pobj);
static bool sanity_check_shard_conf_json(JSON_Object *sobj);
static void display_server_shard(struct shard *srv_sd);
static rstatus_t conf_shard_to_server_shard(struct conf_shard *conf_sd,
                                            struct shard *srv_sd,
                                            struct server_pool *sp);
static struct server* find_server_from_conf_server(struct conf_server *conf_srv,
                                                   struct array *srvs);
static bool conf_match_server(struct conf_server *conf_srv, struct server *srv);
static void set_server_shard_status(struct conf_shard *conf_sd,
                                    struct shard *srv_sd);
static bool is_valid_conf_server(struct conf_server *conf_srv);
static rstatus_t update_server_shards_from_conf_json(JSON_Object *pobj,
                                    struct array *srv_shards,
                                    struct server_pool *sp);

static struct command conf_commands[] = {
    { string("listen"),
      conf_set_listen,
      offsetof(struct conf_pool, listen) },

    { string("hash"),
      conf_set_hash,
      offsetof(struct conf_pool, hash) },

    { string("hash_tag"),
      conf_set_hashtag,
      offsetof(struct conf_pool, hash_tag) },

    { string("distribution"),
      conf_set_distribution,
      offsetof(struct conf_pool, distribution) },

    { string("timeout"),
      conf_set_num,
      offsetof(struct conf_pool, timeout) },

    { string("backlog"),
      conf_set_num,
      offsetof(struct conf_pool, backlog) },

    { string("client_connections"),
      conf_set_num,
      offsetof(struct conf_pool, client_connections) },

    { string("redis"),
      conf_set_bool,
      offsetof(struct conf_pool, redis) },

    { string("tcpkeepalive"),
      conf_set_bool,
      offsetof(struct conf_pool, tcpkeepalive) },

    { string("redis_auth"),
      conf_set_string,
      offsetof(struct conf_pool, redis_auth) },

    { string("redis_db"),
      conf_set_num,
      offsetof(struct conf_pool, redis_db) },

    { string("preconnect"),
      conf_set_bool,
      offsetof(struct conf_pool, preconnect) },

    { string("auto_eject_hosts"),
      conf_set_bool,
      offsetof(struct conf_pool, auto_eject_hosts) },

    { string("server_connections"),
      conf_set_num,
      offsetof(struct conf_pool, server_connections) },

    { string("server_retry_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, server_retry_timeout) },

    { string("server_failure_limit"),
      conf_set_num,
      offsetof(struct conf_pool, server_failure_limit) },

    { string("servers"),
      conf_add_server,
      offsetof(struct conf_pool, server) },

    null_command
};

static void
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    string_init(&cs->addrstr);
    cs->port = 0;
    cs->weight = 0;

    memset(&cs->info, 0, sizeof(cs->info));

    cs->valid = 0;

    log_debug(LOG_VVERB, "init conf server %p", cs);
}

static void
conf_server_deinit(struct conf_server *cs)
{
    string_deinit(&cs->pname);
    string_deinit(&cs->name);
    string_deinit(&cs->addrstr);
    cs->valid = 0;
    log_debug(LOG_VVERB, "deinit conf server %p", cs);
}

static void
conf_shard_deinit(struct conf_shard *cs) {
    conf_server_deinit(&cs->master);
    while (array_n(&cs->slaves) != 0) {
      conf_server_deinit(array_pop(&cs->slaves));
    }
    array_deinit(&cs->slaves);
}

static rstatus_t
conf_server_to_server(struct conf_server *cs, struct server *s)
{
    // a struct server owner is a srv_pool.
    s->owner = NULL;

    string_init(&s->pname);
    string_init(&s->name);
    string_init(&s->addrstr);
    string_duplicate(&s->pname, &cs->pname);
    string_duplicate(&s->name, &cs->name);
    string_duplicate(&s->addrstr, &cs->addrstr);
    //s->pname = cs->pname;
    //s->name = cs->name;
    //s->addrstr = cs->addrstr;

    s->port = (uint16_t)cs->port;
    s->weight = (uint32_t)cs->weight;

    nc_memcpy(&s->info, &cs->info, sizeof(cs->info));

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    log_debug(LOG_VERB, "transform to server %"PRIu32" '%.*s'",
              s->idx, s->pname.len, s->pname.data);

    return NC_OK;
}

// Conver a conf_server to real backend server.
rstatus_t
conf_server_each_transform(void *elem, void *data)
{
    struct conf_server *cs = elem;
    struct array *server = data;
    struct server *s;

    ASSERT(cs->valid);

    s = array_push(server);
    ASSERT(s != NULL);

    s->idx = array_idx(server, s);
    s->owner = NULL;

    s->pname = cs->pname;
    s->name = cs->name;
    s->addrstr = cs->addrstr;
    s->port = (uint16_t)cs->port;
    s->weight = (uint32_t)cs->weight;

    nc_memcpy(&s->info, &cs->info, sizeof(cs->info));

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;

    log_debug(LOG_VERB, "transform to server %"PRIu32" '%.*s'",
              s->idx, s->pname.len, s->pname.data);

    return NC_OK;
}

static rstatus_t
conf_pool_init(struct conf_pool *cp, struct string *name)
{
    rstatus_t status;

    string_init(&cp->name);

    string_init(&cp->listen.pname);
    string_init(&cp->listen.name);
    string_init(&cp->redis_auth);
    cp->listen.port = 0;
    memset(&cp->listen.info, 0, sizeof(cp->listen.info));
    cp->listen.valid = 0;

    cp->hash = CONF_UNSET_HASH;
    string_init(&cp->hash_tag);
    cp->distribution = CONF_UNSET_DIST;

    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;

    cp->client_connections = CONF_UNSET_NUM;

    cp->redis = CONF_UNSET_NUM;
    cp->tcpkeepalive = CONF_UNSET_NUM;
    cp->redis_db = CONF_UNSET_NUM;
    cp->preconnect = CONF_UNSET_NUM;
    cp->auto_eject_hosts = CONF_UNSET_NUM;
    cp->server_connections = CONF_UNSET_NUM;
    cp->server_retry_timeout = CONF_UNSET_NUM;
    cp->server_failure_limit = CONF_UNSET_NUM;

    array_null(&cp->server);

    cp->valid = 0;

    status = string_duplicate(&cp->name, name);
    if (status != NC_OK) {
        return status;
    }

    status = array_init(&cp->server, CONF_DEFAULT_SERVERS,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
        string_deinit(&cp->name);
        return status;
    }

    log_debug(LOG_VVERB, "init conf pool %p, '%.*s'", cp, name->len, name->data);

    return NC_OK;
}

static void
conf_pool_deinit(struct conf_pool *cp)
{
    string_deinit(&cp->name);

    string_deinit(&cp->listen.pname);
    string_deinit(&cp->listen.name);

    if (cp->redis_auth.len > 0) {
        string_deinit(&cp->redis_auth);
    }

    while (array_n(&cp->server) != 0) {
        conf_server_deinit(array_pop(&cp->server));
    }
    array_deinit(&cp->server);

    while (array_n(&cp->proxies) != 0) {
        //string_deinit(array_pop(&cp->proxies));
        conf_server_deinit(array_pop(&cp->proxies));
    }
    array_deinit(&cp->proxies);

    while (array_n(&cp->shards) != 0) {
        conf_shard_deinit(array_pop(&cp->shards));
    }
    array_deinit(&cp->shards);

    log_debug(LOG_VVERB, "deinit conf pool %p", cp);
}

rstatus_t
conf_pool_each_transform(void *elem, void *data)
{
    rstatus_t status;
    struct conf_pool *cp = elem;
    struct array *server_pool = data;
    struct server_pool *sp;

    ASSERT(cp->valid);

    sp = array_push(server_pool);
    ASSERT(sp != NULL);

    sp->idx = array_idx(server_pool, sp);
    sp->ctx = NULL;

    sp->p_conn = NULL;
    sp->nc_conn_q = 0;
    TAILQ_INIT(&sp->c_conn_q);

    array_null(&sp->server);
    sp->ncontinuum = 0;
    sp->nserver_continuum = 0;
    sp->continuum = NULL;
    sp->nlive_server = 0;
    sp->next_rebuild = 0LL;

    sp->name = cp->name;
    sp->addrstr = cp->listen.pname;
    sp->port = (uint16_t)cp->listen.port;

    nc_memcpy(&sp->info, &cp->listen.info, sizeof(cp->listen.info));
    sp->perm = cp->listen.perm;

    sp->key_hash_type = cp->hash;
    sp->key_hash = hash_algos[cp->hash];
    sp->dist_type = cp->distribution;
    sp->hash_tag = cp->hash_tag;

    sp->tcpkeepalive = cp->tcpkeepalive ? 1 : 0;

    sp->redis = cp->redis ? 1 : 0;
    sp->timeout = cp->timeout;
    sp->backlog = cp->backlog;
    sp->redis_db = cp->redis_db;

    sp->redis_auth = cp->redis_auth;
    sp->require_auth = cp->redis_auth.len > 0 ? 1 : 0;

    sp->client_connections = (uint32_t)cp->client_connections;
    sp->server_connections = (uint32_t)cp->server_connections;
    sp->server_retry_timeout = (int64_t)cp->server_retry_timeout * 1000LL;
    sp->server_failure_limit = (uint32_t)cp->server_failure_limit;
    sp->auto_eject_hosts = cp->auto_eject_hosts ? 1 : 0;
    sp->preconnect = cp->preconnect ? 1 : 0;

    // Init the per-pool lock.
    pthread_mutex_init(&sp->lock, NULL);

    // Init sp->server[] and server_shards in "conf_shards_to_server_shards".

    /*status = server_init(&sp->server, &cp->server, sp);
    if (status != NC_OK) {
        return status;
    }*/

    sp->shard_range_min = cp->shard_range_min;
    sp->shard_range_max = cp->shard_range_max;

    log_debug(LOG_VERB, "transform to pool %"PRIu32" '%.*s'", sp->idx,
              sp->name.len, sp->name.data);

    // Populate server shards (and servers per shard) from conf-shards.
    log_debug(LOG_NOTICE, "init shards at pool %d (\"%s\")", sp->idx, sp->name.data);
    //array_init(&sp->shards, array_n(&cp->shards), sizeof(struct shard));
    // "cp->shards" may be empty when we start from an empty pool.
    array_init(&sp->shards, 16, sizeof(struct shard));
    //status = array_each(&cp->shards, conf_shard_each_server_shard, &sp->shards);
    status = conf_shards_to_server_shards(&cp->shards, &sp->shards, sp);
    if (status != NC_OK) {
        return status;
    }

    struct shard* srv_sd = NULL;
    for (uint32_t i = 0; i < array_n(&sp->shards); i++) {
        srv_sd = (struct shard*) array_get(&sp->shards, i);
        display_server_shard(srv_sd);
    }

    dump_server_pool(sp);

    return NC_OK;
}

static void
conf_dump(struct conf *cf)
{
    uint32_t i, j, npool, nserver;
    struct conf_pool *cp;
    struct string *s;

    npool = array_n(&cf->pool);
    if (npool == 0) {
        return;
    }

    log_debug(LOG_VVERB, "%"PRIu32" pools in configuration file '%s'", npool,
              cf->fname);

    for (i = 0; i < npool; i++) {
        cp = array_get(&cf->pool, i);

        log_debug(LOG_VVERB, "%.*s", cp->name.len, cp->name.data);
        log_debug(LOG_VVERB, "  listen: %.*s",
                  cp->listen.pname.len, cp->listen.pname.data);
        log_debug(LOG_VVERB, "  timeout: %d", cp->timeout);
        log_debug(LOG_VVERB, "  backlog: %d", cp->backlog);
        log_debug(LOG_VVERB, "  hash: %d", cp->hash);
        log_debug(LOG_VVERB, "  hash_tag: \"%.*s\"", cp->hash_tag.len,
                  cp->hash_tag.data);
        log_debug(LOG_VVERB, "  distribution: %d", cp->distribution);
        log_debug(LOG_VVERB, "  client_connections: %d",
                  cp->client_connections);
        log_debug(LOG_VVERB, "  redis: %d", cp->redis);
        log_debug(LOG_VVERB, "  preconnect: %d", cp->preconnect);
        log_debug(LOG_VVERB, "  auto_eject_hosts: %d", cp->auto_eject_hosts);
        log_debug(LOG_VVERB, "  server_connections: %d",
                  cp->server_connections);
        log_debug(LOG_VVERB, "  server_retry_timeout: %d",
                  cp->server_retry_timeout);
        log_debug(LOG_VVERB, "  server_failure_limit: %d",
                  cp->server_failure_limit);

        nserver = array_n(&cp->server);
        log_debug(LOG_VVERB, "  servers: %"PRIu32"", nserver);

        for (j = 0; j < nserver; j++) {
            s = array_get(&cp->server, j);
            log_debug(LOG_VVERB, "    %.*s", s->len, s->data);
        }
    }
}

static rstatus_t
conf_yaml_init(struct conf *cf)
{
    int rv;

    ASSERT(!cf->valid_parser);

    rv = fseek(cf->fh, 0L, SEEK_SET);
    if (rv < 0) {
        log_error("conf: failed to seek to the beginning of file '%s': %s",
                  cf->fname, strerror(errno));
        return NC_ERROR;
    }

    rv = yaml_parser_initialize(&cf->parser);
    if (!rv) {
        log_error("conf: failed (err %d) to initialize yaml parser",
                  cf->parser.error);
        return NC_ERROR;
    }

    yaml_parser_set_input_file(&cf->parser, cf->fh);
    cf->valid_parser = 1;

    return NC_OK;
}

static void
conf_yaml_deinit(struct conf *cf)
{
    if (cf->valid_parser) {
        yaml_parser_delete(&cf->parser);
        cf->valid_parser = 0;
    }
}

static rstatus_t
conf_token_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_token);

    rv = yaml_parser_scan(&cf->parser, &cf->token);
    if (!rv) {
        log_error("conf: failed (err %d) to scan next token", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_token = 1;

    return NC_OK;
}

static void
conf_token_done(struct conf *cf)
{
    ASSERT(cf->valid_parser);

    if (cf->valid_token) {
        yaml_token_delete(&cf->token);
        cf->valid_token = 0;
    }
}

static rstatus_t
conf_event_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_event);

    rv = yaml_parser_parse(&cf->parser, &cf->event);
    if (!rv) {
        log_error("conf: failed (err %d) to get next event", cf->parser.error);
        return NC_ERROR;
    }
    cf->valid_event = 1;

    return NC_OK;
}

static void
conf_event_done(struct conf *cf)
{
    if (cf->valid_event) {
        yaml_event_delete(&cf->event);
        cf->valid_event = 0;
    }
}

static rstatus_t
conf_push_scalar(struct conf *cf)
{
    rstatus_t status;
    struct string *value;
    uint8_t *scalar;
    uint32_t scalar_len;

    scalar = cf->event.data.scalar.value;
    scalar_len = (uint32_t)cf->event.data.scalar.length;
    if (scalar_len == 0) {
        return NC_ERROR;
    }

    log_debug(LOG_VVERB, "push '%.*s'", scalar_len, scalar);

    value = array_push(&cf->arg);
    if (value == NULL) {
        return NC_ENOMEM;
    }
    string_init(value);

    status = string_copy(value, scalar, scalar_len);
    if (status != NC_OK) {
        array_pop(&cf->arg);
        return status;
    }

    return NC_OK;
}

static void
conf_pop_scalar(struct conf *cf)
{
    struct string *value;

    value = array_pop(&cf->arg);
    log_debug(LOG_VVERB, "pop '%.*s'", value->len, value->data);
    string_deinit(value);
}

static rstatus_t
conf_handler(struct conf *cf, void *data)
{
    struct command *cmd;
    struct string *key, *value;
    uint32_t narg;

    if (array_n(&cf->arg) == 1) {
        value = array_top(&cf->arg);
        log_debug(LOG_VVERB, "conf handler on '%.*s'", value->len, value->data);
        return conf_pool_init(data, value);
    }

    narg = array_n(&cf->arg);
    value = array_get(&cf->arg, narg - 1);
    key = array_get(&cf->arg, narg - 2);

    log_debug(LOG_VVERB, "conf handler on %.*s: %.*s", key->len, key->data,
              value->len, value->data);

    for (cmd = conf_commands; cmd->name.len != 0; cmd++) {
        char *rv;

        if (string_compare(key, &cmd->name) != 0) {
            continue;
        }

        rv = cmd->set(cf, cmd, data);
        if (rv != CONF_OK) {
            log_error("conf: directive \"%.*s\" %s", key->len, key->data, rv);
            return NC_ERROR;
        }

        return NC_OK;
    }

    log_error("conf: directive \"%.*s\" is unknown", key->len, key->data);

    return NC_ERROR;
}

static rstatus_t
conf_begin_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next begin event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_MAPPING_START_EVENT:
            ASSERT(cf->depth < CONF_MAX_DEPTH);
            cf->depth++;
            done = true;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);

    } while (!done);

    return NC_OK;
}

static rstatus_t
conf_end_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next end event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done);

    conf_yaml_deinit(cf);

    return NC_OK;
}

static rstatus_t
conf_parse_core(struct conf *cf, void *data)
{
    rstatus_t status;
    bool done, leaf, new_pool;

    ASSERT(cf->sound);

    status = conf_event_next(cf);
    if (status != NC_OK) {
        return status;
    }

    log_debug(LOG_VVERB, "next event %d depth %"PRIu32" seq %d", cf->event.type,
              cf->depth, cf->seq);

    done = false;
    leaf = false;
    new_pool = false;

    switch (cf->event.type) {
    case YAML_MAPPING_END_EVENT:
        cf->depth--;
        if (cf->depth == 1) {
            conf_pop_scalar(cf);
        } else if (cf->depth == 0) {
            done = true;
        }
        break;

    case YAML_MAPPING_START_EVENT:
        cf->depth++;
        break;

    case YAML_SEQUENCE_START_EVENT:
        cf->seq = 1;
        break;

    case YAML_SEQUENCE_END_EVENT:
        conf_pop_scalar(cf);
        cf->seq = 0;
        break;

    case YAML_SCALAR_EVENT:
        status = conf_push_scalar(cf);
        if (status != NC_OK) {
            break;
        }

        /* take appropriate action */
        if (cf->seq) {
            /* for a sequence, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        } else if (cf->depth == CONF_ROOT_DEPTH) {
            /* create new conf_pool */
            data = array_push(&cf->pool);
            if (data == NULL) {
                status = NC_ENOMEM;
                break;
           }
           new_pool = true;
        } else if (array_n(&cf->arg) == cf->depth + 1) {
            /* for {key: value}, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        }
        break;

    default:
        NOT_REACHED();
        break;
    }

    conf_event_done(cf);

    if (status != NC_OK) {
        return status;
    }

    if (done) {
        /* terminating condition */
        return NC_OK;
    }

    if (leaf || new_pool) {
        status = conf_handler(cf, data);

        if (leaf) {
            conf_pop_scalar(cf);
            if (!cf->seq) {
                conf_pop_scalar(cf);
            }
        }

        if (status != NC_OK) {
            return status;
        }
    }

    return conf_parse_core(cf, data);
}

static rstatus_t
conf_parse(struct conf *cf)
{
    rstatus_t status;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(array_n(&cf->arg) == 0);

    status = conf_begin_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_parse_core(cf, NULL);
    if (status != NC_OK) {
        return status;
    }

    status = conf_end_parse(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->parsed = 1;

    return NC_OK;
}

static struct conf *
conf_open(char *filename)
{
    rstatus_t status;
    struct conf *cf;
    FILE *fh;

    fh = fopen(filename, "r");
    if (fh == NULL) {
        log_error("conf: failed to open configuration '%s': %s", filename,
                  strerror(errno));
        return NULL;
    }

    cf = nc_alloc(sizeof(*cf));
    if (cf == NULL) {
        fclose(fh);
        return NULL;
    }
    memset(cf, 0, sizeof(*cf));

    status = array_init(&cf->arg, CONF_DEFAULT_ARGS, sizeof(struct string));
    if (status != NC_OK) {
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    status = array_init(&cf->pool, CONF_DEFAULT_POOL, sizeof(struct conf_pool));
    if (status != NC_OK) {
        array_deinit(&cf->arg);
        nc_free(cf);
        fclose(fh);
        return NULL;
    }

    cf->fname = filename;
    cf->fh = fh;
    cf->depth = 0;
    /* parser, event, and token are initialized later */
    cf->seq = 0;
    cf->valid_parser = 0;
    cf->valid_event = 0;
    cf->valid_token = 0;
    cf->sound = 0;
    cf->parsed = 0;
    cf->valid = 0;

    log_debug(LOG_VVERB, "opened conf '%s'", filename);

    return cf;
}

static rstatus_t
conf_validate_document(struct conf *cf)
{
    rstatus_t status;
    uint32_t count;
    bool done;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    count = 0;
    done = false;
    do {
        yaml_document_t document;
        yaml_node_t *node;
        int rv;

        rv = yaml_parser_load(&cf->parser, &document);
        if (!rv) {
            log_error("conf: failed (err %d) to get the next yaml document",
                      cf->parser.error);
            conf_yaml_deinit(cf);
            return NC_ERROR;
        }

        node = yaml_document_get_root_node(&document);
        if (node == NULL) {
            done = true;
        } else {
            count++;
        }

        yaml_document_delete(&document);
    } while (!done);

    conf_yaml_deinit(cf);

    if (count != 1) {
        log_error("conf: '%s' must contain only 1 document; found %"PRIu32" "
                  "documents", cf->fname, count);
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_tokens(struct conf *cf)
{
    rstatus_t status;
    bool done, error;
    int type;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    do {
        status = conf_token_next(cf);
        if (status != NC_OK) {
            return status;
        }
        type = cf->token.type;

        switch (type) {
        case YAML_NO_TOKEN:
            error = true;
            log_error("conf: no token (%d) is disallowed", type);
            break;

        case YAML_VERSION_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: version directive token (%d) is disallowed", type);
            break;

        case YAML_TAG_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: tag directive token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_START_TOKEN:
            error = true;
            log_error("conf: document start token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_END_TOKEN:
            error = true;
            log_error("conf: document end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_START_TOKEN:
            error = true;
            log_error("conf: flow sequence start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_END_TOKEN:
            error = true;
            log_error("conf: flow sequence end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_START_TOKEN:
            error = true;
            log_error("conf: flow mapping start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_END_TOKEN:
            error = true;
            log_error("conf: flow mapping end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_ENTRY_TOKEN:
            error = true;
            log_error("conf: flow entry token (%d) is disallowed", type);
            break;

        case YAML_ALIAS_TOKEN:
            error = true;
            log_error("conf: alias token (%d) is disallowed", type);
            break;

        case YAML_ANCHOR_TOKEN:
            error = true;
            log_error("conf: anchor token (%d) is disallowed", type);
            break;

        case YAML_TAG_TOKEN:
            error = true;
            log_error("conf: tag token (%d) is disallowed", type);
            break;

        case YAML_BLOCK_SEQUENCE_START_TOKEN:
        case YAML_BLOCK_MAPPING_START_TOKEN:
        case YAML_BLOCK_END_TOKEN:
        case YAML_BLOCK_ENTRY_TOKEN:
            break;

        case YAML_KEY_TOKEN:
        case YAML_VALUE_TOKEN:
        case YAML_SCALAR_TOKEN:
            break;

        case YAML_STREAM_START_TOKEN:
            break;

        case YAML_STREAM_END_TOKEN:
            done = true;
            log_debug(LOG_VVERB, "conf '%s' has valid tokens", cf->fname);
            break;

        default:
            error = true;
            log_error("conf: unknown token (%d) is disallowed", type);
            break;
        }

        conf_token_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_validate_structure(struct conf *cf)
{
    rstatus_t status;
    int type, depth;
    uint32_t i, count[CONF_MAX_DEPTH + 1];
    bool done, error, seq;

    status = conf_yaml_init(cf);
    if (status != NC_OK) {
        return status;
    }

    done = false;
    error = false;
    seq = false;
    depth = 0;
    for (i = 0; i < CONF_MAX_DEPTH + 1; i++) {
        count[i] = 0;
    }

    /*
     * Validate that the configuration conforms roughly to the following
     * yaml tree structure:
     *
     * keyx:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     *
     * keyy:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     */
    do {
        status = conf_event_next(cf);
        if (status != NC_OK) {
            return status;
        }

        type = cf->event.type;

        log_debug(LOG_VVERB, "next event %d depth %d seq %d", type, depth, seq);

        switch (type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_MAPPING_START_EVENT:
            if (depth == CONF_ROOT_DEPTH && count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has more than one \"key:value\" at depth"
                          " %d", cf->fname, depth);
            } else if (depth >= CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has a depth greater than %d", cf->fname,
                          CONF_MAX_DEPTH);
            }
            depth++;
            break;

        case YAML_MAPPING_END_EVENT:
            if (depth == CONF_MAX_DEPTH) {
                if (seq) {
                    seq = false;
                } else {
                    error = true;
                    log_error("conf: '%s' missing sequence directive at depth "
                              "%d", cf->fname, depth);
                }
            }
            depth--;
            count[depth] = 0;
            break;

        case YAML_SEQUENCE_START_EVENT:
            if (seq) {
                error = true;
                log_error("conf: '%s' has more than one sequence directive",
                          cf->fname);
            } else if (depth != CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has sequence at depth %d instead of %d",
                          cf->fname, depth, CONF_MAX_DEPTH);
            } else if (count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has invalid \"key:value\" at depth %d",
                          cf->fname, depth);
            }
            seq = true;
            break;

        case YAML_SEQUENCE_END_EVENT:
            ASSERT(depth == CONF_MAX_DEPTH);
            count[depth] = 0;
            break;

        case YAML_SCALAR_EVENT:
            if (depth == 0) {
                error = true;
                log_error("conf: '%s' has invalid empty \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_ROOT_DEPTH && count[depth] != 0) {
                error = true;
                log_error("conf: '%s' has invalid mapping \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_MAX_DEPTH && count[depth] == 2) {
                /* found a "key: value", resetting! */
                count[depth] = 0;
            }
            count[depth]++;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? NC_OK : NC_ERROR;
}

static rstatus_t
conf_pre_validate(struct conf *cf)
{
    rstatus_t status;

    status = conf_validate_document(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_tokens(cf);
    if (status != NC_OK) {
        return status;
    }

    status = conf_validate_structure(cf);
    if (status != NC_OK) {
        return status;
    }

    cf->sound = 1;

    return NC_OK;
}

static int
conf_server_name_cmp(const void *t1, const void *t2)
{
    const struct conf_server *s1 = t1, *s2 = t2;

    return string_compare(&s1->name, &s2->name);
}

static int
conf_pool_name_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->name, &p2->name);
}

static int
conf_pool_listen_cmp(const void *t1, const void *t2)
{
    const struct conf_pool *p1 = t1, *p2 = t2;

    return string_compare(&p1->listen.pname, &p2->listen.pname);
}

static rstatus_t
conf_validate_server(struct conf *cf, struct conf_pool *cp)
{
    uint32_t i, nserver;
    bool valid;

    nserver = array_n(&cp->server);
    if (nserver == 0) {
        log_error("conf: pool '%.*s' has no servers", cp->name.len,
                  cp->name.data);
        return NC_ERROR;
    }

    /*
     * Disallow duplicate servers - servers with identical "host:port:weight"
     * or "name" combination are considered as duplicates. When server name
     * is configured, we only check for duplicate "name" and not for duplicate
     * "host:port:weight"
     */
    array_sort(&cp->server, conf_server_name_cmp);
    for (valid = true, i = 0; i < nserver - 1; i++) {
        struct conf_server *cs1, *cs2;

        cs1 = array_get(&cp->server, i);
        cs2 = array_get(&cp->server, i + 1);

        if (string_compare(&cs1->name, &cs2->name) == 0) {
            log_error("conf: pool '%.*s' has servers with same name '%.*s'",
                      cp->name.len, cp->name.data, cs1->name.len,
                      cs1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

static rstatus_t
conf_validate_pool(struct conf *cf, struct conf_pool *cp)
{
    rstatus_t status;

    ASSERT(!cp->valid);
    ASSERT(!string_empty(&cp->name));

    if (!cp->listen.valid) {
        log_error("conf: directive \"listen:\" is missing");
        return NC_ERROR;
    }

    /* set default values for unset directives */

    if (cp->distribution == CONF_UNSET_DIST) {
        cp->distribution = CONF_DEFAULT_DIST;
    }

    if (cp->hash == CONF_UNSET_HASH) {
        cp->hash = CONF_DEFAULT_HASH;
    }

    if (cp->timeout == CONF_UNSET_NUM) {
        cp->timeout = CONF_DEFAULT_TIMEOUT;
    }

    if (cp->backlog == CONF_UNSET_NUM) {
        cp->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
    }

    cp->client_connections = CONF_DEFAULT_CLIENT_CONNECTIONS;

    if (cp->redis == CONF_UNSET_NUM) {
        cp->redis = CONF_DEFAULT_REDIS;
    }

    if (cp->tcpkeepalive == CONF_UNSET_NUM) {
        cp->tcpkeepalive = CONF_DEFAULT_TCPKEEPALIVE;
    }

    if (cp->redis_db == CONF_UNSET_NUM) {
        cp->redis_db = CONF_DEFAULT_REDIS_DB;
    }

    if (cp->preconnect == CONF_UNSET_NUM) {
        cp->preconnect = CONF_DEFAULT_PRECONNECT;
    }

    if (cp->auto_eject_hosts == CONF_UNSET_NUM) {
        cp->auto_eject_hosts = CONF_DEFAULT_AUTO_EJECT_HOSTS;
    }

    if (cp->server_connections == CONF_UNSET_NUM) {
        cp->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
    } else if (cp->server_connections == 0) {
        log_error("conf: directive \"server_connections:\" cannot be 0");
        return NC_ERROR;
    }

    if (cp->server_retry_timeout == CONF_UNSET_NUM) {
        cp->server_retry_timeout = CONF_DEFAULT_SERVER_RETRY_TIMEOUT;
    }

    if (cp->server_failure_limit == CONF_UNSET_NUM) {
        cp->server_failure_limit = CONF_DEFAULT_SERVER_FAILURE_LIMIT;
    }

    if (!cp->redis && cp->redis_auth.len > 0) {
        log_error("conf: directive \"redis_auth:\" is only valid for a redis pool");
        return NC_ERROR;
    }

    status = conf_validate_server(cf, cp);
    if (status != NC_OK) {
        return status;
    }

    cp->valid = 1;

    return NC_OK;
}

static rstatus_t
conf_post_validate(struct conf *cf)
{
    rstatus_t status;
    uint32_t i, npool;
    bool valid;

    ASSERT(cf->sound && cf->parsed);
    ASSERT(!cf->valid);

    npool = array_n(&cf->pool);
    if (npool == 0) {
        log_error("conf: '%.*s' has no pools", cf->fname);
        return NC_ERROR;
    }

    /* validate pool */
    for (i = 0; i < npool; i++) {
        struct conf_pool *cp = array_get(&cf->pool, i);

        status = conf_validate_pool(cf, cp);
        if (status != NC_OK) {
            return status;
        }
    }

    /* disallow pools with duplicate listen: key values */
    array_sort(&cf->pool, conf_pool_listen_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->listen.pname, &p2->listen.pname) == 0) {
            log_error("conf: pools '%.*s' and '%.*s' have the same listen "
                      "address '%.*s'", p1->name.len, p1->name.data,
                      p2->name.len, p2->name.data, p1->listen.pname.len,
                      p1->listen.pname.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    /* disallow pools with duplicate names */
    array_sort(&cf->pool, conf_pool_name_cmp);
    for (valid = true, i = 0; i < npool - 1; i++) {
        struct conf_pool *p1, *p2;

        p1 = array_get(&cf->pool, i);
        p2 = array_get(&cf->pool, i + 1);

        if (string_compare(&p1->name, &p2->name) == 0) {
            log_error("conf: '%s' has pools with same name %.*s'", cf->fname,
                      p1->name.len, p1->name.data);
            valid = false;
            break;
        }
    }
    if (!valid) {
        return NC_ERROR;
    }

    return NC_OK;
}

struct conf *
conf_create(char *filename)
{
    rstatus_t status;
    struct conf *cf;

    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }

    /* validate configuration file before parsing */
    status = conf_pre_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* parse the configuration file */
    status = conf_parse(cf);
    if (status != NC_OK) {
        goto error;
    }

    /* validate parsed configuration */
    status = conf_post_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    conf_dump(cf);

    fclose(cf->fh);
    cf->fh = NULL;

    return cf;

error:
    log_stderr("nutcracker: configuration file '%s' syntax is invalid",
               filename);
    fclose(cf->fh);
    cf->fh = NULL;
    conf_destroy(cf);
    return NULL;
}

void
conf_destroy(struct conf *cf)
{
    if (!cf) return;

    while (array_n(&cf->arg) != 0) {
        conf_pop_scalar(cf);
    }
    array_deinit(&cf->arg);

    int i = 0;
    while (array_n(&cf->pool) != 0) {
        log_debug(LOG_NOTICE, "deinit conf pool %d", i++);
        conf_pool_deinit(array_pop(&cf->pool));
    }
    array_deinit(&cf->pool);

    nc_free(cf);
}

char *
conf_set_string(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

char *
conf_set_listen(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct string *value;
    struct conf_listen *field;
    uint8_t *p, *name;
    uint32_t namelen;

    p = conf;
    field = (struct conf_listen *)(p + cmd->offset);

    if (field->valid == 1) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(&field->pname, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    if (value->data[0] == '/') {
        uint8_t *q, *start, *perm;
        uint32_t permlen;


        /* parse "socket_path permissions" from the end */
        p = value->data + value->len -1;
        start = value->data;
        q = nc_strrchr(p, start, ' ');
        if (q == NULL) {
            /* no permissions field, so use defaults */
            name = value->data;
            namelen = value->len;
        } else {
            perm = q + 1;
            permlen = (uint32_t)(p - perm + 1);

            p = q - 1;
            name = start;
            namelen = (uint32_t)(p - start + 1);

            errno = 0;
            field->perm = (mode_t)strtol((char *)perm, NULL, 8);
            if (errno || field->perm > 0777) {
                return "has an invalid file permission in \"socket_path permission\" format string";
            }
        }
    } else {
        uint8_t *q, *start, *port;
        uint32_t portlen;

        /* parse "hostname:port" from the end */
        p = value->data + value->len - 1;
        start = value->data;
        q = nc_strrchr(p, start, ':');
        if (q == NULL) {
            return "has an invalid \"hostname:port\" format string";
        }

        port = q + 1;
        portlen = (uint32_t)(p - port + 1);

        p = q - 1;

        name = start;
        namelen = (uint32_t)(p - start + 1);

        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port\" format string";
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = nc_resolve(&field->name, field->port, &field->info);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    field->valid = 1;

    return CONF_OK;
}

char *
conf_add_server(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct array *a;
    struct string *value;
    struct conf_server *field;
    uint8_t *p, *q, *start;
    uint8_t *pname, *addr, *port, *weight, *name;
    uint32_t k, delimlen, pnamelen, addrlen, portlen, weightlen, namelen;
    char delim[] = " ::";

    p = conf;
    a = (struct array *)(p + cmd->offset);

    field = array_push(a);
    if (field == NULL) {
        return CONF_ERROR;
    }

    conf_server_init(field);

    value = array_top(&cf->arg);

    /* parse "hostname:port:weight [name]" or "/path/unix_socket:weight [name]" from the end */
    p = value->data + value->len - 1;
    start = value->data;
    addr = NULL;
    addrlen = 0;
    weight = NULL;
    weightlen = 0;
    port = NULL;
    portlen = 0;
    name = NULL;
    namelen = 0;

    delimlen = value->data[0] == '/' ? 2 : 3;

    for (k = 0; k < sizeof(delim); k++) {
        q = nc_strrchr(p, start, delim[k]);
        if (q == NULL) {
            if (k == 0) {
                /*
                 * name in "hostname:port:weight [name]" format string is
                 * optional
                 */
                continue;
            }
            break;
        }

        switch (k) {
        case 0:
            name = q + 1;
            namelen = (uint32_t)(p - name + 1);
            break;

        case 1:
            weight = q + 1;
            weightlen = (uint32_t)(p - weight + 1);
            break;

        case 2:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return "has an invalid \"hostname:port:weight [name]\"or \"/path/unix_socket:weight [name]\" format string";
    }

    pname = value->data;
    pnamelen = namelen > 0 ? value->len - (namelen + 1) : value->len;
    status = string_copy(&field->pname, pname, pnamelen);
    if (status != NC_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);

    field->weight = nc_atoi(weight, weightlen);
    if (field->weight < 0) {
        return "has an invalid weight in \"hostname:port:weight [name]\" format string";
    } else if (field->weight == 0) {
        return "has a zero weight in \"hostname:port:weight [name]\" format string";
    }

    if (value->data[0] != '/') {
        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    status = string_copy(&field->addrstr, addr, addrlen);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    /*
     * The address resolution of the backend server hostname is lazy.
     * The resolution occurs when a new connection to the server is
     * created, which could either be the first time or every time
     * the server gets re-added to the pool after an auto ejection
     */

    field->valid = 1;

    return CONF_OK;
}

char *
conf_set_num(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int num, *np;
    struct string *value;

    p = conf;
    np = (int *)(p + cmd->offset);

    if (*np != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    num = nc_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;

    return CONF_OK;
}

char *
conf_set_bool(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int *bp;
    struct string *value, true_str, false_str;

    p = conf;
    bp = (int *)(p + cmd->offset);

    if (*bp != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
    string_set_text(&true_str, "true");
    string_set_text(&false_str, "false");

    if (string_compare(value, &true_str) == 0) {
        *bp = 1;
    } else if (string_compare(value, &false_str) == 0) {
        *bp = 0;
    } else {
        return "is not \"true\" or \"false\"";
    }

    return CONF_OK;
}

char *
conf_set_hash(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    hash_type_t *hp;
    struct string *value, *hash;

    p = conf;
    hp = (hash_type_t *)(p + cmd->offset);

    if (*hp != CONF_UNSET_HASH) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (hash = hash_strings; hash->len != 0; hash++) {
        if (string_compare(value, hash) != 0) {
            continue;
        }

        *hp = hash - hash_strings;

        return CONF_OK;
    }

    return "is not a valid hash";
}

char *
conf_set_distribution(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    dist_type_t *dp;
    struct string *value, *dist;

    p = conf;
    dp = (dist_type_t *)(p + cmd->offset);

    if (*dp != CONF_UNSET_DIST) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (dist = dist_strings; dist->len != 0; dist++) {
        if (string_compare(value, dist) != 0) {
            continue;
        }

        *dp = dist - dist_strings;

        return CONF_OK;
    }

    return "is not a valid distribution";
}

char *
conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    if (value->len != 2) {
        return "is not a valid hash tag string with two characters";
    }

    status = string_duplicate(field, value);
    if (status != NC_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

// Validate json config file format.
static rstatus_t
conf_json_pre_validate(struct conf *cf)
{
    //struct conf_server srv;
    //string_to_conf_server("192.168.1.64:23434", &srv);
    JSON_Value *root_value = json_parse_file_with_comments(cf->fname);
    if (!root_value) {
        log_stderr("configuration file '%s' invalid JSON syntax\n",
                   cf->fname);
        return NC_ERROR;
    }
    json_value_free(root_value);

    return NC_OK;
}

// Validate conf contents after conf-file is loaded.
static rstatus_t
conf_json_post_validate(struct conf *cf)
{
  uint32_t npools = array_n(&cf->pool);

  for (uint32_t i = 0; i < npools; i++) {
    struct conf_pool* pool = array_get(&cf->pool, i);

    // Init some misc fields.
    pool->hash = HASH_MURMUR;  // HASH_FNV1_64;
    pool->redis = 1;
    pool->redis_db = 0;
    pool->tcpkeepalive = 1;
    pool->preconnect  =1;

    pool->auto_eject_hosts = 0;
    // use a larger server timeout.
    pool->server_retry_timeout = 200000;
    pool->server_failure_limit = 2;

    if (pool->client_connections < 1) {
      pool->client_connections = 1;
    }
    if (pool->backlog < 1) {
        pool->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
    }
    if (pool->server_connections < 1) {
      pool->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
    }
    if (pool->timeout < 1) {
      pool->timeout = 100;
    }
  }
  return NC_OK;
}

// Input Json "obj" represents a pool.
//
// Mandatory pool fields:
//      "name": pool name
//      "shard_range": key hash value range in this shard, [low, high)
//      "shards" : a list of shards.
static rstatus_t
conf_json_init_pool(JSON_Object* obj,
                    struct conf_pool* pool,
                    char *proxy_addr)
{
    rstatus_t status;
    struct string plname;

    // TODO: verify "proxies", "shard_range", "shards", "master", "slaves" exists.
    //
    // init pool name.
    string_init(&plname);
    const char* name = json_object_get_string(obj, "name");
    status = string_copy(&plname, (uint8_t*)name, (uint32_t)strlen(name));
    if (status != NC_OK) {
      return status;
    }

    // Init the pool to default value.
    conf_pool_init(pool, &plname);
    string_deinit(&plname);

    // Init the list of proxies.
    status = array_init(&pool->proxies, CONF_DEFAULT_PROXIES,
                        sizeof(struct conf_server));

    JSON_Array*  jproxies = json_object_get_array(obj, "proxy");
    ASSERT(jproxies != NULL);
    size_t nproxies = json_array_get_count(jproxies);
    ASSERT(nproxies > 0);
    for (size_t i = 0; i < nproxies; i++) {
        const char* p = json_array_get_string(jproxies, i);
        if (proxy_addr) {
            if ((strlen(proxy_addr) != strlen(p)) ||
                (strncmp(proxy_addr, p, strlen(proxy_addr)) != 0)) {
                continue;
            }
        }
        struct conf_server* cp = (struct conf_server*) array_push(&pool->proxies);
        string_to_conf_server((const uint8_t*) p, cp);
        break;
    }

    // Skip a pool if it isn't covered by the specified "porxy_addr".
    if (array_n(&pool->proxies) == 0) {
        log_debug(LOG_NOTICE, "skip pool \"%s\"", name);

        //conf_server_deinit(array_pop(&pool->proxies));
        array_deinit(&pool->proxies);
        string_deinit(&pool->name);
        return NC_ERROR;
    } else {
        // init the "listen" address, which is proxy.
        conf_server_to_conf_listen(array_get(&pool->proxies, 0),
                                   &pool->listen);
    }

    // hash value range: [min, max]
    JSON_Array * hv_range = json_object_get_array(obj, "shard_range");
    ASSERT(hv_range != NULL);
    ASSERT(json_array_get_count(hv_range) == 2);
    pool->shard_range_min = (uint32_t)json_array_get_number(hv_range, 0);
    pool->shard_range_max = (uint32_t)json_array_get_number(hv_range, 1);
    ASSERT(pool->shard_range_min < pool->shard_range_max);

    // Init shards.
    status = array_init(&pool->shards, CONF_DEFAULT_SHARDS,
                        sizeof(struct conf_shard));
    JSON_Array* jshards = json_object_get_array(obj, "shards");
    size_t nshards = json_array_get_count(jshards);
    ASSERT(nshards > 0);

    for (size_t i = 0; i < nshards; i++) {
        // Add a shard into pool.
        struct conf_shard* cfshard = (struct conf_shard*) array_push(&pool->shards);
        if (cfshard == NULL) {
            status = NC_ENOMEM;
            break;
        }
        memset(cfshard, 0, sizeof(struct conf_shard));

        JSON_Object* js = json_array_get_object(jshards, i);
        cfshard->range_begin = (uint32_t)json_object_get_number(js, "range_start");
        cfshard->range_end = (uint32_t)json_object_get_number(js, "range_end");

        // Add master / slave conf_servers in each conf_shard.
        const char* jsmaster = json_object_get_string(js, "master");
        string_to_conf_server((const uint8_t*)jsmaster, &cfshard->master);

        JSON_Array* jsslaves = json_object_get_array(js, "slave");
        size_t nslaves = json_array_get_count(jsslaves);
        ASSERT(nslaves > 0);
        status = array_init(&cfshard->slaves, CONF_DEFAULT_SLAVES,
                            sizeof(struct conf_server));

        for (size_t s = 0; s < nslaves; s++) {
          const char* slvstr = json_array_get_string(jsslaves, s);
          struct conf_server* slv = array_push(&cfshard->slaves);
          string_to_conf_server((const uint8_t*)slvstr, slv);
          //struct string* slv = (struct string*) array_push(&cfshard->slaves);
          //string_init(slv);
          //string_copy(slv, (uint8_t*)slvstr, (uint32_t)strlen(slvstr));
        }
    }

    return NC_OK;
}

static rstatus_t
conf_json_parse(struct conf *cf, struct instance *nci)
{
    rstatus_t status = NC_OK;

    JSON_Value *root_value = json_parse_file_with_comments(cf->fname);
    ASSERT(root_value != NULL);

    // Top level is a JSON object.
    // Mandatory fields:
    //      "pools": a list of pools.
    JSON_Object* root_obj = json_value_get_object(root_value);
    JSON_Array* pools = json_object_get_array(root_obj, "pools");
    size_t num_pools = json_array_get_count(pools);

    log_debug(LOG_INFO, "conf %s contains %d pools\n", cf->fname,  num_pools);

    char proxy_addr[200];
    sprintf(proxy_addr, "%s:%d", nci->proxy_ip, nci->proxy_port);

    for (size_t i = 0; i < num_pools; i++) {
        struct conf_pool* pool = (struct conf_pool*) array_push(&cf->pool);
        if (pool == NULL) {
            status = NC_ENOMEM;
            break;
        }

        JSON_Object* pool_obj = json_array_get_object(pools, i);

        status = conf_json_init_pool(pool_obj, pool, proxy_addr);
        if (status != NC_OK) {
            array_pop(&cf->pool);
            continue;
        }
        pool->valid = 1;
    }
    json_value_free(root_value);

    return status;
}



static void
conf_json_dump(struct conf *cf)
{
    uint32_t npool = array_n(&cf->pool);
    log_debug(LOG_NOTICE, "conf %s: have %d pools, valid = %d\n",
              cf->fname, npool, cf->valid);
    for (uint32_t i = 0; i < npool; i++) {
        log_debug(LOG_NOTICE, "pool %d ::", i);

        struct conf_pool* pool = (struct conf_pool*) array_get(&cf->pool, i);

        uint32_t nproxies = array_n(&pool->proxies);
        uint32_t nshards = array_n(&pool->shards);

        log_debug(LOG_NOTICE, "\tpool name : \"%s\"", pool->name.data);
        log_debug(LOG_NOTICE, "\t%d proxies, listen: %s",
                  nproxies, pool->listen.name.data);
        log_debug(LOG_NOTICE, "\t%d shards", nshards);
        log_debug(LOG_NOTICE, "\tshard range : [%d, %d]",
                  pool->shard_range_min, pool->shard_range_max);

        for (uint32_t j = 0; j < nproxies; j++) {
            struct conf_server* p = (struct conf_server*) array_get(&pool->proxies, j);
            log_debug(LOG_NOTICE, "\tproxy %d : %s", j, p->pname.data);
        }

        for (uint32_t j = 0; j < nshards; j++) {
            struct conf_shard* sh = (struct conf_shard*) array_get(&pool->shards, j);
            uint32_t nslaves = array_n(&sh->slaves);

            log_debug(LOG_NOTICE, "\tshard %d : range [%d, %d], 1 master, %d slaves",
                      j, sh->range_begin, sh->range_end, nslaves);
            // master is a conf_server
            log_debug(LOG_NOTICE, "\t\tmaster : %s", sh->master.pname.data);

            // "slaves" is conf_server[].
            for (uint32_t k = 0; k < nslaves; k++) {
                struct conf_server* s = (struct conf_server*) array_get(&sh->slaves, k);
                log_debug(LOG_NOTICE, "\t\tslave %d : %s", k, s->pname.data);
            }
        }
    }
    log_debug(LOG_NOTICE, "\n");

}

struct conf *
conf_json_create_from_zk(char *zkservers, struct instance* nci, struct context *ctx)
{
    size_t max_path = 200;
    char zkpath[max_path];
    // Connect to ZK, grab conf, save to a local file.
    ctx->zkh = ZKConnect(zkservers);
    if (!ctx->zkh) {
      log_error("cannot connect to zk %s", zkservers);
      return NULL;
    }

    // Read conf from zk, save to local file.
    // This conf contains all info for a particular pool.
    int buflen = 64000;
    char buf[buflen];
    int sync = 1;
    int watch = 0;
    snprintf(zkpath, max_path, "%s/proxy/%s", ZK_BASE, nci->pool_name);
    int conf_len = ZKGet(ctx->zkh, zkpath, buf, buflen, watch, sync);
    if (conf_len <= 0) {
      log_error("read conf from zk: %s, ret %d", zkpath, conf_len);
      ZKClose(ctx->zkh);
      ctx->zkh = NULL;
      return NULL;
    }

    char fname[500];
    sprintf(fname, "%s-proxy-%s-%d",
            CONF_DEFAULT_FILE_SAVE_PATH, nci->proxy_ip, nci->proxy_port);
    nc_write_file(fname, buf, conf_len);

    struct conf *cf;
    cf = conf_json_create(fname, nci);
    if (cf == NULL) {
      return NULL;
    }

    cf->zk_servers = zkservers;
    // cf's "fname" is not needed anywhere else.
    cf->fname = NULL;

    return cf;
}

struct conf *
conf_json_create(char *filename, struct instance* nci)
{
    rstatus_t status = NC_OK;
    struct conf *cf;

    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }
    // We don't need file-handle.
    fclose(cf->fh);
    cf->fh = NULL;
    array_null(&cf->arg);

    // Validate conf file format correctness.
    status = conf_json_pre_validate(cf);
    if (status != NC_OK) {
      goto error;

    }

    // Parse conf file.
    status = conf_json_parse(cf, nci);
    if (status != NC_OK) {
        goto error;
    }

    // validate parsed configuration
    status = conf_json_post_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    cf->valid = 1;
    conf_json_dump(cf);

    return cf;

error:
    log_stderr("nutcracker: configuration file '%s' JSON syntax invalid",
               filename);
    conf_destroy(cf);
    return NULL;
}


/*
static void
init_server_from_confserver(struct server* s, struct conf_server* cfsrv)
{
    s->pname = cfsrv->pname;
    s->addrstr = cfsrv->addrstr;
    s->port = (uint16_t)cfsrv->port;
    s->weight = 0;    // we don't need weight.

    nc_memcpy(&s->info, &cfsrv->info, sizeof(cfsrv->info));

    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
    s->failure_count = 0;
}
*/

// This method is obsolete.
/*
static rstatus_t
conf_shard_each_server_shard(struct conf_shard* cs, struct array* shards)
{
    uint32_t nslaves = array_n(&cs->slaves);
    ASSERT(nslaves > 0);

    struct shard* shard = array_push(shards);
    ASSERT(shard != NULL);

    shard->idx = array_idx(shards, shard);
    shard->range_begin = cs->range_begin;
    shard->range_end = cs->range_end;

    // get shard's master.
    struct conf_server* cfsrv = &cs->master;
    struct server* s = shard->master;

    s->idx = 0;
    s->owner = shard;   // server is owned by shard.
    init_server_from_confserver(s, cfsrv);

    log_debug(LOG_NOTICE, "shard %d: init master server '%.*s'",
              shard->idx, s->pname.len, s->pname.data);

    // init slaves for this shard.
    array_init(&shard->slaves, nslaves, sizeof(struct server));
    for (uint32_t i = 0; i < array_n(&cs->slaves); i++) {
        cfsrv = (struct conf_server*) array_get(&cs->slaves, i);
        s = *((struct server**)array_push(&shard->slaves));
        s->idx = i;
        // a server is owned by enclosing shard.
        s->owner = shard;
        init_server_from_confserver(s, cfsrv);
        log_debug(LOG_NOTICE, "shard %d: init slave server %d '%.*s'",
                  shard->idx, s->idx, s->pname.len, s->pname.data);
    }
    return NC_OK;
}
*/

// Convert an address string "hostname:port" to a conf_server.
static rstatus_t
string_to_conf_server(const uint8_t* saddr, struct conf_server* srv)
{
    conf_server_init(srv);
    uint32_t len = (uint32_t)strlen((const char*)saddr);
    const uint8_t *s_name, *s_port;

    uint8_t *delim = nc_strchr((uint8_t*)saddr,
                               (uint8_t*) (saddr + len),
                               ':');
    ASSERT(delim != NULL);
    s_name = saddr;
    s_port = delim + 1;

    string_copy(&srv->pname, saddr, len);  // hostname:port
    string_copy(&srv->name, saddr, len);   // hostname:port
    string_copy(&srv->addrstr, saddr, (uint32_t)(delim - s_name));  // hostname only.

    srv->port = nc_atoi(s_port, (uint32_t)(len - (delim + 1 - s_name)));

    rstatus_t status = nc_resolve(&srv->addrstr, srv->port, &srv->info);
    if (status != NC_OK) {
        return NC_ERROR;
    }

    srv->valid = 1;

    return NC_OK;
}

static void
conf_server_to_conf_listen(struct conf_server* cs, struct conf_listen* cl)
{
    string_init(&cl->name);
    string_init(&cl->pname);
    string_duplicate(&cl->name, &cs->name);
    string_duplicate(&cl->pname, &cs->name);

    cl->port = cs->port;
    nc_memcpy(&cl->info, &cs->info, sizeof(cl->info));

    cl->valid = 1;
}

// Init server shard from info at conf_shards.
//
// server_pool* is the owner pool for this server shard.
static rstatus_t
conf_shards_to_server_shards(struct array* cf_shards,
                             struct array* srv_shards,
                             struct server_pool* sp)
{
    struct conf_shard* conf_sd = NULL;
    struct shard* srv_sd = NULL;
    rstatus_t rv = NC_OK;

    array_init(&sp->server, 16, sizeof(struct server));

    for (uint32_t i = 0; i < array_n(cf_shards); i++) {
        conf_sd = array_get(cf_shards, i);
        srv_sd = array_push(srv_shards);
        memset(srv_sd, 0, sizeof(struct shard));
        srv_sd->idx = array_idx(srv_shards, srv_sd);

        rv = conf_shard_to_server_shard(conf_sd, srv_sd, sp);
    }

    return rv;
}


// In renewed ZK scheme, each pool has its topology stored in znode
// "/distkv/proxy/<pool name>".
//
// This method reads a pool's conf from znode, and init a "conf" object.
struct conf*
get_pool_conf_from_zk(char* zkservers, struct context *ctx)
{
  struct instance *inst = ctx->owner_inst;
  char *pool_name = inst->pool_name;

  if (ctx->zkh == NULL) {
    ctx->zkh = ZKConnect(zkservers);
    if (!ctx->zkh) {
      log_error("cannot connect to zk %s", zkservers);
      return NULL;
    }
  }

  size_t max_path = 500;
  char path[max_path];
  snprintf(path, max_path, "%s/%s", inst->zk_config_root, pool_name);

  // Connect to ZK, grab a pool's conf, save to a local file.
  // This conf contains all info for a particular pool.
  int buflen = 10000;
  char buf[buflen];
  int sync = 1;
  int watch = 0;
  int conf_len = ZKGet(ctx->zkh, path, buf, buflen, watch, sync);
  if (conf_len <= 0 || conf_len >= buflen) {
    log_error("pool \"%s \"conf incorrect: %s, ret %d",
              pool_name, path, conf_len);
    ZKClose(ctx->zkh);
    ctx->zkh = NULL;
    return NULL;
  }

  // Now, save pool's conf to a disk file.
  sprintf(path, "%s-pool-%s", CONF_DEFAULT_FILE_SAVE_PATH, pool_name);
  nc_write_file(path, buf, conf_len);

  // Read this pool's config file, init a conf object.
  struct conf *cf;
  cf = create_pool_conf_from_file(path, ctx);
  if (cf == NULL) {
    return NULL;
  }

  // After conf is created, correct some options.
  rstatus_t status = conf_json_post_validate(cf);
  if (status != NC_OK) {
    conf_destroy(cf);
    return NULL;
  }

  cf->zk_servers = zkservers;
  cf->valid = 1;
  conf_json_dump(cf);
  conf_dump(cf);
  // the "fname" is not used anywhere else. Set to NULL.
  cf->fname = NULL;

  return cf;
}


struct conf*
create_pool_conf_from_file(char *filepath, struct context *ctx)
{
  struct instance *inst = ctx->owner_inst;

  rstatus_t status = NC_OK;
  struct conf *cf = conf_open(filepath);
  if (cf == NULL) {
      return NULL;
  }
  if (cf->fh) {
    fclose(cf->fh);
    cf->fh = NULL;
  }
  array_null(&cf->arg);

  status = parse_pool_conf_file(cf, inst->pool_name);
  if (status != NC_OK) {
    goto error;
  }

  // Init proxy listening address.
  // Each context has only one proxy for a given pool.
  uint32_t num_pools = array_n(&cf->pool);
  for (uint32_t i = 0; i < num_pools; i++) {
    struct conf_pool* pool = (struct conf_pool*) array_get(&cf->pool, i);
    int str_len = 200;
    char proxy_str[str_len];
    snprintf(proxy_str, (size_t)str_len, "%s:%d",
             inst->proxy_ip, inst->proxy_port);
    struct conf_server* cp = (struct conf_server*)array_push(&pool->proxies);
    string_to_conf_server((const uint8_t*)proxy_str, cp);
    conf_server_to_conf_listen(cp, &pool->listen);
  }

  return cf;

error:
  log_stderr("configuration file '%s' JSON syntax invalid", filepath);
  conf_destroy(cf);
  return NULL;
}

// Parse a pool's conf file, and populate a conf object.
static rstatus_t
parse_pool_conf_file(struct conf *cf, char *pool_name)
{
    rstatus_t status = NC_OK;

    // Open conf file (a json file), and perform sanity check.
    JSON_Value *root_value = json_parse_file_with_comments(cf->fname);
    ASSERT(root_value != NULL);
    if (JSONObject != json_type(root_value)) {
      log_error("config file error format : %s", cf->fname);
      return NC_ERROR;
    }

    JSON_Object* pool_obj = json_value_get_object(root_value);
    if (!sanity_check_pool_conf_json(pool_obj)) {
      log_error("config file invalid content : %s", cf->fname);
      return NC_ERROR;
    }

    // Create a conf_pool object.
    struct conf_pool* pool = (struct conf_pool*) array_push(&cf->pool);
    if (pool == NULL) {
      return NC_ENOMEM;
    }
    memset(pool, 0, sizeof(struct conf_pool));

    // Populate conf_pool obj from the json obj.
    status = conf_json_to_conf_pool(pool_obj, pool, pool_name);
    if (status != NC_OK) {
      array_pop(&cf->pool);
    } else {
      pool->valid = 1;
    }

    json_value_free(root_value);
    return status;
}

// Convert the configuration in a json obj to a conf_pool obj.
static rstatus_t
conf_json_to_conf_pool(JSON_Object *pobj, struct conf_pool *pool, char *pool_name)
{
  rstatus_t status;
  struct string plname;

  string_init(&plname);
  status = string_copy(&plname,
                       (uint8_t*)pool_name,
                       (uint32_t)strlen(pool_name));

  // Init the pool to default value.
  conf_pool_init(pool, &plname);
  string_deinit(&plname);

  // Init the conf_pool options.
  pool->redis = 1;
  pool->redis_db = 0;
  pool->hash = HASH_MURMUR;

  const char* name = json_object_get_string(pobj, "name");
  const char* id = json_object_get_string(pobj, "id");

  // Get this pool's hash value range.
  pool->shard_range_min = (uint32_t)atoi(json_object_get_string(pobj, "pool_begin"));
  pool->shard_range_max = (uint32_t)atoi(json_object_get_string(pobj, "pool_end"));

  // Max number of connections a proxy can establish to a server.
  const char *srv_conns = json_object_get_string(pobj, "server_connections");
  if (srv_conns) {
    pool->server_connections = (uint32_t)atoi(srv_conns);
  }

  // Init the list of proxies.
  status = array_init(&pool->proxies, CONF_DEFAULT_PROXIES,
                      sizeof(struct conf_server));

  // Init shards.
  status = array_init(&pool->shards, CONF_DEFAULT_SHARDS,
                      sizeof(struct conf_shard));
  JSON_Array* jshards = json_object_get_array(pobj, "shards");
  size_t nshards = json_array_get_count(jshards);


  for (size_t i = 0; i < nshards; i++) {
    // Add a shard into pool.
    struct conf_shard* cfshard = (struct conf_shard*) array_push(&pool->shards);
    if (cfshard == NULL) {
      status = NC_ENOMEM;
      break;
    }
    memset(cfshard, 0, sizeof(struct conf_shard));

    status = array_init(&cfshard->slaves, CONF_DEFAULT_SLAVES,
                        sizeof(struct conf_server));
    if (status != NC_OK) {
      return NC_ENOMEM;
    }

    JSON_Object* js = json_array_get_object(jshards, i);
    cfshard->range_begin = (uint32_t)atoi(json_object_get_string(js, "shard_begin"));
    cfshard->range_end = (uint32_t)atoi(json_object_get_string(js, "shard_end"));

    const char *str_status = json_object_get_string(js, "status");
    if (str_status) {
      strncpy(cfshard->status, str_status, 31);
      cfshard->status[31] = 0;
    } else {
      strncpy(cfshard->status, "unknown", 8);
    }

    // Init the master target's conf
    const char* jsmaster = json_object_get_string(js, "master_target");
    if (jsmaster && strlen(jsmaster) > 2) {
      string_to_conf_server((const uint8_t*)jsmaster, &cfshard->master);
    } else {
      log_error("pool %s shard %d: invalid master: %s\n", name, i, jsmaster);
    }

    // Init slave targets.
    JSON_Array* jsslaves = json_object_get_array(js, "slave_target");
    size_t nslaves = json_array_get_count(jsslaves);
    if (nslaves == 0) continue;

    for (size_t s = 0; s < nslaves; s++) {
      const char* slvstr = json_array_get_string(jsslaves, s);
      struct conf_server* slv = array_push(&cfshard->slaves);
      if (slvstr && strlen(slvstr) > 2) {
        string_to_conf_server((const uint8_t*)slvstr, slv);
      } else {
        log_error("pool %s shard %d: invalid slave : %s\n", name, i, jsmaster);
      }
    }
  }

  return status;

}


// Check if a pool json obj is valid or not.
// a pool's mandatory fields:
// {
//   "name"  : name of this pool
//   "id"    : pool id
//   "pool_begin" : key's hash range beginning
//   "pool_end"   : key's hash range ending
//   "shards"     : a list of shards.
// }
//
// Every shard must contain:
//   "shard_begin", "shard_end":  hash value range covered by this shard
//   "name" : shard name
//   "id"   : shard id
//   "pool_name", "pool_id":  owner pool's name/id
//   "master_target"  :  address ("ip:port") of master target
//   "slave_target"   :  list of slaves. Each elem is an "ip:port" string.
static bool
sanity_check_pool_conf_json(JSON_Object *pobj)
{
  if (!pobj) return false;

  uint32_t pool_begin = 0, pool_end = 0;

  if (json_object_get_string(pobj, "name") == NULL) {
    return false;
  }
  if (json_object_get_string(pobj, "id") == NULL) {
    return false;
  }

  if (json_object_get_string(pobj, "pool_begin") == NULL) {
    return false;
  } else {
    pool_begin = atoi(json_object_get_string(pobj, "pool_begin"));
  }

  if (json_object_get_string(pobj, "pool_end") == NULL) {
    return false;
  } else {
    pool_end = atoi(json_object_get_string(pobj, "pool_end"));
  }

  // Sanity check shards in the pool.
  JSON_Array* jshards = json_object_get_array(pobj, "shards");
  size_t nshards = json_array_get_count(jshards);
  if (nshards == 0) {
    // Allow an empty pool without and shards.
    return true;
  }

  uint32_t shard_range_min = 0xFFFFFFFF, shard_range_max = 0;

  for (size_t i = 0; i < nshards; i++) {
    JSON_Object* js = json_array_get_object(jshards, i);
    if (!sanity_check_shard_conf_json(js)) {
      return false;
    }

    // TODO: make sure shards ranges don't overlap.
    uint32_t begin = atoi(json_object_get_string(js, "shard_begin"));
    uint32_t end = atoi(json_object_get_string(js, "shard_end"));
    if (begin == shard_range_min) {
      log_error("duplicated shard range begin: %d", begin);
      return false;
    }
    if (end == shard_range_max) {
      log_error("duplicated shard range end: %d", end);
      return false;
    }

    if (shard_range_min > begin) {
      shard_range_min = begin;
    }
    if (shard_range_max < end) {
      shard_range_max = end;
    }
  }

  if (shard_range_min != pool_begin) {
    log_error("pool range min %d != shard range min %d",
              pool_begin, shard_range_min);
    return false;
  }
  if (shard_range_max != pool_end) {
    log_error("pool range max %d != shard range max %d",
              pool_end, shard_range_max);
    return false;
  }

  return true;
}

// Check if a shard json obj is valid or not.
static bool
sanity_check_shard_conf_json(JSON_Object *sobj)
{
  if (!sobj) return false;

  if (json_object_get_string(sobj, "shard_begin") == NULL) {
    return false;
  }
  if (json_object_get_string(sobj, "shard_end") == NULL) {
    return false;
  }
  if (json_object_get_string(sobj, "master_target") == NULL) {
    return false;
  }
  if (json_object_get_array(sobj, "slave_target") == NULL) {
    return false;
  }

  return true;
}


static void
display_server_shard(struct shard *srv_sd)
{
  struct server *srv = srv_sd->master;

  log_debug(LOG_NOTICE, "shard %d [%d ~ %d], master %p, %d slaves",
            srv_sd->idx, srv_sd->range_begin, srv_sd->range_end,
            srv, array_n(&srv_sd->slaves));

  if (srv) {
    log_debug(LOG_NOTICE, "\tmaster srv (idx %d): %s", srv->idx, srv->pname.data);
    if (srv->owner_shard != srv_sd) {
      log_error("master server %s owner mismatch\n", srv->pname.data);
    }
  }

  for (uint32_t j = 0; j < array_n(&srv_sd->slaves); j++) {
    struct server** ppsrv = (struct server**)(array_get(&srv_sd->slaves, j));
    srv = *ppsrv;
    log_debug(LOG_NOTICE, "\tslave srv (idx %d): %s", srv->idx, srv->pname.data);
    if (srv->owner_shard != srv_sd) {
      log_error("master server %s owner mismatch\n", srv->pname.data);
    }
  }
}

// Convert a conf-shard to a server_shard.
//
// Note: This method changes the pool's topology, particularly a new shard is
// added. The caller should acquire the pool lock before calling this method.
static rstatus_t
conf_shard_to_server_shard(struct conf_shard *conf_sd,
                           struct shard *srv_sd,
                           struct server_pool *sp)
{
  rstatus_t rv = NC_OK;
  // "shard" is owned by server-pool
  srv_sd->owner = sp;
  srv_sd->range_begin = conf_sd->range_begin;
  srv_sd->range_end = conf_sd->range_end;

  struct server *srv;

  // Add the shard's master server to pool's server[]
  struct conf_server *conf_srv = &conf_sd->master;
  if (is_valid_conf_server(conf_srv)) {
    srv = array_push(&sp->server);
    srv->idx = array_idx(&sp->server, srv);
    rv = conf_server_to_server(conf_srv, srv);
    if (rv != NC_OK) {
      log_error("failed to init master server\n");
      goto error_master;
    }
    srv->owner = sp;
    srv->owner_shard = srv_sd;
    srv_sd->master = srv;
  } else {

    srv_sd->master = NULL;
  }

  // Now, init the shard's slaves.
  // During init, a shard may have no slave online.
  array_init(&srv_sd->slaves, 4, sizeof(struct server*));

  uint32_t slaves_added = 0;

  for (uint32_t i = 0; i < array_n(&conf_sd->slaves); i++) {
    conf_srv = array_get(&conf_sd->slaves, i);
    if (!is_valid_conf_server(conf_srv)) {
      continue;
    }
    srv = array_push(&sp->server);
    slaves_added++;

    srv->idx = array_idx(&sp->server, srv);

    // Add a slave server to the pool's server[]
    rv = conf_server_to_server(conf_srv, srv);
    if (rv != NC_OK) {
      log_error("failed to init slave server\n");
      goto error_slave;
    }

    //srv = array_get(&sp->server, array_n(&sp->server) - 1);
    srv->owner = sp;
    srv->owner_shard = srv_sd;

    struct server** ppsrv = array_push(&srv_sd->slaves);
    *ppsrv = srv;
  }

  // Init a shard status from conf_shard status.
  set_server_shard_status(conf_sd, srv_sd);

  return rv;

error_slave:
  // Discard the newly added servers.
  for (uint32_t i = 0; i < slaves_added; i++) {
    array_pop(&sp->server);
  }

error_master:
  array_pop(&sp->server);

  return rv;
}


//find_server_shard(JSON_Object *jobj, struct array *srv_shards)
//uint32_t begin = atoi(json_object_get_string(jobj, "shard_begin"));
//
// Given a conf-shard, find the matching server-shard.
//
static struct shard*
find_server_shard(struct conf_shard *conf_sd, struct array *srv_shards)
{
  uint32_t begin = conf_sd->range_begin;

  for (uint32_t i = 0; i < array_n(srv_shards); i++) {
    struct shard *srv_sd = array_get(srv_shards, i);
    if (srv_sd->range_begin == begin) {
      return srv_sd;
    }
  }

  return NULL;
}


// Given a conf_srv, find a match "server" obj from the srvs array.
//
// NOTE: the "srvs" array contains "struct server" pointers, not real objs.
//
static struct server*
find_server_from_conf_server(struct conf_server *conf_srv, struct array *srvs)
{
  for (uint32_t i = 0; i < array_n(srvs); i++) {
    struct server* srv = *(struct server **)array_get(srvs, i);
    if (conf_match_server(conf_srv, srv)) {
      return srv;
    }
  }
  return NULL;
}


// Check if a conf_server obj matches with a "server" obj.
static bool
conf_match_server(struct conf_server *conf_srv, struct server *srv)
{
  if (!srv) return false;

  if (srv->name.len != conf_srv->name.len ||
      strncmp((const char*)srv->name.data,
              (const char*)conf_srv->name.data,
              srv->name.len)) {
    return false;
  }
  return true;
}

// When the pool's topology is changed (such as migration, split, new slaves
// join, etc), the pool config znode is updated, and this method is called
// to update the server_pool members to reflect the topology change.
//
// NOTE: caller must have acquired pool lock before coming here.
//
// "pobj":        a json obj representing the pool's new config.
// "srv_shards":  array of currenet shards in the pool.
// "sp" :         the owner server_pool.
//
static rstatus_t
update_server_shards_from_conf_json(JSON_Object *pobj,
                                    struct array *srv_shards,
                                    struct server_pool *sp)
{
  rstatus_t rv = NC_OK;

  if (!sanity_check_pool_conf_json(pobj)) {
    log_error("invalid json conf object\n");
    return NC_ERROR;
  }

  // Make sure pool name matches.
  const char *pool_name = json_object_get_string(pobj, "name");
  if (strlen(pool_name) != strlen((const char*)sp->name.data) ||
      strncmp(pool_name, (const char*)sp->name.data, strlen(pool_name))) {
    log_error("pool conf znode name %s != pool name %s\n",
              pool_name, sp->name.data);
    return NC_ERROR;
  }

  // Convert the json obj to a conf_pool object.
  struct conf_pool cfpool;
  memset(&cfpool, 0, sizeof(struct conf_pool));
  rv = conf_json_to_conf_pool(pobj, &cfpool, (char*)pool_name);

  // Compare the conf_pool against server_pool to find difference.
  struct conf_shard *conf_sd = NULL;
  struct conf_server *conf_srv = NULL;

  struct shard* srv_sd = NULL;
  struct server* srv = NULL;

  uint32_t nshards = array_n(&cfpool.shards);

  for (uint32_t i = 0; i < nshards; i++) {

    // Exam each conf_shard.
    conf_sd = array_get(&cfpool.shards, i);

    srv_sd = find_server_shard(conf_sd, srv_shards);

    ////////////////////////////
    // A new shard is added (due to a split).
    if (srv_sd == NULL) {
      log_warn("found a new conf_shard: [%d : %d]\n",
               conf_sd->range_begin, conf_sd->range_end);
      if (strncmp(conf_sd->status, "running", 7) != 0) {
        log_warn("new shard [%d : %d] is added but status %s, skip it",
                 conf_sd->range_begin, conf_sd->range_end, conf_sd->status);
        continue;
      }

      srv_sd = array_push(srv_shards);
      memset(srv_sd, 0, sizeof(struct shard));

      srv_sd->idx = array_idx(srv_shards, srv_sd);
      rv = conf_shard_to_server_shard(conf_sd, srv_sd, sp);

      // add new servers to stats-server.
      srv = srv_sd->master;
      if (srv) {
        add_server_to_stats(srv, sp);
      }

      for (uint32_t j = 0; j < array_n(&srv_sd->slaves); j++) {
        srv = *(struct server**)array_get(&srv_sd->slaves, j);
        add_server_to_stats(srv, sp);
      }

      continue;
    }

    ////////////////////////////
    // Shard range shrinks (due to split)
    if (conf_sd->range_end != srv_sd->range_end ||
        conf_sd->range_begin != srv_sd->range_begin) {
      log_warn("conf-shard range shrinked. We might have seen a split: "
               "origin [%d : %d], new [%d : %d]\n",
               srv_sd->range_begin, srv_sd->range_end,
               conf_sd->range_begin, conf_sd->range_end);
      srv_sd->range_begin = conf_sd->range_begin;
      srv_sd->range_end = conf_sd->range_end;
    }

    ////////////////////////////
    // Master change (due to migration, or, failover).
    srv = srv_sd->master;
    conf_srv = &conf_sd->master;
    if (!is_valid_conf_server(conf_srv)) {
      // not a valid conf_server, skip.
      log_error("conf_shard has invalid master conf_server");
      continue;
    } else if (!conf_match_server(conf_srv, srv)) {
      log_warn("shard master changed. Use new master: %s\n",
               conf_srv->name.data);

      // replace old master with new master.
      struct server *new_srv =
          AddServerFromAddressString(srv_sd, (char*)conf_srv->name.data);

      add_server_to_stats(new_srv, sp);

      srv_sd->master = new_srv;

      // TODO: how to handle the old master??
    }

    ////////////////////////////
    // More slaves added.
    for (uint32_t j = 0; j < array_n(&conf_sd->slaves); j++) {
      conf_srv = array_get(&conf_sd->slaves, j);
      srv = find_server_from_conf_server(conf_srv, &srv_sd->slaves);
      if (srv == NULL) {
        log_warn("Add a new slave server %s to shard [%d : %d]\n",
                 conf_srv->name.data,
                 srv_sd->range_begin,
                 srv_sd->range_end);
        struct server *new_srv =
            AddServerFromAddressString(srv_sd, (char*)conf_srv->name.data);
        struct server **ppsrv = (struct server**)array_push(&srv_sd->slaves);
        *ppsrv = new_srv;

        // Add new server to stats pool.
        add_server_to_stats(new_srv, sp);
      }
    }

    // Check the shard status.
    set_server_shard_status(conf_sd, srv_sd);
  }

  conf_pool_deinit(&cfpool);

  return rv;
}

static void
set_server_shard_status(struct conf_shard *conf_sd, struct shard *srv_sd)
{
  log_warn("before: shard [%d : %d] status: can-read %d, can-write %d",
           srv_sd->range_begin, srv_sd->range_end,
           srv_sd->can_read, srv_sd->can_write);
  if (strncmp(conf_sd->status, "init", 4) == 0) {
    srv_sd->can_read = 0;
    srv_sd->can_write = 0;
  } else if (strncmp(conf_sd->status, "running", 7) == 0) {
    srv_sd->can_read = 1;
    srv_sd->can_write = 1;
  } else if (strncmp(conf_sd->status, "draining", 8) == 0) {
    srv_sd->can_read = 1;
    srv_sd->can_write = 0;
  } else {
    log_error("unknown shard status %s\n", conf_sd->status);
    srv_sd->can_read = 0;
    srv_sd->can_write = 0;
  }
  log_warn("after: shard [%d : %d] status: can-read %d, can-write %d",
           srv_sd->range_begin, srv_sd->range_end,
           srv_sd->can_read, srv_sd->can_write);
}

static bool
is_valid_conf_server(struct conf_server *conf_srv)
{
  return (conf_srv->name.data != NULL &&
          conf_srv->port != 0 &&
          conf_srv->valid);
}

static void
conf_pool_watcher(zhandle_t *zkh, int type, int state,
                  const char *path, void *ctx)
{
  struct server_pool *sp = (struct server_pool*)ctx;
  ASSERT(sp != NULL);

  log_error("conf_pool_watcher got event %s, state %s at path %s",
            Type2String(type), State2String(state), path);

  if (type == ZOO_CREATED_EVENT || type == ZOO_CHANGED_EVENT) {
    int buflen = 10000;
    char buf[buflen];
    memset(buf, 0, (size_t)buflen);

    int rc = ZKGet(zkh, path, buf, buflen, 0, 1);
    if (rc < 0 || rc >= buflen) {
      log_error("read %s failed, ret %d", path, rc);

    } else {
      // convert the content to a json obj.
      JSON_Value *jroot = json_parse_string_with_comments(buf);
      if (!jroot || (JSONObject != json_type(jroot))) {
        log_error("pool config znode error format : %s", buf);
        goto exit;
      }

      JSON_Object* pool_obj = json_value_get_object(jroot);
      if (!sanity_check_pool_conf_json(pool_obj)) {
        log_error("pool config invalid format: %s", buf);
        goto exit;
      }

      // Apply the updated pool conf to server_pool.
      pthread_mutex_lock(&sp->lock);
      update_server_shards_from_conf_json(pool_obj, &sp->shards, sp);
      pthread_mutex_unlock(&sp->lock);

    }
  } else if (type == ZOO_DELETED_EVENT) {

  }

exit:
  ZKSetExistsWatch(zkh, (char*)path, conf_pool_watcher, ctx);
}


// Add a watcher on a pool's config znode.
rstatus_t
add_watcher_on_conf_pool(struct context *ctx)
{
  if (ctx->zkh == NULL) {
    log_error("NO connection to ZK is established.\n");
    return NC_ERROR;
  }

  size_t pathlen = 500;
  char zkpath[pathlen];
  struct array *pools = &ctx->pool;
  struct instance *inst = ctx->owner_inst;
  rstatus_t rv = NC_OK;

  // For now, we know each context has only one pool.
  for (uint32_t i = 0; i < array_n(pools); i++ ) {

    // Watch on the pool's config znode.
    snprintf(zkpath, pathlen, "%s/%s", inst->zk_config_root, inst->pool_name);

    struct server_pool *sp = (struct server_pool *)array_get(pools, i);
    int rc = ZKSetExistsWatch(ctx->zkh, zkpath, conf_pool_watcher, sp);
    if (rc != ZOK) {
      log_error("failed to set watcher on znode %s", zkpath);
      rv = NC_ERROR;
    } else {
      log_warn("have put watcher on znode %s", zkpath);
      rv = NC_OK;
    }
  }

  return rv;
}
