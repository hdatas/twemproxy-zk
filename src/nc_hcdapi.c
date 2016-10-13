/*
 * 
 */

#include <nc_hcdapi.h>
#include <stdbool.h>
#include <string.h>
#include <nc_core.h>
#include <nc_conf.h>
#include <nc_stats.h>
#include <nc_server.h>
#include <parson/parson.h>

int proxy_ret_port;  // >0 for new proxy port number

/*
 * Input:  buffer with redis SET parameters  
 * Output: key
 *         value
 */
static bool 
setparser(char *buffer, char **key, char **value) 
{
    char *keybuf;

    char *words[18], *aPtr;
    int count = 0;
    if (!buffer) 
        return false;
    while( aPtr = strsep(&buffer, "\x0d\x0a") ) {
        if (count==8) {
          keybuf = aPtr;
        }
        /* only need the count at 8 and 12 */
        if (count>14)
          break;
        words[count++] = aPtr;
    }
    /* skip the last "\0" of keybuf, if needed */
    if (count<12) {    
        while (*(++keybuf) != NULL); 
        keybuf++;
        while (aPtr = strsep(&keybuf, "\x0d\x0a")) {
            if (count>14)
              break;
            words[count++] = aPtr;
        }
    }
    /* fail if no enough parameters to parse in */
    if (count<12)
        return false;
    *key   = words[8];
    *value = words[12];

    return true;
}

static rstatus_t
h_conf_json_parse(struct conf *cf, 
              struct msg *msg, struct context *ctx,
              JSON_Value *jvalue, JSON_Object* jobject)
{
    rstatus_t status = NC_OK;

    JSON_Value *root_value = jvalue;
    ASSERT(root_value != NULL);

    struct conf_pool* pool = (struct conf_pool*) array_push(&cf->pool);
    if (pool == NULL) {
        status = NC_ENOMEM;
        return status;
    }

    JSON_Object* pool_obj = jobject;
    status = conf_json_init_pool(pool_obj, pool, ctx->owner_inst);
    if (status != NC_OK) {
            array_pop(&cf->pool);
            return status;
    }
    pool->valid = 1;

    return status;
}


static struct conf *
h_conf_json_create(char *filename, struct msg *msg, struct context *ctx,
                   JSON_Value *jvalue, JSON_Object* jobject)
{
    rstatus_t status = NC_OK;
    struct conf *cf;
    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }
    fclose(cf->fh);
    cf->fh = NULL;
    array_null(&cf->arg);

    // Parse conf file.
    status = h_conf_json_parse(cf, msg, ctx, jvalue, jobject);
    if (status != NC_OK) {
        goto error;
    }

    // validate parsed configuration
    status = conf_json_post_validate(cf);
    if (status != NC_OK) {
        goto error;
    }

    cf->valid = 1;

    return cf;

error:
    log_stderr("nutcracker: temp configuration file '%s' JSON syntax invalid",
               filename);
    conf_destroy(cf);
    return NULL;
}

static rstatus_t
h_stats_pool_map(struct array *stats_pool, struct array *server_pool)
{
    rstatus_t status;
    uint32_t npool;

    npool = array_n(server_pool);
    ASSERT(npool != 0);

    struct server_pool *sp = array_get(server_pool, npool-1);
    struct stats_pool *stp = array_push(stats_pool);

    status = stats_pool_init(stp, sp);
    if (status != NC_OK) {
        return status;
    }

    log_debug(LOG_VVVERB, "map %"PRIu32" stats pools", npool);

    return NC_OK;
}

static bool
h_hdr_init(struct server_pool* sp)
{
    // Init hdr-histogram
    int64_t lowest = 1;
    int64_t highest = 1000000000;
    int sig_digits = 3;

    if (hdr_init(lowest, highest, sig_digits, &sp->histogram) == ENOMEM) {
       return false;
    }
    pthread_mutex_init(&sp->histo_lock, NULL);
    return true;
}

static struct stats *
h_stats_create(struct stats *st, struct array *server_pool)
{
    rstatus_t status=NC_OK;

    status = h_stats_pool_map(&st->current, server_pool);
    if (status != NC_OK) {
        goto error;
    }

    status = h_stats_pool_map(&st->shadow, server_pool);
    if (status != NC_OK) {
        goto error;
    }

    status = h_stats_pool_map(&st->sum, server_pool);
    if (status != NC_OK) {
        goto error;
    }

    return st;

error:
    return NULL;
}


static bool
h_ctx_create(struct context *ctx, struct conf_pool *conf_pool, 
             struct array *server_pool, JSON_Object* pool_obj, struct msg* msg)
{
    conf_pool_each_transform(conf_pool, server_pool);

    struct server_pool *sp;
    sp = array_get(&ctx->pool, array_n(&ctx->pool)-1);
    sp->ctx = ctx;
    if (h_hdr_init(sp)==false) {
        return false;
    }
    if (h_stats_create(ctx->stats, server_pool) == false) {
        return false;
    }
    // Apply the updated pool conf to server_pool.
    pthread_mutex_lock(&sp->lock);
    update_server_shards_from_conf_json(pool_obj, &sp->shards, sp);
    pthread_mutex_unlock(&sp->lock);
 
    if (proxy_each_init(array_get(&ctx->pool, array_n(&ctx->pool)-1), NULL) == NC_OK) {
        return true;
    } else {
        log_debug(LOG_NOTICE, "Fail to start proxy for new pool \"%s\"", sp->name.data);
        return false;
    }
}

static bool
h_create_pool(char *buffer, unsigned len,  
              struct msg *msg, struct context *ctx,
              JSON_Value *jvalue, JSON_Object* jobject)
{
    nc_write_file("h_tempfile", buffer, (int)strlen(buffer)); 

    struct conf *cf;
    cf = h_conf_json_create("h_tempfile", msg, ctx, jvalue, jobject);
    remove("h_tempfile");
    if (cf == NULL) {
      return NULL;
    }

    struct conf_pool *cf_pool = array_get(&cf->pool, 0);

    if (h_ctx_create(ctx, cf_pool, &ctx->pool, jobject, msg)) {
        log_debug(LOG_NOTICE, "HCD done creating a new pool ");
        return true;
    }  
    return false;
}

bool 
hcdset(char *buffer, unsigned len, struct msg *msg)
{
    char *keybuf, *valuebuf;
    struct conn *c_conn;
    struct msg *response = msg->peer;
    struct server_pool *pool;

    c_conn = response->owner;
    pool = c_conn->owner;
    struct context *ctx = pool->ctx;
    struct array *pools = &ctx->pool;
    log_debug(LOG_NOTICE, "hcd msg received at:%08x, owner:%08x, ctx:%08x, pname:%s", 
              msg, msg->owner, ctx, pool->name.data);
    ASSERT(array_n(pools)>0);
    if (setparser(buffer, &keybuf, &valuebuf) == false) {
        return false;
    }

    JSON_Value *jroot = json_parse_string_with_comments(valuebuf);
    if (!jroot || (JSONObject != json_type(jroot))) {
        log_error("pool config error format : %s, len: %d", valuebuf, len);
        return false;
    }
    JSON_Object* pool_obj = json_value_get_object(jroot);
    ASSERT(pool_obj!=NULL);
    char *name_input = json_object_get_string(pool_obj, "name");
    ASSERT(name_input!=NULL);
    log_debug(LOG_NOTICE, "Total pools:%d, Key = %s, Value = %s", 
              array_n(pools), keybuf, valuebuf);
    bool same_pool=false;
    struct server_pool *sp;
    for (uint32_t i = 0; i < array_n(pools); i++ ) {
        sp = (struct server_pool *)array_get(pools, i);  
        if (strncmp(name_input, sp->name.data, strlen(sp->name.data))==0) {
            same_pool = true;
            break;
        } 
    }

    if (same_pool) {
        log_debug(LOG_NOTICE, "Update the existing pool config at \"%s\"", name_input); 
        return hcdsetbuf(valuebuf, len, sp, msg, ctx);
    } else {
        log_debug(LOG_NOTICE, "Create a new pool config for \"%s\"", name_input); 
        return h_create_pool(valuebuf, len, msg, ctx, jroot, pool_obj);
    }
}

bool 
hcdget(char *buffer, unsigned len) 
{
    printf("inside hcdget\n");
    return true;
}


