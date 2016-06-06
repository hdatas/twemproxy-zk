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

/*
 * Input:  buffer with redis SET parameters  
 * Output: key
 *         value
 */
bool 
setparser(char *buffer, char **key, char **value) 
{
    char *keybuf;

    char *words[18], *aPtr;
    int count = 0;
    if (!buffer) 
        return false;
    while(aPtr = strsep(&buffer, "\x0d\x0a")) {
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

//    Array* pools = &ctx->pool;
//    size_t num_pools = json_array_get_count(pools);

//printf("====wgu===h_conf_json_parse, pool: %s, pools: %08x, total pools: %d\n", cf->pool, (int)pools, num_pools);
printf("=***===wgu===h_parse pool, before pool #: %d\n", array_n(&cf->pool));
    struct conf_pool* pool = (struct conf_pool*) array_push(&cf->pool);
    if (pool == NULL) {
        status = NC_ENOMEM;
        return status;
    }
printf("=***===wgu===h_parse pool, after pool #: %d\n", array_n(&cf->pool));

    JSON_Object* pool_obj = jobject;
    int nobj2 = json_object_get_count(pool_obj);
// pick the first element from json array
//printf("====wgu 2===before conf_json_parse, pool_obj=%08x, pool:%08x, pools: %08x, num-obj:%d\n", (int)pool_obj, pool, pools, nobj2);
    status = conf_json_init_pool(pool_obj, pool, ctx->owner_inst);
    if (status != NC_OK) {
//printf("====wgu 2===conf_json_parse, pool_obj=%08x, pool:%08x, pools: %08x\n", (int)pool_obj, pool, pools);
            array_pop(&cf->pool);
            return status;
    }
    pool->valid = 1;
//printf("====wgu 33===conf_json_parse, pool_obj=%08x, pools: %08x\n", (int)pool_obj, pools);
    
//    json_value_free(root_value);

    return status;
}


struct conf *
h_conf_json_create(char *filename, 
              struct msg *msg, struct context *ctx,
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
//    conf_json_dump(cf);

    return cf;

error:
    log_stderr("nutcracker: temp configuration file '%s' JSON syntax invalid",
               filename);
    conf_destroy(cf);
    return NULL;
}

void 
printctx(struct msg *msg) {
  struct conn *c_conn;
  struct msg *response = msg->peer;
  struct server_pool *pool;
  c_conn = response->owner;
  pool = c_conn->owner;

  printf("!!!!!!!!!!!wgu!! pool->ctx: %08x\n", pool->ctx);
}

static rstatus_t
h_stats_pool_map(struct array *stats_pool, struct array *server_pool)
{
    rstatus_t status;
    uint32_t i, npool;

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
printf("==========================================wgu===stat:%08x\n", st->lock);
//    status = stats_create_buf(st);
//    if (status != NC_OK) {
//        goto error;
//    }

//    status = stats_start_aggregator(st);
//    if (status != NC_OK) {
//        goto error;
//    }

    return st;

error:
//    stats_destroy(st);
    return NULL;

}


bool
h_ctx_create(struct context *ctx, struct conf_pool *conf_pool, 
             struct array *server_pool, JSON_Object* pool_obj, struct msg* msg)
{
    
//    printf("=====wgu==h_ctx_create: array_n(&ctx->pool):%d, sp->lock:%08x\n", 
//           array_n(&ctx->pool), sp->lock);
//    pthread_mutex_init(&sp->lock, NULL);
//    pthread_mutex_lock(&sp->lock);
printf("!!!=====wgu000===ctx:%08x\n", ctx);    
printctx(msg); 
    conf_pool_each_transform(conf_pool, server_pool);
printf("!!!=====wgu00===ctx:%08x\n", ctx);    
printctx(msg); 
    h_stats_create(ctx->stats, server_pool);

printf("!!!=====wgu0===ctx:%08x\n", ctx);    
printctx(msg); 
    struct server_pool *sp;
    sp = array_get(&ctx->pool,array_n(&ctx->pool)-1);
    sp->ctx = ctx;
printf("!!!=====wgu1===ctx:%08x\n", ctx);    
    // Apply the updated pool conf to server_pool.
printctx(msg); 
    pthread_mutex_lock(&sp->lock);
    update_server_shards_from_conf_json(pool_obj, &sp->shards, sp);
    pthread_mutex_unlock(&sp->lock);
 
//    server_pool_each_preconnect(array_get(&ctx->pool,array_n(&ctx->pool)-1), NULL);
    proxy_each_init(array_get(&ctx->pool,array_n(&ctx->pool)-1), NULL);
//    pthread_mutex_unlock(&sp->lock);

}

bool
h_create_pool(char *buffer, unsigned len,  
              struct msg *msg, struct context *ctx,
              JSON_Value *jvalue, JSON_Object* jobject)
{
    rstatus_t status = NC_OK;
    nc_write_file("h_tempfile", buffer, strlen(buffer)); 

//JSON_Array* pools = &ctx->pool;
//size_t num_pools = json_array_get_count(pools);

//printf("====wgu===h_create_pool, before pool #: %d, NC_OK=%d\n", array_n(pools), NC_OK);
    struct conf *cf;
    cf = h_conf_json_create("h_tempfile", msg, ctx, jvalue, jobject);
    remove("h_tempfile");
    if (cf == NULL) {
      return NULL;
    }

printctx(msg); 
    struct conf_pool *cf_pool = array_get(&cf->pool, 0);
    h_ctx_create(ctx, cf_pool, &ctx->pool, jobject, msg);
printf("====wgu===HCD Done===========================\n"); 

    return true;
}

bool 
hcdset(char *buffer, unsigned len, struct msg *msg, struct context *ctx)
{
    char *keybuf, *valuebuf;
    struct conn *c_conn;
    struct msg *response = msg->peer;
    struct server_pool *pool;
    struct array *pools = &ctx->pool;

    c_conn = response->owner;
    pool = c_conn->owner;
    log_debug(LOG_NOTICE, "c_conn proxy = %d, client = %d\n", c_conn->proxy, c_conn->client);

//    msg->owner = c_conn;
printf("!!!=====wgu===msg:%08x, owner:%08x == c_conn:%08x, pool->ctx:%08x, ctx:%08x, pname:%s\n", 
       msg, msg->owner, c_conn, pool->ctx, ctx, pool->name.data);    

    ASSERT(array_n(pools)>0);
//    ASSERT(true==setparser(buffer, &keybuf, &valuebuf));
    if (false == setparser(buffer, &keybuf, &valuebuf)) {
        return false;
    }
    log_debug(LOG_NOTICE, "Key   = %s\nValue = %s\n", keybuf, valuebuf);

    JSON_Value *jroot = json_parse_string_with_comments(valuebuf);
    if (!jroot || (JSONObject != json_type(jroot))) {
        log_error("pool config error format : %s, len: %d", valuebuf, len);
        return false;
    }
    JSON_Object* pool_obj = json_value_get_object(jroot);
    ASSERT(pool_obj!=NULL);
    char *name_input = json_object_get_string(pool_obj, "name");
    ASSERT(name_input!=NULL);
printf("=========wgu===pool array_n: %d\n", array_n(pools));
    bool same_pool=false;
    for (uint32_t i = 0; i < array_n(pools); i++ ) {
        struct server_pool *sp = (struct server_pool *)array_get(pools, i);  
        if (strncmp(name_input, sp->name.data, strlen(sp->name.data))==0) {
            same_pool = true;
            printf("====wgu== pool name is the same, name_input:%s\n", name_input);
            break;
        } 
    }

    if (same_pool) {
        return hcdsetbuf(valuebuf, len, pool, msg, ctx);
    } else {
        return h_create_pool(valuebuf, len, msg, ctx, jroot, pool_obj);
    }
}

bool 
hcdget(char *buffer, unsigned len) 
{
    printf("inside hcdget\n");
    return true;
}


