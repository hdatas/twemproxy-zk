/*
 * 
 */

#include <nc_hcdapi.h>
#include <stdbool.h>
#include <string.h>
#include <nc_core.h>
#include <nc_conf.h>
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
#if 0
static rstatus_t
h_conf_json_init_pool(JSON_Object* obj,
                    struct conf_pool* pool)
{
    rstatus_t status;
    struct string plname;

    // TODO: verify "proxies", "shard_range", "shards", "master", "slaves" exists.
    //
    // init pool name.
    string_init(&plname);
int numobj1 = json_object_get_count(obj);
//    size_t numobj = json_array_get_count(obj);
    const char* name = json_object_get_string(obj, "name");
printf("===wgu==inside 4 name=%s, pool_obj:%08x, pool:%08x, num of obj:%08x\n", name, (int)obj, (int)pool, numobj1);
    status = string_copy(&plname, name, (uint32_t)strlen(name));
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
printf("===wgu===proxy#:%d\n", nproxies);
    ASSERT(nproxies > 0);
    char *inst_proxy_ip;               // proxy listen address
    uint16_t inst_proxy_port;          // proxy listen port

    for (size_t i = 0; i < nproxies; i++) {
        int str_len = 200;
        char proxy_addr[str_len];
        char proxy_str[str_len];
        const char* p = json_array_get_string(jproxies, i);
        if (*p != NULL) {
            inst_proxy_ip   = strsep(&p, ":");
            inst_proxy_port = atoi(strsep(&p, ":"));
printf("===wgu===proxy: ip %s, port %d\n", inst_proxy_ip, inst_proxy_port);
        }
        if (inst_proxy_ip != NULL) {
            snprintf(proxy_addr, (size_t)str_len, "%s:%d", inst_proxy_ip,
                    inst_proxy_port);
//printf("===wgu===proxy: cmd:%s, file:%s\n", proxy_addr, p);
       //     if ((strlen(proxy_addr) != strlen(p)) ||
       //           (strncmp(proxy_addr, p, strlen(proxy_addr)) != 0)) {
       //         continue;
       //     } 
        } else {
            log_debug(LOG_NOTICE, "missing proxy ip:port\n");
            array_deinit(&pool->proxies);
            string_deinit(&pool->name);
            return NC_ERROR;
        }
//printf("====wgu==pool->proxies: %s\n", &pool->proxies);
        struct conf_server* cp = (struct conf_server*) array_push(&pool->proxies);
        if (inst->unix_path == NULL){
            snprintf(proxy_str, (size_t)str_len, "%s", proxy_addr);
        } else {
            snprintf(proxy_str, (size_t)str_len, "%s/%s_%d_%s", inst->unix_path,
                    inst_proxy_ip, inst_proxy_port, name);
        }
printf("===wgu== proxy_str:%s, cp:%08x\n", proxy_str, cp);
        string_to_conf_server((const uint8_t*) proxy_str, cp);
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
if (hv_range>0) {
//    ASSERT(hv_range != NULL);
    ASSERT(json_array_get_count(hv_range) == 2);
    pool->shard_range_min = atoi(json_array_get_string(hv_range, 0));
    pool->shard_range_max = atoi(json_array_get_string(hv_range, 1));
printf("shard min:%d, max:%d\n", pool->shard_range_min, pool->shard_range_max);
    ASSERT(pool->shard_range_min < pool->shard_range_max);
} else {
    pool->shard_range_min = 0;
    pool->shard_range_max = 1000000;
}
    // Init shards.
    status = array_init(&pool->shards, CONF_DEFAULT_SHARDS,
                        sizeof(struct conf_shard));
    JSON_Array* jshards = json_object_get_array(obj, "shards");
    size_t nshards = json_array_get_count(jshards);
    //ASSERT(nshards > 0);

    for (size_t i = 0; i < nshards; i++) {
        // Add a shard into pool.
        struct conf_shard* cfshard = (struct conf_shard*) array_push(&pool->shards);
        if (cfshard == NULL) {
            status = NC_ENOMEM;
            break;
        }
        memset(cfshard, 0, sizeof(struct conf_shard));

        JSON_Object* js = json_array_get_object(jshards, i);
        cfshard->range_begin = (uint32_t)(atoi)(json_object_get_string(js, "shard_begin"));
        cfshard->range_end = (uint32_t)(atoi)(json_object_get_string(js, "shard_end"));

printf("shard start:%d, end:%d\n", cfshard->range_begin, cfshard->range_end);
        // Add master / slave conf_servers in each conf_shard.
        const char* jsmaster = json_object_get_string(js, "master_target");
        string_to_conf_server((const uint8_t*)jsmaster, &cfshard->master);

        JSON_Array* jsslaves = json_object_get_array(js, "slave_target");
        size_t nslaves = json_array_get_count(jsslaves);
        if (nslaves>0) {
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
    }

    return NC_OK;
}
#endif

static rstatus_t
h_conf_json_parse(struct conf *cf, 
              struct msg *msg, struct context *ctx,
              JSON_Value *jvalue, JSON_Object* jobject)
{
    rstatus_t status = NC_OK;

    JSON_Value *root_value = jvalue;
    ASSERT(root_value != NULL);

    JSON_Array* pools = &ctx->pool;
    size_t num_pools = json_array_get_count(pools);

printf("====wgu===h_conf_json_parse, pool: %s, pools: %08x, total pools: %d\n", cf->pool, (int)pools, num_pools);
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
printf("====wgu 2===before conf_json_parse, pool_obj=%08x, pool:%08x, pools: %08x, num-obj:%d\n", (int)pool_obj, pool, pools, nobj2);
    status = conf_json_init_pool(pool_obj, pool, ctx->owner_inst);
    if (status != NC_OK) {
printf("====wgu 2===conf_json_parse, pool_obj=%08x, pool:%08x, pools: %08x\n", (int)pool_obj, pool, pools);
            array_pop(&cf->pool);
            return status;
    }
    pool->valid = 1;
printf("====wgu 33===conf_json_parse, pool_obj=%08x, pools: %08x\n", (int)pool_obj, pools);
    
    json_value_free(root_value);

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

bool
h_ctx_create(struct conf_pool *conf_pool, struct array *server_pool)
{
printf("====wgu===ctx-create before transform. cp name:%s\n", conf_pool->name.data);
    conf_pool_each_transform(conf_pool, server_pool);
printf("====wgu===ctx-create\n");

}

bool
h_create_pool(char *buffer, unsigned len,  
              struct msg *msg, struct context *ctx,
              JSON_Value *jvalue, JSON_Object* jobject)
{
    rstatus_t status = NC_OK;
    nc_write_file("h_tempfile", buffer, strlen(buffer)); 

JSON_Array* pools = &ctx->pool;
size_t num_pools = json_array_get_count(pools);

printf("====wgu===h_create_pool, before pool #: %d, NC_OK=%d\n", array_n(pools), NC_OK);
    struct conf *cf;
    cf = h_conf_json_create("h_tempfile", msg, ctx, jvalue, jobject);
    remove("h_tempfile");
    if (cf == NULL) {
      return NULL;
    }

printf("====wgu===h_create_pool, after pool #: %d, cp->valid:%d, cp:%08x, &ctx->pool:%08x\n", 
       array_n(pools), cf->valid, cf, &ctx->pool);
    struct conf_pool *cf_pool = array_get(&cf->pool, 0);
    h_ctx_create(cf_pool, &ctx->pool);
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

    ASSERT(array_n(pools)>0);
//    ASSERT(true==setparser(buffer, &keybuf, &valuebuf));
    if (false==setparser(buffer, &keybuf, &valuebuf)) {
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


