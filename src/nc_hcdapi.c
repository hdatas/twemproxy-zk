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
bool setparser(char *buffer, char **key, char **value) {
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

bool hcdset(char *buffer, unsigned len, struct server_pool *pool) {
    char *keybuf, *valuebuf;

    ASSERT(true==setparser(buffer, &keybuf, &valuebuf));
    log_debug(LOG_NOTICE, "Key   = %s\nValue = %s\n", keybuf, valuebuf);

    return hcdsetbuf(valuebuf, len, pool);
}

bool hcdget(char *buffer, unsigned len) {
    printf("inside hcdget\n");
    return true;
}

