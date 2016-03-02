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
#ifndef _NC_H_
#define _NC_H_
#include <stdint.h>
#include <stdlib.h>
int nc_run_standalone(int argc, char **argv);
int nc_run_lib(int log_level, char *log_file, char *conf_file, char *pid_file,
        char *zk_servers, char *zk_root, uint16_t stats_port, char *stats_addr,
        int stats_interval, size_t mbuf_size, uint16_t proxy_port, char
        *proxy_name);
#endif
