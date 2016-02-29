MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -I/usr/local/include/zookeeper

noinst_LTLIBRARIES = libproto.la

noinst_HEADERS = nc_proto.h

libproto_la_SOURCES =			\
	nc_memcache.c			\
	nc_redis.c
