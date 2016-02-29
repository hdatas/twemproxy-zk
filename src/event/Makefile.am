MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -I/usr/local/include/zookeeper

noinst_LTLIBRARIES = libevent.la

noinst_HEADERS = nc_event.h

libevent_la_SOURCES =	\
	nc_epoll.c	\
	nc_kqueue.c	\
	nc_evport.c

