MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LTLIBRARIES = libhdrhistogram.la

noinst_HEADERS = hdr_histogram.h

libhdrhistogram_la_SOURCES =	hdr_histogram.c \
                      hdr_encoding.c  \
                      hdr_histogram_log.c \
                      hdr_interval_recorder.c \
                      hdr_thread.c \
                      hdr_time.c \
                      hdr_writer_reader_phaser.c
