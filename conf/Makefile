
CC = g++ -std=c++11
all : loadconf

loadconf : loadconf.cc
	$(CC) $^ -l jsoncpp -l hiredis -o $@
