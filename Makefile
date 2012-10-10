# makefile for comp2310 assignment 2, 2012 
# written by Peter Strazdins RSCS ANU, 09/12                                   
# version 1.0 30/10/21

# usage: to compile and link the bsystem, use 
#	make 
# To remove all generatec files, use
#	make clean


.SUFFIXES:

OBJS=quicklib.o distquicklib.o
EXE=quicksort

CC=clang
CFLAGS=-O2 -Wall -Wextra -g
INCLUDEFLAGS=
ARCHFLAGS=

LDFLAGS=#-lmcheck -lpthread -lm
# uncomment this if you get the `memory clobbered before allocated block'
# problem for quickThreads() 
#LDFLAGS=-lpthread -lm 

default: $(EXE)

%: %.o $(OBJS)
	$(CC) $(CFLAGS) -o $* $*.o $(OBJS) $(LDFLAGS)

%.o: %.c  
	$(CC) $(CFLAGS) $(ARCHFLAGS) $(INCLUDEFLAGS) -c $*.c

clean:
	rm -f $(EXE) *.o
