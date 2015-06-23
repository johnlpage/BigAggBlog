CC=gcc
CFLAGS= -g --std=c99 -D_XOPEN_SOURCE -D_GNU_SOURCE
INCLUDE=-I. -I /usr/local/include/libbson-1.0 -I /usr/local/include/libmongoc-1.0

DEPS = 
OBJ = fastload.o
LIBS = -lmongoc-1.0 -lbson-1.0

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(INCLUDE) $(CFLAGS)

fastload : $(OBJ)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)

clean:
	rm -f *.o fastload
