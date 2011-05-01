all:	default client

default: dhtmain.c
	gcc -o dhtmain dhtmain.c -lcrypto -lm -lpthread

client: client.c
	gcc -o client client.c 

clean:
	rm -f dhtmain client *~
