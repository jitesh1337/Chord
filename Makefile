
default: client
	gcc -o dhtmain dhtmain.c -lcrypto -lm

client: client.c
	gcc -o client client.c 

clean:
	rm -f dhtmain client *~
