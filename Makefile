
default: client
	gcc -o dhtmain dhtmain.c -lcrypto

client:
	gcc -o client client.c 

clean:
	rm -f dhtmain client *~
