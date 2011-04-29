/*
 *	  CSC 501 - HW5 sample code
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <openssl/md5.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <time.h>
#include <fcntl.h>
#include <sys/file.h>
#include <math.h>

#define BUFLEN 512
#define NODEFILE	"nodelist"

#define MAX_NODES	512
#define DEBUG		0

int curr_host;
int TOTAL_NODES;
int portnum = 50000;
int my_portnum, is_initiator = 0;

void forward_message(int port, char *m);

struct node_entry {
	int portnum;
	unsigned char h[16];
};
struct node_entry node_list[MAX_NODES];
struct node_entry my_successor;
struct node_entry my_predecessor; 

unsigned char myhash[16];

struct node_entry finger_table[128];

int well_known_port = 10000;

#define	MAX_TUPLES 1000
struct key_val {
	char key[128];
	char value[128];
} key_vals[MAX_TUPLES];

void printhash(unsigned char h[16])
{	 int i;
	 for(i=0;i<16;i++)
		 printf("%02x",h[i]);
}

void sprinthash(unsigned char h[16], char op[17])
{	 int i;
	 for(i=0;i<16;i++)
	 	sprintf(op[i], "%02x", h[i]);
	 op[i] = '\0';
}
void copyhash(unsigned char d[16], unsigned char s[16])
{
	int i;
	for(i=0;i<16;i++)
		d[i]=s[i];
}

/*
 *  calculates the hash and stores in h
 */
void calculatehash(char *c, int len, char *h)
{	MD5(c,len, h);
}

int get_file_length(int fd)
{
	int end = lseek(fd, 0, SEEK_END);
	lseek(fd, 0, SEEK_SET);
	return end;
}

void get_hash(int portnum, unsigned char h[16]) 
{
	int i;
	for (i=0 ; i<TOTAL_NODES ; i++) {
		if ( node_list[i].portnum == portnum ) {
			copyhash(h, node_list[i].h);
			break;
		}
	}
}

void add(unsigned char h[16], int i, unsigned char r[16])
{
        unsigned char t[16];
        int j, carry=0;

        memset(t, 0, 16);

        t[(15 - i/8)] = 1 << (i%8);

        for ( j=15; j >=0 ; j--) {
                r[j] = h[j] + t[j] + carry;
                carry = 0;
                if ( r[j] < h[j] || r[j] < t[j] ) {
                        carry = 1;
                }
        }
}

void init_node(int portnum, int fd)
{
	int count=0, filelen, port, i;
	char *tok, *ptr, *filestr;
	char h[16];

	lseek(fd, 0, SEEK_SET);
	filelen = get_file_length(fd);
	filestr = malloc(filelen + 1);
	read(fd, filestr, filelen);
	filestr[filelen] = '\0';

	tok = strtok(filestr, "\n");
	while (tok != NULL) {
		/* Remove the ":" between hostname and port */
		ptr = strstr(tok, ":");
		port = atoi(ptr+1);
		while(*ptr != '\0') {
			*ptr = *(ptr + 1);
			ptr++;
		}

		/* Calculate hash */
		calculatehash(tok, strlen(tok), h);

		node_list[count].portnum = port;
		copyhash(node_list[count].h, h);
		if (portnum == port) 
			copyhash(myhash, h);
		count++;

		tok = strtok(NULL, "\n");
	}

}

int compare_nodes(const void *n1, const void *n2)
{
	struct node_entry *n1_s, *n2_s;

	n1_s = (struct node_entry *)n1;
	n2_s = (struct node_entry *)n2;
	return memcmp(n1_s->h, n2_s->h, 16);
}

int find_next(int portnum)
{
	int i;
	for(i = 0; i < TOTAL_NODES; i++) {
		if (node_list[i].portnum == portnum)
			break;
	}

	return node_list[(i + 1) % TOTAL_NODES].portnum;
}

int find_prev(int portnum)
{
	int i;
	for(i = 0; i < TOTAL_NODES; i++) {
		if (node_list[i].portnum == portnum)
			break;
	}

	return node_list[(i + TOTAL_NODES - 1) % TOTAL_NODES].portnum;
}

int is_in_between(unsigned char prev[16], unsigned char next[16], unsigned char result[16])
{
	int ret1, ret2;

	//if (compare_nodes(prev, next) <= 0) {
	if (memcmp(prev, next, 16) <= 0) {
		if (memcmp(prev, result, 16) <= 0 && memcmp(result, next, 16) < 0) {
			#if DEBUG
			printf ("%d %d ", memcmp(prev, result, 16), memcmp(result, next, 16));
			#endif
			return 1;
		}
	} else {
		ret1 = memcmp(prev, result, 16);
		ret2 = memcmp(result, next, 16);
		if ((ret1 < 0  && ret2 > 0) || (ret1 > 0 && ret2 < 0))
			return 1;
	}

	return 0;
}

void create_finger_table(int my_portnum) 
{
	int i, j;
	unsigned char result[16];

	my_successor.portnum = find_next(my_portnum);
	get_hash(my_successor.portnum, my_successor.h);
	my_predecessor.portnum = find_prev(my_portnum);
	get_hash(my_predecessor.portnum, my_predecessor.h);

	#if DEBUG
	printf("My(%d) Successor: %d ", my_portnum, my_successor.portnum);
	printhash(my_successor.h);
	printf("\n");
	printf("My(%d) Predecessor: %d\n", my_portnum, my_predecessor.portnum);
	printhash(my_predecessor.h);
	printf("\n");
	#endif
	memset(key_vals, 0, sizeof(key_vals));

	for ( i=0 ; i<128 ; i++ ) {
		add(myhash, i, result);
		for  (j=0 ; j<TOTAL_NODES ; j++ ) {
			if ( is_in_between(node_list[j].h, node_list[(j+1)%TOTAL_NODES].h, result)) {
				#if DEBUG	
				printhash(node_list[j].h);
				printf(" ");
				printhash(result);
				printf(" ");
				printhash(node_list[(j+1)%TOTAL_NODES].h);
				printf("  %d", is_in_between(node_list[j].h, node_list[(j+1)%TOTAL_NODES].h, result));
				printf("\n");
				#endif
				copyhash(finger_table[i].h, node_list[(j+1)%TOTAL_NODES].h);
				finger_table[i].portnum = node_list[j].portnum;
				break;
			}
		}
	}

#if DEBUG
	for ( i=0 ; i<128 ; i++ ) {
		printf("Finger %d) %d:",i, portnum);
		printhash(finger_table[i].h);
		printf("\n");
	}
#endif
}

void initialize_host(int portnum) 
{
	int fd, filelen, count = 0;
	char *filestr, *ptr, *tok;
	char buf[100], md5[32];
	int next;

	fd = open(NODEFILE, O_RDWR | O_CREAT, 0777);
	if (fd < 0) {
		printf("Error opening file %s\n", NODEFILE);
		exit(1);
	}
	flock(fd, LOCK_EX);

	lseek(fd, 0, SEEK_END);
	sprintf(buf, "localhost:%d\n", portnum);
	write(fd, buf, strlen(buf));

	filelen = get_file_length(fd);
	filestr = malloc(filelen + 1);
	read(fd, filestr, filelen);
	filestr[filelen] = '\0';

	tok = strtok(filestr, "\n");
	while (tok != NULL) {
		count++;
		/* ptr = strstr(tok, ":");
		while(*ptr != '\0') {
			*ptr = *(ptr + 1);
			ptr++;
		}
		printf(".%s.\n", tok);
		*/
		tok = strtok(NULL, "\n");
	}

	if (count == TOTAL_NODES) {
		printf("I am %d and I will now start init\n", portnum);
		init_node(portnum, fd);
		qsort(node_list, TOTAL_NODES, sizeof(struct node_entry), compare_nodes);
		next = find_next(portnum);
		printf("%d: Next port: %d\n", my_portnum, next);
		is_initiator = 1;
		create_finger_table(portnum);
		forward_message(next, "START");

		int i;
		for(i=0;i<TOTAL_NODES;i++) {
			printf("Node %d)%d:",i,node_list[i].portnum);
			printhash(node_list[i].h);
			printf("\n");
		}
	}


	flock(fd, LOCK_UN);
	close(fd);
}

int listen_on_well_known_port()
{
	struct sockaddr_in sock_server, sock_client;
	int s, slen = sizeof(sock_client);
	char *command;
	int client;
	int destport;

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		printf("error in socket creation");
		exit(1);
	}

	memset((char *) &sock_server, 0, sizeof(sock_server));
	sock_server.sin_family = AF_INET;
	sock_server.sin_port = htons(well_known_port);
	sock_server.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(s, (struct sockaddr *) &sock_server, sizeof(sock_server)) == -1) {
		printf("error in binding socket");
		exit(1);
	}

	if (listen(s, 10) == -1) {
		printf("listen error");
		exit(1);
	}

	if ((client = accept(s, (struct sockaddr *) &sock_client, &slen)) == -1) {
		printf("accept error");
		exit(1);
	}

	if (recv(client, command, 10, 0) == -1) {
		printf("recv error");
		exit(1);
	}

	destport = atoi(command);

	return(destport);
}

int find_successor(unsigned char keyhash[16], int flag)
{
	int i;
	char msg[100], hashop[17];

	if ( is_in_between(my_predecessor.h, myhash, keyhash) ) {
		return my_portnum;	
	}

	for (i=0 ; i<128 ; i++) {
		if (is_in_between(finger_table[i].h, finger_table[(i+1)%128].h, keyhash)) {
			sprinthash(keyhash, hashop);
			sprintf(msg, "GET_FORWARD:%d:%s", well_known_port, hashop);
			forward_message(well_known_port, msg);
			if ( flag == 0 )
				return -1;
			else
				return listen_on_well_known_port();
		}
	}
}

/*
 * forwards message m to port
 */
void forward_message(int port, char *m)
{
		struct sockaddr_in sock_client;
		struct hostent *hent;
		int sc, i, slen = sizeof(sock_client);

		if ((sc = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
			printf("socket creation failed ");
			exit(1);
		}

		hent = gethostbyname("localhost");
		if(hent == NULL)
		{	printf("gethostbyname failed ");
			exit(1);
		}

		memset((char *) &sock_client, 0, sizeof(sock_client));

		sock_client.sin_family = AF_INET;
		sock_client.sin_port = htons(port);
		sock_client.sin_addr = *(struct in_addr*)(hent ->h_addr_list[0]);

		if (connect(sc, (struct sockaddr *) &sock_client, slen) == -1) {
			printf("connect failed");
			exit(1);
		}

		if (send(sc, m, BUFLEN, 0) == -1) {
			printf("send failed ");
			exit(1);
		}
		close(sc);
}

void server_listen() {
	struct sockaddr_in sock_server, sock_client;
	int s, slen = sizeof(sock_client);
	char *command, *key, *value, *tmpport, *tmp;
	char buf[BUFLEN], hashop[17], msg[100];
	int client, next, fd, i, destport, tmpportnum;

	unsigned char keyhash[16];

	srand(time(NULL));

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		printf("error in socket creation");
		exit(1);
	}

	memset((char *) &sock_server, 0, sizeof(sock_server));
	sock_server.sin_family = AF_INET;
	sock_server.sin_port = htons(portnum);
	sock_server.sin_addr.s_addr = htonl(INADDR_ANY);

	/* Each server instance created should listen on a different port. Generate a random number between 1024 to 65535.
 	   Keep on generating new random numbers until bind succeeds.
 	 */
	while (bind(s, (struct sockaddr *) &sock_server, sizeof(sock_server)) == -1) {
		portnum = rand() % ( (65535-1024) + 1024);
		if (portnum == well_known_port)
			continue;
		sock_server.sin_port = htons(portnum);
	}
	my_portnum = portnum;

	initialize_host(portnum);

	if (listen(s, 10) == -1) {
		printf("listen error");
		exit(1);
	}

	char nodehash_string[20], hash[16];

	sprintf(nodehash_string,"localhost%d",portnum);
	calculatehash(nodehash_string, strlen(nodehash_string), hash);

	printf("DHT node (");
	printhash(hash);
	printf("): Listening on port number %d . . . \n", portnum);

	while (1) { /* quit only on END message */
		if ((client = accept(s, (struct sockaddr *) &sock_client, &slen)) == -1) {
			printf("accept error");
			exit(1);
		}

		if (recv(client, buf, BUFLEN, 0) == -1) {
			printf("recv error");
			exit(1);
		}

		command = strtok(buf, ":");
		if (strcmp(command, "END") == 0) {

				printf("END message received \n");

		}
		else if (strcmp(command, "GET") == 0) {
			key = strtok(NULL, ":");
			calculatehash(key, strlen(key), keyhash);
			printf("Keyhash :");
			printhash(keyhash);
			printf("\n");
			
			destport =  find_successor(keyhash, 1);

			if (destport == my_portnum) {
				for(i = 0; i < MAX_TUPLES; i++) {
					if (strlen(key) == 0)
						goto close;
					if (strcmp(key, key_vals[i].key) == 0) {
						printf("found %s:%s\n", key, key_vals[i].value);
						goto close;
					}
				}	
			} else {
				sprinthash(keyhash, hashop);
				sprintf(msg, "GET_CONFIDENCE:%s", hashop);
				forward_message(destport, msg);
			}
		}
		else if (strcmp(command, "GET_FORWARD") == 0) {
			tmpport = strtok(NULL, ":");
			tmpportnum = atoi(tmpport);
			tmp = strtok(NULL, ":");
			memcpy(keyhash, tmp, 16);
			
			if ( (memcmp(myhash, keyhash, 16) == 0) || (memcmp(myhash, my_successor.h, 16) == 0)) {
				//send my or succ's portnum over the well-known port
			}
		}
		else if (strcmp(command, "PUT") == 0) {



		} else if (strcmp(command, "START") == 0) {
			printf("%d: Start command received\n", my_portnum);
			if (is_initiator == 1)
				goto close;
			else {
				fd = open(NODEFILE, O_RDWR | O_CREAT, 0777);
				init_node(portnum, fd);
				close(fd);
				qsort(node_list, TOTAL_NODES, sizeof(struct node_entry), compare_nodes);
			}
			next = find_next(my_portnum);
			create_finger_table(my_portnum);
			forward_message(next, "START");
		} else if (strcmp(command, "GET_CONFIDENCE") == 0) {
			key = strtok(NULL, ":");
			for(i = 0; i < MAX_TUPLES; i++) {
				if (strlen(key) == 0)
					goto close;
				if (strcmp(key, key_vals[i].key) == 0) {
					printf("found %s:%s\n", key, key_vals[i].value);
					goto close;
				}
			}
		} else if (strcmp(command, "PUT_CONFIDENCE") == 0) {
			key = strtok(NULL, ":");
			value = strtok(NULL, ":");
			for(i = 0; i < MAX_TUPLES; i++) {
				if (strlen(key_vals[i].key) == 0) {
					strcpy(key_vals[i].key, key);
					strcpy(key_vals[i].value, value);
					goto close;
				}
			}
		}
close:
		close(client);
	}

	close(s);
}

int main(int argc, char *argv[]) {

	if (argc != 2) {
		printf("wrong number of arguments");
		return;
	}

	TOTAL_NODES = atoi(argv[1]);
	//curr_host = atoi(argv[2]);

	//initialize_host();
	server_listen();

	return 0;
}

