/*
 *   CSC 501 - HW5 sample code
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>

#define BUFLEN 512

/*
 * Example:
 *
 * client 50000 "PUT:key1:value1"
 *
 * valid commands:
 * PUT, GET, END, PRINT
 *
 *  Syntax: ./client portnumber command
 */

int main(int argc, char *argv[]) {
	struct sockaddr_in sr;
	int s, i, slen = sizeof(sr);
	char buf[BUFLEN];
	struct hostent *hent;

	int portnum;

	if (argc != 3) {
		printf("Correct syntax: %s <portnum> <message>", argv[0]);
		return 1;
	}

	portnum = atoi(argv[1]);
	strcpy(buf, argv[2]);

	hent = gethostbyname("localhost");
	if(hent == NULL)
	{	printf("gethostbyname failed ");
		exit(1);
	}

	if ((s = socket(AF_INET, SOCK_STREAM, 0)) == -1) {}

	memset((char *) &sr, 0, sizeof(sr));
	sr.sin_family = AF_INET;
	sr.sin_port = htons(portnum);
	sr.sin_addr = *(struct in_addr*)(hent ->h_addr_list[0]);

	if (connect(s, (struct sockaddr *) &sr, sizeof(sr)) == -1) {
		printf("connect error");
		exit(1);
	}

	/* send a message to the server PORT on machine HOST */
	if (send(s, buf, BUFLEN, 0) == -1) {
		printf("send error");
		exit(1);
	}

	close(s);
	return 0;
}

