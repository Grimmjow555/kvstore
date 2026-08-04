#include "server.h"
#include <stdio.h>

/*
 *msg: request message
 *length: length of request message
 *response: need to send
 *@return: length of response
 */

int kvs_protocol(char* msg, int length, char* response) {
    printf("[kvs_protocol] recv %d: %s\n", length, msg);
    return 0;
}

int kvs_request(struct conn* c) {
    printf("recv %d: %s\n", c->rlength, c->rbuffer);
    c->wlength = kvs_protocol(c->rbuffer, c->rlength, c->wbuffer);

    return 0;
}
int kvs_response(struct conn* c) {
    printf("recv %d: %s\n", c->wlength, c->wbuffer);
    return 0;
}