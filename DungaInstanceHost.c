/**
 * Implementation file for functions to simulate a cloud-like server instance
 * host.
 * 
 * @author [Sarath Kumar Dunga]
 * @version 
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include "InstanceHost.h"
#include "LoadBalancer.h" 

struct host {
    pthread_mutex_t M;
    bool allRequestsHandled;
    int currHostsCount;
};

typedef struct threadArgs {
    host* hostServer;
    struct job_node* batch;
}threadArgs;


host* host_create() {
    host* h = (host*)malloc(sizeof(host));
    if (h == NULL) {
        fprintf(stderr, "Error: Unable to allocate memory for host.\n");
        exit(EXIT_FAILURE);
    }

    if (pthread_mutex_init(&(h->M), NULL) != 0) {
        fprintf(stderr, "Error: Failed to initialize mutex.\n");
        exit(EXIT_FAILURE);
    }
    //Initializing host data fields
    h->allRequestsHandled = false;
    h->currHostsCount = 0;

    return h;
}


void host_destroy(host** h) {
    if (*h == NULL) {
        return;
    }

    // Clean up any remaining instances
    pthread_mutex_destroy(&((*h)->M));
    free(*h);
    *h = NULL;
}

void* processCurrBatch(void* args) {
    threadArgs* tArgs = (threadArgs*)args;
    host* h = tArgs->hostServer;
    struct job_node* batch = tArgs->batch;

    // Processing the jobs
    while (batch != NULL) {
        //squaring
        *(batch->data_result) = batch->data * batch->data;
        batch = batch->next;
    }

    //Acquiring lock on mutex
    pthread_mutex_lock(&(h->M));

    // Decrease current hosts count
    h->currHostsCount--;

    /*
    //If there are jobs left to be processed
    if (h->currHostsCount == 0 && !h->allRequestsHandled) {
        host_request_instance(h, batch);
        printf("Creating another instance of the server for remaining jobs...\n");
    }
    */

    // Check if all requests are handled
    if (h->currHostsCount == 0 && h->allRequestsHandled) {
        // All requests are handled so, set flag to true
        printf("All requests are handled.\n");
        h->allRequestsHandled = true;
    }
    
    // Unlock mutex
    pthread_mutex_unlock(&(h->M));

    free(tArgs);
    return NULL;
}

void host_request_instance(host* h, struct job_node* batch) {

    pthread_mutex_lock(&(h->M));

    // Increase number of instances
    h->currHostsCount++;

    // Unlock mutex
    pthread_mutex_unlock(&(h->M));

    // Initialize thread arguments
    threadArgs* args = (threadArgs*)malloc(sizeof(threadArgs));
    args->hostServer = h;
    args->batch = batch;

    // Create thread for new instance
    pthread_t thread;
    if (pthread_create(&thread, NULL, processCurrBatch, (void*)args) != 0) {
        fprintf(stderr, "Error: Failed to create thread for new server instance.\n");
        exit(EXIT_FAILURE);
    }
}


