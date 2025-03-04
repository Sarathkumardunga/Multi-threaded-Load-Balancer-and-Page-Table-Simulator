/**
 * Implementation file for functions to simulate a load balancer.
 * 
 * @author [Your Name]
 * @version 1.0
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "LoadBalancer.h"

struct balancer {
    int currReqCount;
    int batchCapacity;
    struct job_node* startPtr;
    struct job_node* endPtr;
    pthread_mutex_t M;
};


balancer* balancer_create(int batch_size) {
    balancer* lb = (balancer*)malloc(sizeof(balancer));
    // if there is an error in creating the load balancer
    if (lb == NULL) {
        fprintf(stderr, "Error: Unable to allocate memory for load balancer.\n");
        exit(EXIT_FAILURE);
    }

    // Initializing load balancer data field
    lb->currReqCount = 0;
    lb->batchCapacity = batch_size;
    lb->startPtr = NULL;
    lb->endPtr = NULL;

    // Initialize mutex
    if (pthread_mutex_init(&(lb->M), NULL) != 0) {
        fprintf(stderr, "Error: Failed to initialize mutex.\n");
        exit(EXIT_FAILURE);
    }

    return lb;
}

void balancer_destroy(balancer** lbAddr) {
    if (!lbAddr || !(*lbAddr)) 
        return;

    balancer* lb = *lbAddr;
    //Acquiring lock on mutex for last instance of server
    pthread_mutex_lock(&(lb->M));
    // If there are any additional jobs left process them at last before destroying the balancer.
    if (lb->currReqCount > 0) {
        host_request_instance(host_create(), lb->startPtr);

        lb->batchCapacity = 0;
        lb->startPtr = NULL;
        lb->endPtr = NULL;
    }

    pthread_mutex_unlock(&(lb->M));
    //And finally destroying the mutex
    pthread_mutex_destroy(&(lb->M));

    free(lb);
    *lbAddr = NULL;
}


void balancer_add_job(balancer* lb, int user_id, int data, int* data_return) {
    // Create new job node
    struct job_node* new_job = (struct job_node*)malloc(sizeof(struct job_node));
    if (new_job == NULL) {
        fprintf(stderr, "Error: Unable to allocate memory for new job node.\n");
        exit(EXIT_FAILURE);
    }
    new_job->user_id = user_id;
    new_job->data = data;
    new_job->data_result = data_return;
    new_job->next = NULL;

    
    pthread_mutex_lock(&(lb->M));

    // Adding new job request to end of linked list
    if (lb->startPtr == NULL) {
        lb->startPtr = new_job;
    } else {
        lb->endPtr->next = new_job;
    }
    lb->endPtr = new_job;

    // Increment request count
    lb->currReqCount++;

    // If batch is full, request a new instance
    if (lb->currReqCount >= lb->batchCapacity) {
        printf("LoadBalancer: Received batch and spinning up new instance.\n");

        // Requestinfg a new host for this batch
        host_request_instance(host_create(), lb->startPtr);

        // Resetting the balancer for next batch
        lb->currReqCount = 0;
        lb->startPtr = NULL;
        lb->endPtr = NULL;
    }

    // Unlock mutex
    pthread_mutex_unlock(&(lb->M));
}
