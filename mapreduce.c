#include "mapreduce.h"
#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

Mapper map_func;
Reducer reduce_func;
Partitioner partition_func;

int num_partitions;

// LINKED LIST: FOR MEMORY EFFICIENTY, LINKED LIST WILL BE USED TO STORE MAPS

// map_node: KEY, VALUE, AND NEXT MAP NODE ARE STORED IN map_node
struct map_node{
	char * key;
	char * value;
	struct map_node * next;
};

struct map_node ** iterators; // for get_next

// partition: Store the locks and the heads of partition linked lists.
struct partition{
	pthread_mutex_t plock;
	struct map_node * head;
};

struct partition * partition_list;	

// Map Thread
void * map_thread(void * file_name){
	map_func(*(char **) file_name);
	return NULL;
}


// Insertion sort for linked list
// Edited from https://www.geeksforgeeks.org/insertion-sort-for-singly-linked-list/
void sortedInsert(struct map_node ** head, struct map_node * new) 
{ 
    struct map_node * current; 
    /* Special case for the head end */
    if (*head == NULL || strcmp((*head)->key, new->key) >= 0) 
    { 
        new->next = *head; 
        *head = new;
    } 
    else
    { 
        /* Locate the node before the point of insertion */
        current = *head; 
        while (current->next != NULL && strcmp(current->next->key,new->key) < 0) 
        { 
            current = current->next; 
        } 
        new->next = current->next;
        current->next = new;
    } 
	return;
} 

void * insertionSort(void * partition_number) 
{ 
	// Get the partition
	int pn = *(int *) partition_number;
	struct map_node ** head = malloc(sizeof(struct map_node *));
	*head = partition_list[pn].head;
    // Initialize sorted linked list 
    struct map_node *sorted = NULL; 
  
    // Traverse the given linked list and insert every 
    // node to sorted 
    struct map_node *current = *head;
    while (current != NULL) 
    { 
        // Store next for next iteration 
        struct map_node *next = current->next; 
  
        // insert current in sorted linked list 
        sortedInsert(&sorted, current); 
  
        // Update current 
        current = next; 
    } 
  
    // Update head_ref to point to sorted linked list 
    partition_list[pn].head = sorted;
	return NULL;
}

char * get_next(char * key, int partition_number){
	struct map_node * iterator = iterators[partition_number];
	
	// go until key doesn't match
	if(iterator != NULL){
		if(strcmp(iterator->key, key) != 0){
			return NULL;
		}
		char * val = iterator->value;
		iterator = iterator->next;
		iterators[partition_number] = iterator;
		return val;
	}
	return NULL;
		
}

// Reducer thread
void * reducer_thread(void * partition_number){
	int pn = *(int *) partition_number;
	if(partition_list[pn].head == NULL)
		return NULL;
	struct map_node * cur = partition_list[pn].head;
	
	if(cur != NULL){
		reduce_func(cur->key, get_next, pn);
	}
	while(cur != NULL){
		if(cur->next == NULL)
			return NULL;
		if(strcmp(cur->key, cur->next->key) != 0){
			reduce_func(cur->next->key, get_next, pn);
		}
		cur = cur->next;
	}
	return NULL;
}

// ADD MAP TO LINKED LIST OF MAPS (map_head)
void
MR_Emit (char * key, char * value)
{
	// To prevent storing empty keys
	if(strlen(key) == 0)
		return;
	
	// GET THE PARTITION NUMBER FOR KEY
	int pn = partition_func(key, num_partitions);
	
	// Create a new node with key and value
	struct map_node * new_node = malloc(sizeof(struct map_node));
	new_node -> key = malloc(strlen(key)+1); // BECAUSE SIZE OF CHAR = 1, strlen(key)+1 = SIZE OF BYTES NEEDED FOR key.
	strcpy(new_node->key, key); 			 // COPY KEY AND VALUE TO NODE'S KEY AND VALUE
	new_node -> value = malloc(strlen(value)+1);
	strcpy(new_node->value, value);
	
	// Add to the right partition
	pthread_mutex_lock(&partition_list[pn].plock);
	new_node->next = partition_list[pn].head;
	partition_list[pn].head = new_node;
	pthread_mutex_unlock(&partition_list[pn].plock);
	
	return;
}


unsigned long
MR_DefaultHashPartition (char * key, int num_buckets)
{
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_buckets;
    //return -1;
}


void
MR_Run (int argc, char * argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Partitioner partition)
{
	// Initialize
	map_func = map;
	reduce_func = reduce;
	partition_func = partition;
	num_partitions = num_reducers;
	
	partition_list = malloc(sizeof(struct partition) * num_partitions);
	iterators = malloc(sizeof(struct map_node) * num_partitions);
	
	// Adjust num_mappers to num_files
	int num_files = argc-1;
	if(num_mappers != num_files)
		num_mappers = num_files;
	
	// CREATE MAPPER THREADS
	pthread_t mappers[num_mappers];
	int error;
	for (int i = 0; i < num_mappers; i++) {
		error = pthread_create(&mappers[i], NULL, map_thread, &argv[i+1]);
		assert(!error);	
	}
	
	// JOIN MAPPER THREADS
	for (int i = 0; i < num_mappers; i++) {
		error = pthread_join(mappers[i], NULL);
		assert(!error);
	}
	
	
	// Sort partitions
	pthread_t sorters[num_partitions];
	for (int i = 0; i < num_partitions; i++) {
		int * pn = malloc(sizeof(int));
		*pn = i;
		error = pthread_create(&sorters[i], NULL, insertionSort, pn);
		assert(!error);
	}
	
	for (int i = 0; i < num_partitions; i++) {
		error = pthread_join(sorters[i], NULL);
		assert(!error);
	}
	
	// Set iterators for go_next
	for(int i=0; i<num_partitions; i++){
		iterators[i] = partition_list[i].head;
	}
	
	// CREATE REDUCER THREADS
	pthread_t reducers[num_reducers];
	for (int i = 0; i < num_reducers; i++) {
		int * pn = malloc(sizeof(int));
		*pn = i;
		error = pthread_create(&reducers[i], NULL, reducer_thread, pn);
		assert(!error);
	}
	
	// JOIN REDUCER THREADS
	for (int i = 0; i < num_reducers; i++) {
		error = pthread_join(reducers[i], NULL);
		assert(!error);
	}
	
	// Free memory
	
	struct map_node * n;
	struct map_node * tmp;
	for(int i=0; i<num_partitions; i++){
		n = partition_list[i].head;
		while(n != NULL){
			tmp = n;
			n = n->next;
			free(tmp);
		}
	}
	
	free(partition_list);
	free(iterators);
	
	return;
}
