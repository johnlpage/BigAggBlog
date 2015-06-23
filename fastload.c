/* MongoDB C Loader for MOT data from data.gov */
/* Hard coded to be as fast as it can, sharded or not */
/* (c) John Page 2015 */

#include <sys/wait.h>
#include <sys/time.h>
#include <sys/types.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <stdlib.h>
#include <mongoc.h>
#include <bson.h>
#include <stdio.h>

#define DATA_DB "vosa"
#define DATA_COLLECTION "mot_results"
#define NAMESPACE "vosa.mot_results"
#define DEFAULT_URI "mongodb://localhost:27017"
#define INSERT_THREADS_PER_SHARD 3
#define BATCHSIZE 1000

int resulttype[14] = { 1, 1, 2, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 2 };
char *resultfields[14] = { "t", "v", "d", "c", "y", "r", "m", "s", "k", "o",
		"u", "f", "d", "i" };

int nshards = 1;

void run_loader(int thread, char *filename);
void configure_sharding();
const char *get_bson_string(const bson_t *obj, char *name);
int get_bson_int(const bson_t *obj, char *name);
char *shardnames[1024]; // If you have more than a thousand shards you probably know to change this

int main(int argc, char **argv) {
	int t;
	int status;
	mongoc_init();

	if (argc == 0) {
		fprintf(stderr, "usage: fastload <filename>");
		exit(1);
	}
	configure_sharding();

	for (t = 0; t < INSERT_THREADS_PER_SHARD * nshards; t++) {
		if (fork() == 0) {
			run_loader(t, argv[1]);
			printf("Inserter thread finished\n");
			exit(0);
		}
	}
	/* Reap children */
	while (wait(&status) != -1) {
	}
	mongoc_cleanup();
}

void configure_sharding() {
	mongoc_client_t *conn;
	mongoc_collection_t *collection;
	mongoc_collection_t *config;
	bson_error_t error;
	bson_t *command;
	bson_t reply;
	char *str;
	const char *process;
	mongoc_cursor_t *cursor;
	const bson_t *doc;
    bson_t *update = NULL;
    bson_t *query = NULL;

	conn = mongoc_client_new(DEFAULT_URI);
	collection = mongoc_client_get_collection(conn, "admin", "test");

	//Work out if we arer talking to a mongod or a mongos
	command = BCON_NEW("serverStatus", BCON_INT32(1));
	if (mongoc_collection_command_simple(collection, command, NULL, &reply,
			&error)) {
		process = get_bson_string(&reply, "process");
		printf("Loader is talking to %s\n", process);
	} else {
		fprintf(stderr, "Failed to run command: %s\n", error.message);
		exit(1);
	}
	bson_destroy(command);
	bson_destroy(&reply);
	mongoc_collection_destroy(collection);
	//Could be mongos or mongos.exe
	if (strncmp(process, "mongos", 6) != 0) {
		mongoc_client_destroy(conn);
		return;
	}

	collection = mongoc_client_get_collection(conn, "config", "shards");

	query = bson_new();
	cursor = mongoc_collection_find(collection, MONGOC_QUERY_NONE, 0, 0, 0,
			query, NULL, NULL);

	int shardcount = 0;
	while (mongoc_cursor_next(cursor, &doc)) {
		shardnames[shardcount] = strdup(get_bson_string(doc, "_id"));
		shardcount++;
	}
	nshards = shardcount;
	bson_destroy(query);
	mongoc_cursor_destroy(cursor);

	//Shard our collection if it isnt already
	collection = mongoc_client_get_collection(conn, "admin", "test");

	//Work out if we arer talking to a mongod or a mongos
	command = BCON_NEW("collMod", BCON_UTF8(NAMESPACE),"noPadding",BCON_BOOL(false));
	if (mongoc_collection_command_simple(collection, command, NULL, &reply,
			&error)) {

	} else {
		fprintf(stderr, "Failed to disable padding on db (is this WiredTiger?): %s\n", error.message);

	}
	bson_destroy(command);
	bson_destroy(&reply);


	//Work out if we arer talking to a mongod or a mongos
	command = BCON_NEW("enableSharding", BCON_UTF8(DATA_DB));
	if (mongoc_collection_command_simple(collection, command, NULL, &reply,
			&error)) {

	} else {
		fprintf(stderr, "Failed to enable sharding on db: %s\n", error.message);

	}
	bson_destroy(command);
	bson_destroy(&reply);

	//Work out if we arer talking to a mongod or a mongos
	command =
			BCON_NEW("shardCollection", BCON_UTF8(NAMESPACE),"key","{","_id",BCON_INT32(1),"}");
	if (mongoc_collection_command_simple(collection, command, NULL, &reply,
			&error)) {

	} else {
		fprintf(stderr, "Failed to shard colleciton: %s\n", error.message);

	}
	bson_destroy(command);
	bson_destroy(&reply);



	mongoc_collection_destroy(collection);

	collection = mongoc_client_get_collection(conn, "config", "collections");

    query = BCON_NEW ("_id", BCON_UTF8 (NAMESPACE));
    update = BCON_NEW ("$set", "{","noBalance", BCON_BOOL(false),"}");

    if (!mongoc_collection_update (collection, MONGOC_UPDATE_NONE, query, update, NULL, &error)) {
        printf ("Failed to turn off autobalancing for collection%s\n", error.message);
    }

	bson_destroy(query);
	bson_destroy(update);

	mongoc_client_destroy(conn);

}

int carve_chunk( mongoc_client_t *conn,   mongoc_collection_t *collection) {
	bson_error_t error;
	bson_t reply;
	unsigned long long seqno = -1;
	char *str;
	mongoc_collection_t *admin;

	bson_t *command;
	//Get a sequence value using find and modify on a record
	if (mongoc_collection_find_and_modify(collection,
	BCON_NEW("_id", BCON_UTF8("sequence")), NULL,
	BCON_NEW ("$inc", "{", "count", BCON_INT32 (1), "}"), NULL, false, true,
			true, &reply, &error)) {

		seqno = get_bson_int(&reply, "value.count");
		printf("Got Sequence number %llu\n", seqno);
	} else {
		fprintf(stderr, "Cannot get sequence %s\n", error.message);
	}


	//Shard our collection if it isnt already
	admin = mongoc_client_get_collection(conn, "admin", "test");

	//Work out if we arer talking to a mongod or a mongos
	int done = 0;
	while (!done) {
		done = 1;
		command =
				BCON_NEW("split", BCON_UTF8(NAMESPACE),"middle","{","_id",BCON_INT64(seqno<<32),"}");
		if (mongoc_collection_command_simple(admin, command, NULL, &reply,
				&error)) {
			fprintf(stderr, "Split chunk: OK\n");
		} else {
			//	fprintf(stderr, "Failed to split chunk (trying again): %s\n",
			//			error.message);
			if (strcmp(error.message, "split failed") == 0) {
				sleep(1);
				done = 0;
			}
		}
		bson_destroy(command);
		bson_destroy(&reply);
	}

	done = 0;
	while (!done) {
		done = 1;
		command =
				BCON_NEW("moveChunk", BCON_UTF8(NAMESPACE),"find","{","_id",BCON_INT64(seqno<<32),"}","to",BCON_UTF8(shardnames[seqno%nshards]));
		if (mongoc_collection_command_simple(admin, command, NULL, &reply,
				&error)) {
			fprintf(stderr, "Move chunk to %llu: OK\n", seqno % nshards);
		} else {
			//	fprintf(stderr, "Failed to move chunk to %llu(trying again): %s\n",
			//			seqno % nshards, error.message);
			if (strcmp(error.message, "move failed") == 0) {
				sleep(1);
				done = 0;
			}
		}
		bson_destroy(command);
		bson_destroy(&reply);
	}

	mongoc_collection_destroy(admin);
	return seqno;
}

//Each child will target a single shard - avoids any context switching
//Actually N clients per shard to allow for processing of the next batch client side
//CSV parsing isn't free after all

void run_loader(int thread, char *filename) {
	bson_t *record;
	mongoc_client_t *conn;
	mongoc_collection_t *collection;
	bson_error_t error;
	bson_t reply;
	int count;
	FILE *infile;
	char *rptr;
	char ilinebuf[BUFSIZ];
	char rlinebuf[BUFSIZ];
	char *ritem;
	char *rlast = NULL;
	int rfcount = 0;
	int batchcount = 0;
	char *str;
	int total = 0;

	//Get the highest used value on that shard so far

	conn = mongoc_client_new(DEFAULT_URI);

	if (!conn) {
		fprintf(stderr, "Failed to parse URI.\n");
		exit(1);
	}

	collection = mongoc_client_get_collection(conn, DATA_DB, DATA_COLLECTION);

	long long chunkno = carve_chunk(conn, collection);

	printf("Thread %d reading %s\n", thread, filename);
	infile = fopen(filename, "r");
	if (infile == NULL) {
		perror("Opening results file");
		exit(1);
	}


	mongoc_bulk_operation_t *bulk = mongoc_collection_create_bulk_operation(
			collection, true, NULL);
	if(!bulk)
	{
		printf("Failed to create bulk op\n");
	}
	rptr = fgets(rlinebuf, BUFSIZ, infile);
	rlinebuf[strlen(rlinebuf) - 1] = '\0';
	//Read the Results Line

	while (rptr) {
		total++;
		if (total % (INSERT_THREADS_PER_SHARD * nshards) == thread) {

			ritem = strtok_r(rptr, "|", &rlast);
			rfcount = 0;

			record = bson_new();

			//Two part ID - a loader (32 bits for that) and a one_up
			bson_append_int64(record, "_id", -1, (chunkno << 32) + total);

			while (ritem) {
				switch (resulttype[rfcount]) {
				case 0:
					//printf("%s\n",ritem);
					bson_append_utf8(record, resultfields[rfcount], -1, ritem,
							-1);
					break;
				case 1:
					bson_append_int32(record, resultfields[rfcount], -1,
							atoi(ritem));
					break;
				case 2:
					if (strncmp(ritem, "NULL", 4)) {
						struct tm tm;
						if (strptime(ritem, "%Y-%m-%d", &tm)) {

							time_t t = mktime(&tm); // t is now your desired time_t
							bson_append_date_time(record, resultfields[rfcount],
									-1, (long long) t * 1000);
						}
					}
					break;

				default:
					printf("Unknown type col %d = %d\n", rfcount,
							resulttype[rfcount]);
				}
				ritem = strtok_r(NULL, "|", &rlast);
				rfcount++;
			}

			mongoc_bulk_operation_insert(bulk, record);
			bson_destroy(record);
			if (batchcount == (BATCHSIZE - 1)) {

				int ret = mongoc_bulk_operation_execute(bulk, &reply, &error);

				if (!ret) {
					printf( "Error: %s\n", error.message);
				}


				if (thread == 0)
					printf("%s %d\n", filename, total);

				bson_destroy(&reply);

				mongoc_bulk_operation_destroy(bulk);
				batchcount = 0;
				bulk = mongoc_collection_create_bulk_operation(collection, true,
						NULL);

			} else {
				batchcount++;

			}
		}
		//Read next line from file
		rptr = fgets(rlinebuf, BUFSIZ, infile);
		rlinebuf[strlen(rlinebuf) - 1] = '\0';
	}
	int ret = mongoc_bulk_operation_execute(bulk, &reply, &error);



	if (!ret) {
		fprintf(stderr, "Error: %s\n", error.message);
	}
	if (thread == 0)
		printf("%s %d\n", filename, total);
	bson_destroy(&reply);
	mongoc_collection_destroy(collection);
	mongoc_bulk_operation_destroy(bulk);
	mongoc_client_destroy(conn);
}

//I know this is really inefficient code but it's clean as an API to code with.
//I could come back and improve the performance later adding bson hashes.

const char *get_bson_string(const bson_t *obj, char *name) {
	bson_iter_t i;
	bson_type_t t;
	unsigned int length;

	if (bson_iter_init(&i, obj)) {
		if (bson_iter_find(&i, name)) {
			if (bson_iter_type(&i) == BSON_TYPE_UTF8) {
				return bson_iter_utf8(&i, &length);
			}
		}
	}

	return NULL;
}

//Casts to an int
int get_bson_int(const bson_t *obj, char *name) {
	bson_iter_t i;
	bson_iter_t t;

	if (bson_iter_init(&i, obj)) {
		if (bson_iter_find_descendant(&i, name, &t)) {
			if (bson_iter_type(&t) == BSON_TYPE_INT32) {
				return bson_iter_int32(&t);
			}
			if (bson_iter_type(&t) == BSON_TYPE_DOUBLE) {
				return (int) bson_iter_double(&t);
			}
		}
	}

	return 0;
}

