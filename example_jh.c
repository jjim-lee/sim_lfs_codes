/* This program is developed from Jinhyuk Lee as Master thesis in Goethe University.
 * This program start from example file in codes-dev git repository. (Location : codes-dev/doc/example/example.c)
 * This program simulates the basic I/O operation refer to Lustre file system. 
 * Final modification : 18/10/2023
 */

#include <string.h>
#include <assert.h>
#include <ross.h>

#include "codes/lp-io.h"
#include "codes/lp-msg.h"
#include "codes/codes.h"
#include "codes/codes_mapping.h"
#include "codes/configuration.h"
#include "codes/model-net.h"
#include "codes/lp-type-lookup.h"
#include "codes/jenkins-hash.h"
#include "codes/quickhash.h"
#include "codes/net/common-net.h"

// hash table size should be a prime number!!
#define HASH_TABLE_SIZE 4999
#define QUEUE_MAX 100

// To distinguish message headers
static int magic_client = 31;
static int magic_mds = 32;
static int magic_mdt = 33;
static int magic_oss = 34;
static int magic_ost = 35;

static int num_reqs = 0;    // number of requests for each client
static uint64_t payload_sz; // size of message 

// default value of stripe size and count. it would be updated with user input. default is as below
// in C, it is impossible to change array size in runtime. Hence I fix array size with MAX_STRIPE_COUNT
#define MAX_STRIPE_COUNT 100
static int stripe_size = 1;
static int stripe_count = 4;

static int net_id = 0;
static int num_client, num_MD_server, num_MD_storage, num_OS_server, num_OS_storage;

// Customized data on configuration file
static char *param_group_nm   = "clt_req";
static char *num_reqs_key     = "num_reqs";
static char *payload_sz_key   = "payload_sz";

typedef struct all_msg all_msg;

typedef struct client_state client_state;
typedef struct mds_state mds_state;
typedef struct mdt_state mdt_state;
typedef struct oss_state oss_state;
typedef struct ost_state ost_state;
typedef struct my_queue my_queue;

// Simulation value to be saved..
int sim_file_name[10] = {1473, 1473, 1473, 1473, 1473, 1473, 1473, 0, 0, 0};
int sim_file_size[10] = {16,   16,   16,    16,    16,   16,   16,   0, 0, 0};
int sim_work_type[10] = {21,   20,   20,   20,   20,   20,   20,   0, 0, 0};

/* types of events that will constitute server activities */
enum all_event
{
    KICKOFF = 1,    /* initial event */
    REQ,            /* request event */
    ACK,            /* ack event */
    LOCAL           // local event ( only for CLT )
};

enum work_type
{
    OPEN = 19,      // File open
    READ,           // File read
    WRITE,          // File write
    WRITE_APPEND,   // File write with append option
    CLOSE           // File close
};

/* This struct serves as the persistent state of each LP (client, MDS, MDT, OSS, OST)
 * This struct is setup when the LP initialization function pointer is called */

struct client_state
{
    tw_lpid id;             // client relative id
    int MD_req_count;       // counting requests to MD(Meta Data)
    int OS_req_count;       // counting requests to OS(Object Storage)
    int req_complete_count; // request completed count
    int open_file;          // File process status value ( 1: open, 0: others )
    int working_fz;         // File size that this client is currently working on. ( it can vary when writing or reading job is done. )
	int total_fz;			// Same as above, but total size remains same.
    int status;             // progress check flag, to cope with rev.. 0 : start, 1 : open file and wait, 2 : open file check finish( only applied to WT and WA), 3 : MDT finished and send data to OST,
    my_queue * fcqueue;     // controls sending files to OSS
    my_queue * cl_fcqueue;  // controls cleanup files to OSS
	my_queue * bk_fcqueue;	// controls backup messages in Local when unexpected happens
	int already_done;		// check if request is already done..
	int bk_status;			// backup status
	int stored_file_name;	// For READ simulation - saving file name
	int read_flag;			// For READ simulation - if writing is finished..
    tw_stime start_ts;      // saves start time
    tw_stime end_ts;        // saves end time
};

struct mds_state
{
    tw_lpid id;                     // server relative id
    int lock;                       // Server lock (MDS)
    int try_again;                  // If fail happens..
    my_queue * fcqueue;             // flow control queue 
    tw_stime start_ts;             // start time
    tw_stime end_ts;               // end time
};

struct oss_state
{
    tw_lpid id;                    // server relative id
    int * locks;                    // Server locks (OSS)
    int * try_again;                // If fail happens.. for REQ
    int * bk_status;				// If fail happens.. for ACK
	my_queue * fcqueue;             // flow control queue
	my_queue * bk_fcqueue;			// backup when local has sending problem..
    tw_stime start_ts;             // start time
    tw_stime end_ts;               // end time
};

struct mdt_state
{
    tw_lpid id;                 // storage relative id
    int last_saved_OST;             // remembers last saved OST ID to calculate next saving place of OST
    struct qhash_table *strg_tbl;   // hash table to store data
    tw_stime start_ts;
    tw_stime end_ts;
};

struct ost_state
{
    tw_lpid id;                 // storage relative id
    struct qhash_table *strg_tbl;   // hash table to store data
    int saved_size;                 // only for OST
    tw_stime start_ts;
    tw_stime end_ts;
};

// HASH related struct & functions - MDT ///////////////////////////////////////
// Hash key : To find right file, I am using 2 Characteristics: file size and file uid
struct jh_mdt_hash_key
{
    int f_name;
};

// node structure of MDT hash table ( or can be entry )
struct mdt_qhash_entry
{
    struct jh_mdt_hash_key key;
    int uid;                                        // unique ID ( This would be assigned from MDT )
    int size;                                       // file size
    int stripe_place[MAX_STRIPE_COUNT];             // OST ID where data is saved
    int stripe_stored_size[MAX_STRIPE_COUNT];       // size of each stripe
    int stripe_count;                               // how many stripes are filled...
    struct qhash_head hash_link;
};

// MDT hash comparison
int mdt_hash_compare(void *key, struct qhash_head *link)
{
    struct jh_mdt_hash_key * message_key = (struct jh_mdt_hash_key *)key;
    struct mdt_qhash_entry *tmp = NULL;

    tmp = qhash_entry(link, struct mdt_qhash_entry, hash_link);

    if ( tmp->key.f_name == message_key->f_name )
        return 1;

    return 0;
}

// MDT hash function - simply mod of number of OS storage (OST)
int mdt_hash_func(void *k, int table_size)
{
    struct jh_mdt_hash_key * tmp = (struct jh_mdt_hash_key *)k;
    int key = (tmp->f_name + 2003) * num_OS_storage;
    return key & (table_size - 1);
}

// HASH related struct & functions - OST ////////////////////////////////////////
struct jh_ost_hash_key
{
    int f_name;
    int uid;
};

// node structure of OST hash table ( or can be entry )
struct ost_qhash_entry
{
    struct jh_ost_hash_key key;
    int stripe_stored_size;
    struct qhash_head hash_link;
};

// OST hash comparison
int ost_hash_compare(void *key, struct qhash_head *link)
{
    struct jh_ost_hash_key * message_key = (struct jh_ost_hash_key *)key;
    struct ost_qhash_entry *tmp = NULL;

    tmp = qhash_entry(link, struct ost_qhash_entry, hash_link);

    if ( (tmp->key.f_name == message_key->f_name) && (tmp->key.uid == message_key->uid) )
        return 1;

    return 0;
}

// OST hash function - simply mod of table_size
int ost_hash_func(void *k, int table_size)
{
    struct jh_ost_hash_key * tmp = (struct jh_ost_hash_key *)k;
    int key = (tmp->f_name + tmp->uid + 2003) * num_OS_storage;
    return key & (table_size - 1);
}

/* This is the structure of message between LPs */
struct fid
{
    int FID_value;              // file id that user wants to work      *****************   file unique id would be random from 30 ~ 1000
    int stripe_count;           // number of stripes for this file..
    int stripe_place[MAX_STRIPE_COUNT];
    int stripe_stored_size[MAX_STRIPE_COUNT];
    int cleanup_count;
    int cleanup_place[MAX_STRIPE_COUNT];
};

struct all_msg
{
    msg_header m_header;        // It includes source id, event type and magic
    int work_type;              // file read(0) or write(1)
    tw_lpid client_id;          // requested client id
    int open_file;              // is it open file req? 0: no, 1: yes
	int come_from_MD;			// check if it comes from MD ( client only )
    int file_name;              // file name : because it is simulation, I will use numbers instead of characters from 1001 ~ 2000
    int file_size;              // file size that user wants to work   
    struct fid fid_values;      // This value would be FID(Meta data of file)
    int rff[2];                 // Real file fragments : This value would be real data. rff[0] = OST ID to be saved, rff[1] = file fragment size to be saved
    int tmp_array_num;			// For OSS
	model_net_event_return ret; /* model net reverse computation var */
};

///////// Circular queue to be added... ////////////////////////////////////////////

struct my_queue
{
    struct all_msg queue[QUEUE_MAX];
    int front;
    int rear;
    int size;
};

struct my_queue * mq_init(void) {
    struct my_queue * tq = (struct my_queue *) calloc(1, sizeof(struct my_queue));
    tq->front = 0;
    tq->rear = 0;
    tq->size = 0;
}

int is_empty(struct my_queue * tq) {
    if (tq->front == tq->rear) {
       // printf("queue is empty\n");
        return 1;
    }
    else {
    //    printf("queue is not empty\n");
        return 0;
    }
}

int is_full(struct my_queue * tq) {
    if ( (tq->rear + 1) % QUEUE_MAX == tq->front ) {
       // printf("queue is full\n");
        return 1;
    }
    else {
    //    printf("queue is NOT full\n");
        return 0;
    }
}

void mq_enqueue(struct my_queue * tq, struct all_msg * in_data) {
    if (is_full(tq))
        return;
    tq->rear = (tq->rear +1)% QUEUE_MAX;
    memcpy(&tq->queue[tq->rear], in_data, sizeof(struct all_msg));
    tq->size++;
}

struct all_msg * mq_dequeue(struct my_queue * tq)
{
    struct all_msg * dq = malloc(sizeof(struct all_msg));
    if (is_empty(tq))
        return dq;
    tq->front = (tq->front + 1) % QUEUE_MAX;
    memcpy(dq, &tq->queue[tq->front], sizeof(struct all_msg));
    tq->size--;
    return dq;
}

int mq_get_size(struct my_queue * tq) {
    return tq->size;
}

void mq_end(struct my_queue * tq) {
    free(tq);
}

// Additional useful functions
int calc_current_ost(int cur_ost) {
    int output;
    // Hand-made Round-robin - OST selection
    if (cur_ost == num_OS_storage - 1)
        output = 0;
    else {
        output = cur_ost + (num_OS_storage / num_OS_server);
        if (output >= num_OS_storage)
            output = (output + 1) % num_OS_storage;
    }
    return output;
}

const char * i_to_s_wt(int input) {
    switch (input) {
        case READ:
            return "RD";
            break;
        case WRITE:
            return "WT";
            break;
        case WRITE_APPEND:
            return "WA";
            break;
        default:
            return "ER";
            break;
    }
}

/* ROSS expects four functions per LP:
 * - an LP initialization function, called for each LP
 * - an event processing function
 * - a *reverse* event processing function (rollback), and
 * - a finalization/cleanup function when the simulation ends
 */

/* 1. Client */
static void client_init(client_state * cs, tw_lp * lp);
static void client_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void client_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void client_finalize(client_state * cs, tw_lp * lp);

/* 2. MDS (Meta Data Server) */
static void mds_init(mds_state * mdss, tw_lp * lp);
static void mds_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void mds_rev_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void mds_finalize(mds_state * mdss, tw_lp * lp);

/* 3. OSS (Object Storage Server) */
static void oss_init(oss_state * osss, tw_lp * lp);
static void oss_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void oss_rev_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void oss_finalize(oss_state * osss, tw_lp * lp);

/* 4. MDT (Meta Data Target) */
static void mdt_init(mdt_state * mdts, tw_lp * lp);
static void mdt_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp);
static void mdt_rev_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp);
static void mdt_finalize(mdt_state * mdts, tw_lp * lp);

/* 5. OST (Object Storage Target) */
static void ost_init(ost_state * osts, tw_lp * lp);
static void ost_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp);
static void ost_rev_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp);
static void ost_finalize(ost_state * osts, tw_lp * lp);

/* set up the function pointers for ROSS, as well as the size of the LP state
 * structure (NOTE: ROSS is in charge of event and state (de-)allocation) */
tw_lptype client_lp = {
    (init_f) client_init,
    (pre_run_f) NULL,
    (event_f) client_event,
    (revent_f) client_rev_event,
    (commit_f) NULL,
    (final_f)  client_finalize,
    (map_f) codes_mapping,
    sizeof(client_state),
};

tw_lptype mds_lp = {
    (init_f) mds_init,
    (pre_run_f) NULL,
    (event_f) mds_event,
    (revent_f) mds_rev_event,
    (commit_f) NULL,
    (final_f)  mds_finalize,
    (map_f) codes_mapping,
    sizeof(mds_state),
};

tw_lptype oss_lp = {
    (init_f) oss_init,
    (pre_run_f) NULL,
    (event_f) oss_event,
    (revent_f) oss_rev_event,
    (commit_f) NULL,
    (final_f)  oss_finalize,
    (map_f) codes_mapping,
    sizeof(oss_state),
};

tw_lptype mdt_lp = {
    (init_f) mdt_init,
    (pre_run_f) NULL,
    (event_f) mdt_event,
    (revent_f) mdt_rev_event,
    (commit_f) NULL,
    (final_f)  mdt_finalize,
    (map_f) codes_mapping,
    sizeof(mdt_state),
};

tw_lptype ost_lp = {
    (init_f) ost_init,
    (pre_run_f) NULL,
    (event_f) ost_event,
    (revent_f) ost_rev_event,
    (commit_f) NULL,
    (final_f)  ost_finalize,
    (map_f) codes_mapping,
    sizeof(ost_state),
};

extern const tw_lptype* client_get_lp_type();
static void client_add_lp_type();
extern const tw_lptype* mds_get_lp_type();
static void mds_add_lp_type();
extern const tw_lptype* oss_get_lp_type();
static void oss_add_lp_type();
extern const tw_lptype* mdt_get_lp_type();
static void mdt_add_lp_type();
extern const tw_lptype* ost_get_lp_type();
static void ost_add_lp_type();
static tw_stime s_to_ns(tw_stime ns);

/* as we only have a single event processing entry point and multiple event
 * types, for clarity we define "handlers" for each (reverse) event type */
static void handle_client_kickoff_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_client_kickoff_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_client_ack_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_client_ack_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_client_local_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_client_local_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp);

static void handle_mds_req_event      (mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_mds_req_rev_event  (mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_mds_ack_event      (mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_mds_ack_rev_event  (mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_mds_local_event    (mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_mds_local_rev_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp);

static void handle_oss_req_event      (oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_oss_req_rev_event  (oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_oss_ack_event      (oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_oss_ack_rev_event  (oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_oss_local_event    (oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_oss_local_rev_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp);

static void handle_mdt_req_event    (mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_mdt_req_rev_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp);

static void handle_ost_req_event    (ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp);
static void handle_ost_req_rev_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp);

/* arguments to be handled by ROSS - strings passed in are expected to be
 * pre-allocated */
static char conf_file_name[256] = {0};
/* this struct contains default parameters used by ROSS, as well as
 * user-specific arguments to be handled by the ROSS config sys. Pass it in
 * prior to calling tw_init */
const tw_optdef app_opt [] =
{
    TWOPT_GROUP("Model net test case" ),
    TWOPT_CHAR("codes-config", conf_file_name, "name of codes configuration file"),
    TWOPT_UINT("stripe-size", stripe_size, "data size of each stripe chunks"),
    TWOPT_UINT("stripe-count", stripe_count, "number of stripe chunks to divide original file"),
    TWOPT_END()
};

int main(int argc, char **argv)
{
    int nprocs;
    int rank;
    int num_nets, *net_ids;

    printf("Simulation start by jjim.lee\n");

    g_tw_ts_end = s_to_ns(60*60*24*365); /* one year, in nsecs */

    /* ROSS initialization function calls */
    tw_opt_add(app_opt); /* add user-defined args */
    /* initialize ROSS and parse args. NOTE: tw_init calls MPI_Init */
    tw_init(&argc, &argv);

    if (!conf_file_name[0])
    {
        fprintf(stderr, "Expected \"codes-config\" option, please see --help.\n");
        MPI_Finalize();
        return 1;
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    /* loading the config file into the codes-mapping utility, giving us the
     * parsed config object in return.
     * "config" is a global var defined by codes-mapping */
    if (configuration_load(conf_file_name, MPI_COMM_WORLD, &config)){
        fprintf(stderr, "Error loading config file %s.\n", conf_file_name);
        MPI_Finalize();
        return 1;
    }

    /* register model-net LPs with ROSS */
    model_net_register();

    /* register each LP type with ROSS */
    client_add_lp_type();
    mds_add_lp_type();
    oss_add_lp_type();
	mdt_add_lp_type();
    ost_add_lp_type();

	/* Setup takes the global config object, the registered LPs, and
     * generates/places the LPs as specified in the configuration file.
     * This should only be called after ALL LP types have been registered in
     * codes */
    codes_mapping_setup();

    /* Setup the model-net parameters specified in the global config object,
     * returned are the identifier(s) for the network type. In this example, we
     * only expect one*/
    net_ids = model_net_configure(&num_nets);
    assert(num_nets==1);
    net_id = *net_ids;
    free(net_ids);
    /* in this example, we are using simplenet, which simulates point to point
     * communication between any two entities (other networks are trickier to
     * setup). Hence: */
    if(net_id != SIMPLENET)
    {
        printf("\n The test works with simple-net configuration only! ");
        MPI_Finalize();
        return 0;
    }

    /* calculate the number of servers in this simulation,
     * ignoring annotations */
    num_client     = codes_mapping_get_lp_count("client",     0, "client", NULL, 1);
    num_MD_server  = codes_mapping_get_lp_count("MD_server",  0, "mds", NULL, 1);
    num_MD_storage = codes_mapping_get_lp_count("MD_storage", 0, "mdt", NULL, 1);
    num_OS_server  = codes_mapping_get_lp_count("OS_server",  0, "oss", NULL, 1);
    num_OS_storage = codes_mapping_get_lp_count("OS_storage", 0, "ost", NULL, 1);

    /* The custom configuration variables are read from here */
    configuration_get_value_int(&config, param_group_nm, num_reqs_key, NULL, &num_reqs);
    configuration_get_value_int(&config, param_group_nm, payload_sz_key, NULL, (int *)&payload_sz);

    /* begin simulation */
    tw_run();

    /* model-net has the capability of outputting network transmission stats */
    model_net_report_stats(net_id);

    tw_end();
    return 0;
}

/////////////////////////////////////////////////
/* defined functions in front of main function */
/////////////////////////////////////////////////

const tw_lptype* client_get_lp_type()
{
        return(&client_lp);
}
static void client_add_lp_type()
{
    /* lp_type_register should be called exactly once per process per
     * LP type */
    lp_type_register("client", client_get_lp_type());
}

const tw_lptype* mds_get_lp_type()
{
        return(&mds_lp);
}
static void mds_add_lp_type()
{
    lp_type_register("mds", mds_get_lp_type());
}

const tw_lptype* oss_get_lp_type()
{
        return(&oss_lp);
}
static void oss_add_lp_type()
{
    lp_type_register("oss", oss_get_lp_type());
}

const tw_lptype* mdt_get_lp_type()
{
        return(&mdt_lp);
}
static void mdt_add_lp_type()
{
    lp_type_register("mdt", mdt_get_lp_type());
}

const tw_lptype* ost_get_lp_type()
{
        return(&ost_lp);
}
static void ost_add_lp_type()
{
    lp_type_register("ost", ost_get_lp_type());
}

//////////////////////////////////////////////////////
//                  1. Client                       //
//////////////////////////////////////////////////////

static void client_init(client_state * cs, tw_lp * lp)
{
    tw_event *e;
    all_msg *m;
    tw_stime kickoff_time;

    // Initialize all
    memset(cs, 0, sizeof(*cs));
    cs->id = codes_mapping_get_lp_relative_id(lp->gid,1,0); // get client id within client group
    cs->MD_req_count = 0;
    cs->OS_req_count = 0;
    cs->req_complete_count = 0;
    cs->open_file = 0;
    cs->working_fz = 0;
	cs->total_fz = 0;
    cs->status = 0;
    cs->fcqueue = mq_init();
    cs->cl_fcqueue = mq_init();
	cs->bk_fcqueue = mq_init();
	cs->already_done = 0;
	cs->bk_status = 0;
	cs->stored_file_name = 0;
	cs->read_flag = 0;

    /* skew each kickoff event slightly to help avoid event ties later on */
    kickoff_time = g_tw_lookahead + (tw_rand_unif(lp->rng) * .0001);

    /* first create the event (time arg is an offset, not absolute time) */
    e = tw_event_new(lp->gid, kickoff_time, lp);
    /* after event is created, grab the allocated message and set msg-specific
     * data */
    m = tw_event_data(e);
    msg_set_header(magic_client, KICKOFF, lp->gid, &m->m_header);
    /* event is ready to be processed, send it off */
    tw_event_send(e);

    return;
}

/* event processing entry point
 * - simply forward the message to the appropriate handler */
static void client_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
   switch (m->m_header.event_type)
    {
        case REQ:
            printf("\n In client there is no REQ sign coming in..");
            assert(0);
            break;
        case ACK:
            handle_client_ack_event(cs, b, m, lp);
            break;
        case KICKOFF:
            handle_client_kickoff_event(cs, b, m, lp);
            break;
        case LOCAL:
            handle_client_local_event(cs, b, m, lp);
            break;
        default:
            printf("\n Invalid message type %d ", m->m_header.event_type);
            assert(0);
        break;
    }
}

/* reverse event processing entry point
 * - simply forward the message to the appropriate handler */
static void client_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            printf("[UNEXPECTED] CLT REV EVENT : REQ - should NOT be happened\n");
            assert(0);
            break;
        case ACK:
            printf("[UNEXPECTED] CLT %ld REV EVENT : ACK - handle_client_ack_rev_event\n", cs->id);
            handle_client_ack_rev_event(cs, b, m, lp);
            break;
        case KICKOFF:
            printf("[UNEXPECTED] CLT %ld REV EVENT : KICKOFF - handle_client_kickoff_rev_event\n", cs->id);
            handle_client_kickoff_rev_event(cs, b, m, lp);
            break;
        case LOCAL:
            printf("[UNEXPECTED] CLT %ld REV EVENT : LOCAL - handle_client_local_rev_event\n", cs->id);
            handle_client_local_rev_event(cs, b, m, lp);
            break;
        default:
            printf("[UNEXPECTED] CLT %ld REV EVENT : UNKNOWN %d - should NOT be happened\n", cs->id, m->m_header.event_type);
            assert(0);
            break;
    }

    return;
}

/* once the simulation is over, do some output */
static void client_finalize(client_state * cs, tw_lp * lp)
{
    printf("CLT %ld : [FIN] REQ message to MD %d sent, REQ message to OS %d sent, Completed requests : %d\n",
            cs->id, cs->MD_req_count, cs->OS_req_count, cs->req_complete_count);
    return;
}

/* convert seconds to ns */
static tw_stime s_to_ns(tw_stime ns)
{
    return(ns * (1000.0 * 1000.0 * 1000.0));
}

/* handle initial event - it will prepare for file name, file size and work type. And it will send this message to MDS */
static void handle_client_kickoff_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    int dest_MD_server_relative_id;  
    tw_lpid dest_id;
    all_msg start_m;

    if (b->c0) {
        //printf("CLT %ld : [LOCKED]\n", cs->id);
        return;
    }

    // record time
    cs->start_ts = tw_now(lp);

    // enter all message elements
    msg_set_header(magic_mds, REQ, lp->gid, &start_m.m_header);

	/* Simulation method 1. First write and from next read only */
	start_m.file_size = 20;

	if (cs->read_flag) {	
		// SC 2, READ TEST
		start_m.file_name = cs->stored_file_name;
		start_m.work_type = READ;
	}
	else {
    	// SC 1, WRITE TEST
    	start_m.file_name = tw_rand_integer(lp->rng, 1001, 2000);
		start_m.work_type = WRITE;
		cs->stored_file_name = start_m.file_name;
	//	cs->read_flag = 1; // To run only write....
	}
	
	/* Simulation method 2. File name, size, work type will be followed with 3 arrays 
	 * - With this method, user can test with their own testing sequence */
/*
    // For customized simulation..
    start_m.file_name = sim_file_name[cs->req_complete_count];
    start_m.file_size = sim_file_size[cs->req_complete_count];
    start_m.work_type = sim_work_type[cs->req_complete_count];
*/
    // For Debug
    //printf("CLT %ld : [%s] start!\n", cs->id, i_to_s_wt(start_m.work_type));

    // Save the status
    cs->open_file = 1;
    cs->working_fz = start_m.file_size;
	cs->total_fz = start_m.file_size;
    cs->status = 1;
	cs->already_done = 0;

	start_m.client_id = cs->id;     // Send client RELATIVE ID
    start_m.open_file = 1;          // Initial request for working should be open file!
    start_m.come_from_MD = 0;
    start_m.fid_values.FID_value = 0;
    start_m.fid_values.cleanup_count = 0;
	start_m.tmp_array_num = 0;

    // select one of MD server
    dest_MD_server_relative_id = tw_rand_integer(lp->rng, 0, num_MD_server - 1);
    dest_MD_server_relative_id = dest_MD_server_relative_id % num_MD_server;
    dest_id = codes_mapping_get_lpid_from_relative(dest_MD_server_relative_id, "MD_server", "mds", NULL, 0);

    m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(start_m), &start_m, 0, NULL, lp);
    cs->MD_req_count++;

    // Very important! Client lock until work is finished..
    b->c0 = 1;

    // for debug
    printf("CLT %ld : [%s, REQ] -> MDS %d, saved CLT ID : %ld\n", cs->id, i_to_s_wt(start_m.work_type), dest_MD_server_relative_id, start_m.client_id);
}

// handle receiving ack
static void handle_client_ack_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    int dest_OS_server_relative_id, dest_MD_server_relative_id;
    tw_lpid dest_id;
    all_msg os_m;
    all_msg * local_m;

    if (m->come_from_MD == 1) // It comes from MD
    {
        if (m->open_file == 1) {
            if (m->work_type == READ) {
                if (m->fid_values.FID_value == 0) {
                    cs->open_file = 0;
                    cs->req_complete_count++;
                    b->c0 = 0;
                    cs->status = 0;
                    cs->end_ts = tw_now(lp);
                    printf("\nCLT %ld : [COMPLETED %d] - File name %d does NOT exist, time : %.3f ns\n\n",
                            cs->id, cs->req_complete_count, m->file_name, cs->end_ts - cs->start_ts);
                    if (cs->req_complete_count < num_reqs) {
                        handle_client_kickoff_event(cs, b, m, lp);
                    }
                }
                else {
                    cs->end_ts =tw_now(lp);     // In this case, it is middle time save point.
                    // Time checker
                    printf("\n[Time checker] CLT %ld : [%s] - round trip to MDT, time : %.3f ns, message size : %ld Bytes, Bandwidth : %.3f Gbit/s\n\n", cs->id, i_to_s_wt(m->work_type), cs->end_ts - cs->start_ts, sizeof(struct all_msg), 8*sizeof(struct all_msg)/(cs->end_ts - cs->start_ts));

                    cs->status = 3;
                    os_m = *m; // copy all contents first

                    // message for OS creation
                    msg_set_header(magic_oss, REQ, lp->gid, &os_m.m_header);
                    os_m.open_file = 0;
                    os_m.come_from_MD = 0;

                    for ( int i=0; i < m->fid_values.stripe_count; i++ ) {
                        // save id and file size to rff array
                        os_m.rff[0] = m->fid_values.stripe_place[i];
                        os_m.rff[1] = m->fid_values.stripe_stored_size[i];

						mq_enqueue(cs->fcqueue, &os_m);
                    }

					// send message to LOCAL -> to send messages one by one ..
					tw_event *e;
            		tw_stime cur_time = tw_now(lp);
            		e = tw_event_new(lp->gid, cur_time, lp);
            		local_m = tw_event_data(e);
            		msg_set_header(magic_client, LOCAL, lp->gid, &local_m->m_header);
            		tw_event_send(e);
                    cs->OS_req_count++;
                }
            }
            else if (m->work_type == WRITE || m->work_type == WRITE_APPEND) { // Send back to MDS!!!
                cs->status = 2;
                os_m = *m;

                msg_set_header(magic_mds, REQ, lp->gid, &os_m.m_header);
                os_m.open_file = 0;
                os_m.come_from_MD = 0;

                dest_id = m->m_header.src;
                dest_MD_server_relative_id = codes_mapping_get_lp_relative_id(dest_id, 0, 0);

                /* send another request */
                m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(os_m), &os_m, 0, NULL, lp);
                // for debug
                printf("CLT %ld : [%s, REQ] -> MDS %d, Modify meta data with FID %d\n",
                       cs->id, i_to_s_wt(os_m.work_type), dest_MD_server_relative_id, os_m.fid_values.FID_value);
            }
            else {
                printf("CLT %ld : [ERROR] - Wrong work type = %d\n", cs->id, m->work_type);
                return;
            }
        }
        else { // open_file is not 1 - this means that it is case of WRITE and WRITE_APPEND, and it comes with FID and real values.. so need to send it to OSS.
            cs->end_ts =tw_now(lp);     // In this case, it is middle time save point. 

            // Time checker
            printf("\n[Time checker] CLT %ld : [%s] - round trip to MDT, time : %.5f ns, message size : %ld Bytes, Bandwidth : %.3f Gbit/s\n\n", cs->id, i_to_s_wt(m->work_type), cs->end_ts - cs->start_ts, sizeof(struct all_msg), 8*sizeof(struct all_msg)/(cs->end_ts - cs->start_ts));
            cs->status = 3;

            os_m = *m; // copy all contents first

            // message for OS creation
            msg_set_header(magic_oss, REQ, lp->gid, &os_m.m_header);
            os_m.open_file = 0;
            os_m.come_from_MD = 0;

            if (m->work_type == WRITE_APPEND) {
                cs->working_fz = m->file_size;
            }

            for ( int i=0; i < m->fid_values.stripe_count; i++ ) {
                // save id and file size to rff array
                os_m.rff[0] = m->fid_values.stripe_place[i];
                os_m.rff[1] = m->fid_values.stripe_stored_size[i];

                // To distinguish from cleanup, delete cleanup value
                os_m.fid_values.cleanup_count = 0;
                //printf("CLT %ld : check values - src : %ld, type : %d, to OST %d\n", cs->id, os_m.m_header.src, os_m.m_header.event_type, os_m.rff[0]); // for debug
                mq_enqueue(cs->fcqueue, &os_m);
            }

            if (m->fid_values.cleanup_count != 0) { // if we need to clean up..
                os_m.fid_values.cleanup_count = m->fid_values.cleanup_count;    // Recover this value..
                for ( int i=0; i < m->fid_values.cleanup_count; i++ ) {
                    // save id to rff array
                    os_m.rff[0] = m->fid_values.cleanup_place[i];

                    mq_enqueue(cs->cl_fcqueue, &os_m);
                }
            }
            tw_event *e;
            tw_stime cur_time = tw_now(lp);
            e = tw_event_new(lp->gid, cur_time, lp);
            local_m = tw_event_data(e);
            msg_set_header(magic_client, LOCAL, lp->gid, &local_m->m_header);
            tw_event_send(e);
            cs->OS_req_count++;
        }
    }
    else // It comes from OS
    {
        int received_file_size = 0;
        if (m->work_type == READ) {
            printf("CLT %ld : [%s] - Received file fragment is %d MB, from OST %d\n", cs->id, i_to_s_wt(m->work_type), m->file_size, m->rff[0]);
            received_file_size = m->file_size;
        }
        else if (m->work_type == WRITE || m->work_type == WRITE_APPEND) {
            if (m->fid_values.cleanup_count == 0) {
                printf("CLT %ld : [%s] - Received file fragment is %d MB, from OST %d\n", cs->id, i_to_s_wt(m->work_type), m->file_size, m->rff[0]);
                received_file_size = m->file_size;
            }
            else {
                printf("CLT %ld : [%s] - Received CLEANUP complete\n",cs->id, i_to_s_wt(m->work_type));
                received_file_size = 0;
            }
        }
        else {
            printf("CLT %ld : [ERROR] - Wrong work type = %d\n", cs->id, m->work_type);
            return;
        }
        cs->working_fz = cs->working_fz - received_file_size;
        printf("CLT %ld : [%s] - %d MB left...\n", cs->id, i_to_s_wt(m->work_type), cs->working_fz); // for debug

        if (cs->working_fz == 0) {
            tw_stime final_ts = tw_now(lp);
			cs->already_done = 1;

            // Time checker
            printf("\n[Time checker] CLT %ld : [%s] - round trip to OST, time : %.3f ns, file size : %d MB, Bandwidth : %.3f Gbit/s\n", cs->id, i_to_s_wt(m->work_type), final_ts - cs->end_ts, cs->total_fz, 8 * ((cs->total_fz * 1024 * 1024) + sizeof(struct all_msg))/(final_ts - cs->end_ts));

            cs->req_complete_count++;

            // for debug
            printf("\nCLT %ld : [Completed : %d], time : %.3f ns, file size : %d MB, Bandwidth : %.3f Gbit/s\n\n", cs->id, cs->req_complete_count, final_ts - cs->start_ts, cs->total_fz, 8*(cs->total_fz*1024*1024+sizeof(struct all_msg))/(final_ts - cs->start_ts));
            b->c0 = 0;
            cs->status = 0;
            if(cs->req_complete_count < num_reqs)
            {
                handle_client_kickoff_event(cs, b, m, lp);
            }
        }
   	}
    return;
}

static void handle_client_local_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    int dest_OS_server_relative_id;
    tw_lpid dest_id;
    all_msg os_m, local_m;

    // Set up local msg
    msg_set_header(magic_client, LOCAL, lp->gid, &local_m.m_header);

	if (cs->bk_status == 1) { // If reverse handling occured...
		cs->bk_status = 0;

		if (!is_empty(cs->bk_fcqueue)) {
			os_m = *mq_dequeue(cs->bk_fcqueue);

			mq_enqueue(cs->bk_fcqueue, &os_m); // save the backup
				
        	dest_OS_server_relative_id = os_m.rff[0] / (num_OS_storage / num_OS_server);
        	dest_id = codes_mapping_get_lpid_from_relative(dest_OS_server_relative_id, "OS_server", "oss", NULL, 0);

			if (os_m.fid_values.cleanup_count == 0) {
				if (os_m.work_type == READ) {
                	m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(os_m), &os_m, 0, NULL, lp);
                	printf("CLT %ld : [%s, REQ] -> OSS %d, [REV] Find file with FID %d\n", cs->id, i_to_s_wt(os_m.work_type), dest_OS_server_relative_id, os_m.fid_values.FID_value);
            	}
            	else {
                	m->ret = model_net_event(net_id, "req", dest_id, payload_sz + (os_m.rff[1] * 1024 * 1024), 0.0, sizeof(os_m), &os_m, sizeof(local_m), &local_m, lp);
                	//printf("CLT %ld : [%s, REQ] -> OSS %d, [REV] Write file with FID %d, to OST %d, File size : %d MB, Queue left = %d\n",
                    //  cs->id, i_to_s_wt(os_m.work_type), dest_OS_server_relative_id, os_m.fid_values.FID_value, os_m.rff[0], os_m.rff[1], mq_get_size(cs->fcqueue));
            	}
			}
			else {
				m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(os_m), &os_m, sizeof(local_m), &local_m, lp);
            	// for debug
            	printf("CLT %ld : [%s, REQ] -> OSS %d, [REV] Clean up, Clean Queue left = %d\n",
                        cs->id, i_to_s_wt(os_m.work_type), dest_OS_server_relative_id, mq_get_size(cs->cl_fcqueue));
			}
		}
		else
			return;

	}
	else {
		if (!is_empty(cs->bk_fcqueue)) 
			mq_dequeue(cs->bk_fcqueue); // empty backup queue... 
										
    	if (!is_empty(cs->fcqueue)) {
        	os_m = *mq_dequeue(cs->fcqueue);
			mq_enqueue(cs->bk_fcqueue, &os_m); // save the backup

        	// select one of OS server
        	dest_OS_server_relative_id = os_m.rff[0] / (num_OS_storage / num_OS_server);
        	dest_id = codes_mapping_get_lpid_from_relative(dest_OS_server_relative_id, "OS_server", "oss", NULL, 0);

        	//printf("CLT %ld : check values - src : %ld, type : %d, to OST %d, dest_OS_server_relative_id : %d, dest_id : %ld\n", cs->id, os_m.m_header.src, os_m.m_header.event_type, os_m.rff[0], dest_OS_server_relative_id, dest_id); // for debug
		
			if (os_m.work_type == READ) {
				m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(os_m), &os_m, sizeof(local_m), &local_m, lp);
            	printf("CLT %ld : [%s, REQ] -> OSS %d, Find file with FID %d, to OST %d, File size : %d MB, Queue left = %d\n", 
					  cs->id, i_to_s_wt(os_m.work_type), dest_OS_server_relative_id, os_m.fid_values.FID_value, os_m.rff[0], os_m.rff[1], mq_get_size(cs->fcqueue));
			}
			else {
        		m->ret = model_net_event(net_id, "req", dest_id, payload_sz + (os_m.rff[1] * 1024 * 1024), 0.0, sizeof(os_m), &os_m, sizeof(local_m), &local_m, lp);
        		printf("CLT %ld : [%s, REQ] -> OSS %d, Write file with FID %d, to OST %d, File size : %d MB, Queue left = %d\n",
                      cs->id, i_to_s_wt(os_m.work_type), dest_OS_server_relative_id, os_m.fid_values.FID_value, os_m.rff[0], os_m.rff[1], mq_get_size(cs->fcqueue));
			}
    	}
    	else if ( (is_empty(cs->fcqueue)) && (!is_empty(cs->cl_fcqueue)) ) {
        	printf("not EMPTY!!!!\n");
        	os_m = *mq_dequeue(cs->cl_fcqueue);
			mq_enqueue(cs->bk_fcqueue, &os_m); // save the backup

        	// select one of OS server
        	dest_OS_server_relative_id = os_m.rff[0] / (num_OS_storage / num_OS_server);
        	dest_id = codes_mapping_get_lpid_from_relative(dest_OS_server_relative_id, "OS_server", "oss", NULL, 0);

        	m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(os_m), &os_m, sizeof(local_m), &local_m, lp);
        	// for debug
        	printf("CLT %ld : [%s, REQ] -> OSS %d, Clean up, Clean Queue left = %d\n",
                        cs->id, i_to_s_wt(os_m.work_type), dest_OS_server_relative_id, mq_get_size(cs->cl_fcqueue));
    	}
	}
	//printf("LOCAL CLT %ld : check - fcqueue left = %d, cl_fcqueue = %d, bk_fcqueue left = %d\n", cs->id, mq_get_size(cs->fcqueue), mq_get_size(cs->cl_fcqueue), mq_get_size(cs->bk_fcqueue));
    return;
}

/* reverse handler for kickoff */
static void handle_client_kickoff_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    tw_rand_reverse_unif(lp->rng);  // reverse the rng call for getting file size
    tw_rand_reverse_unif(lp->rng);  // reverse the rng call for getting file uid
    b->c0 = 0;                      // rollback lock
    cs->MD_req_count--;
    model_net_event_rc2(lp, &m->ret);

    return;
}

/* reverse handler for ack*/
static void handle_client_ack_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    if (m->come_from_MD == 1) {
        if (m->open_file == 1) {
            if (m->work_type == READ) {
                if (m->fid_values.FID_value == 0) {
                    cs->open_file = 1;
                    cs->req_complete_count--;
                    b->c0 = 1;
                }
                else {
                    cs->OS_req_count--;
                }
            }
            else {
            }
        }
        else { // open_file is not 1 - this means that it is case of WRITE and WRITE_APPEND, and it comes with FID and real values..
            cs->OS_req_count--;
        }
    }
    else // It comes from OS
    {
		if (cs->already_done) {
			printf("Finished!!!!\n");
			return;
		}
        if (cs->status != 3) {
            printf("CLT %ld : [REV] - already done!\n", cs->id);
            return;
        }

        int recover_file_size = 0;
        if (m->work_type == READ) {
            recover_file_size = m->file_size;
        }
        else if (m->work_type == WRITE || m->work_type == WRITE_APPEND) {
            if (m->fid_values.cleanup_count == 0) {
                recover_file_size = m->file_size;
            }
            else {
                recover_file_size = 0;
            }
        }
        else {
        }
        if ((cs->working_fz == 0) && (cs->req_complete_count < num_reqs)) {
            cs->req_complete_count--;
        }
        cs->working_fz = cs->working_fz + recover_file_size;
    }

    model_net_event_rc2(lp, &m->ret);
    return;
}

static void handle_client_local_rev_event(client_state * cs, tw_bf * b, all_msg * m, tw_lp * lp)
{
    printf("LOCAL REV CLT %ld : fcqueue left = %d, bk_fcqueue left = %d\n", cs->id, mq_get_size(cs->fcqueue), mq_get_size(cs->bk_fcqueue));
	if ( mq_get_size(cs->fcqueue) || mq_get_size(cs->bk_fcqueue) ) {
		printf("CLT - DO IT AGAIN!\n");
		cs->bk_status = 1;
	}
	model_net_event_rc2(lp, &m->ret);
    return;
}

//////////////////////////////////////////////////////
//                  2. MDS                          //
//////////////////////////////////////////////////////

static void mds_init(mds_state * mdss, tw_lp * lp)
{
    // Initialize all
    mdss->id = codes_mapping_get_lp_relative_id(lp->gid, 1, 0);
    mdss->fcqueue = mq_init();

    return;
}

static void mds_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
   switch (m->m_header.event_type)
    {
        case REQ:
            handle_mds_req_event(mdss, b, m, lp);
            break;
        case ACK:
            handle_mds_ack_event(mdss, b, m, lp);
            break;
		case LOCAL:
			handle_mds_local_event(mdss, b, m, lp);
			break;
        case KICKOFF:
            printf("\n In MDS there is no KICKOFF sign coming in..");
            assert(0);
            break;
        default:
            printf("\n Invalid message type %d ", m->m_header.event_type);
            assert(0);
        break;
    }
}

static void mds_rev_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            printf("[UNEXPECTED] MDS %ld REV EVENT : REQ - handle_mds_req_rev_event\n", mdss->id);
            handle_mds_req_rev_event(mdss, b, m, lp);
            break;
        case ACK:
            printf("[UNEXPECTED] MDS %ld REV EVENT : ACK - handle_mds_ack_rev_event\n", mdss->id);
            handle_mds_ack_rev_event(mdss, b, m, lp);
            break;
		case LOCAL:
			printf("[UNEXPECTED] MDS %ld REV EVENT : LOCAL - handle_mds_local_rev_event\n", mdss->id);
            handle_mds_local_rev_event(mdss, b, m, lp);
            break;
        case KICKOFF:
            printf("[UNEXPECTED] MDS REV EVENT : KICKOFF - should NOT be happened\n");
            assert(0);
            break;
        default:
            printf("[UNEXPECTED] MDS REV EVENT : UNKNOWN - should NOT be happened\n");
            assert(0);
            break;
    }

    return;
}

/* once the simulation is over, just show comment.. */
static void mds_finalize(mds_state * mdss, tw_lp * lp)
{
    printf("MDS %ld (Global ID : %ld) done\n", mdss->id, lp->gid);
//  mq_end();
    return;
}

static void handle_mds_req_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    int mod;
    int try_again = 0;
    const char * dest_group;
    tw_lpid dest_id;
    all_msg m_to_strg;
    all_msg * dequeue_m;

	mod = num_MD_storage;
    dest_group = "MD_storage";

    // if the message comes from rev... ( unexpected.. )
    if (mdss->try_again) {
        mdss->try_again = 0;
        try_again = 1;
    }

    mq_enqueue(mdss->fcqueue, m);              // enqueue the event first
    //printf("after enqueue : current queue size is %d\n", mq_get_size(mdss->fcqueue));                // for debug

    if (mdss->lock) {
    	//printf("MDS %ld : [LOCKED] - current queue size : %d\n", mdss->id, mq_get_size(mdss->fcqueue));               // for debug
        if (try_again) {
        	printf("MDS : try again\n");
            dequeue_m = mq_dequeue(mdss->fcqueue);          // dequeue
            //printf("MDS %ld : [DEQUEUE,REV] - current queue size is %d\n", mdss->id, mq_get_size(mdss->fcqueue));               // for debug

            // get the destination ID
            dest_id = codes_mapping_get_lpid_from_relative(mdss->id % mod, dest_group, "mdt", NULL, 0);

            // rewrite only header
            msg_set_header(magic_mdt, REQ, lp->gid, &dequeue_m->m_header);  // & operator here is same as &(ptr_dequeue_m->m_header), it means pointer of inner struct "m_header"

            m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(*dequeue_m), dequeue_m, 0, NULL, lp);

            // for debug
            printf("MDS %ld : [%s, REQ] -> MDT %ld, Others first..\n", mdss->id, i_to_s_wt(dequeue_m->work_type), mdss->id % mod);
        }
    }
	else {
		mdss->lock = 1;
        dequeue_m = mq_dequeue(mdss->fcqueue);          // dequeue
        //printf("MDS %ld : [DEQUEUE] - current queue size is %d\n", mdss->id, mq_get_size(mdss->fcqueue));               // for debug

            // get the destination ID
            dest_id = codes_mapping_get_lpid_from_relative(mdss->id % mod, dest_group, "mdt", NULL, 0);

            // rewrite only header
            msg_set_header(magic_mdt, REQ, lp->gid, &dequeue_m->m_header);  // & operator here is same as &(ptr_dequeue_m->m_header), it means pointer of inner struct "m_header"

            m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(*dequeue_m), dequeue_m, 0, NULL, lp);

            // for debug
            printf("MDS %ld : [%s, REQ] -> MDT %ld\n",
                mdss->id, i_to_s_wt(dequeue_m->work_type), mdss->id % mod);
	}

	return;
}

static void handle_mds_ack_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    tw_lpid dest_client_relative_id, dest_id;
    all_msg m_to_cli, local_m;

	// Copy all contents to new message
    m_to_cli = *m;
    // Rewrite header
    msg_set_header(magic_client, ACK, lp->gid, &m_to_cli.m_header);
    // Get destination client relative id
    dest_client_relative_id = m->client_id;
    dest_id = codes_mapping_get_lpid_from_relative(dest_client_relative_id, "client", "client", NULL, 0);

	if (mdss->lock) {
		if (!is_empty(mdss->fcqueue)) { // if Queue has values..
			// Set up local msg
    		msg_set_header(magic_mds, LOCAL, lp->gid, &local_m.m_header);

			/* send message back to client and local*/
            m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

            printf("MDS %ld : [%s, ACK] -> CLT %ld and MYSELF\n", mdss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
            return;
		}
		else { // if queue is empty
			/* send message back to client only*/
        	m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, 0, NULL, lp);

        	printf("MDS %ld : [%s, ACK] -> CLT %ld and no queue\n", mdss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
			mdss->lock = 0;
			return;
		}
	}
	else {
    	/* send message back to client only*/
    	m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, 0, NULL, lp);

    	// for debug
    	printf("MDS %ld : [%s, ACK] -> CLT %ld\n", mdss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
	}
	return;
}

static void handle_mds_local_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{	
	int mod;
    const char * dest_group;
	all_msg * dequeue_m;
	tw_lpid dest_client_relative_id, dest_id;

	dequeue_m = mq_dequeue(mdss->fcqueue);          // dequeue
    printf("MDS %ld : [DEQUEUE] - current queue size is %d\n", mdss->id, mq_get_size(mdss->fcqueue));               // for debug

    mod = num_MD_storage;
    dest_group = "MD_storage";

    // get the destination ID
    dest_id = codes_mapping_get_lpid_from_relative(mdss->id % mod, dest_group, "mdt", NULL, 0);

    // rewrite only header
    msg_set_header(magic_mdt, REQ, lp->gid, &dequeue_m->m_header);  // & operator here is same as &(ptr_dequeue_m->m_header), it means pointer of inner struct "m_header"

    m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(*dequeue_m), dequeue_m, 0, NULL, lp);

    // for debug
    printf("MDS %ld : [%s, REQ] -> MDT %ld\n", mdss->id, i_to_s_wt(dequeue_m->work_type), mdss->id % mod);
}

// reverse handler for req
static void handle_mds_req_rev_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    mdss->try_again = 1;
    model_net_event_rc2(lp, &m->ret);
    return;
}

// reverse handler for ack
static void handle_mds_ack_rev_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    model_net_event_rc2(lp, &m->ret);
    return;
}

static void handle_mds_local_rev_event(mds_state * mdss, tw_bf * b, all_msg * m, tw_lp * lp)
{
	mq_enqueue(mdss->fcqueue, m);
	model_net_event_rc2(lp, &m->ret);
    return;
}

//////////////////////////////////////////////////////
//                  3. OSS                          //
//////////////////////////////////////////////////////

static void oss_init(oss_state * osss, tw_lp * lp)
{
    // Initialize all
    osss->id = codes_mapping_get_lp_relative_id(lp->gid, 1, 0);

    // OSS handles multiple OST, so it should have that much Queues..
    osss->fcqueue = calloc( num_OS_storage / num_OS_server, sizeof(struct my_queue) );
	osss->bk_fcqueue = calloc ( num_OS_storage / num_OS_server, sizeof(struct my_queue) );
    osss->locks = calloc( num_OS_storage / num_OS_server, sizeof(int) );
	osss->try_again = calloc( num_OS_storage / num_OS_server, sizeof(int) );
	osss->bk_status = calloc( num_OS_storage / num_OS_server, sizeof(int) );
    for (int i=0; i<(num_OS_storage / num_OS_server); i++) {
        osss->locks[i] = 0;
		osss->try_again[i] = 0;
		osss->bk_status[i] = 0;
    }

    return;
}

static void oss_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
   switch (m->m_header.event_type)
    {
        case REQ:
            handle_oss_req_event(osss, b, m, lp);
            break;
        case ACK:
            handle_oss_ack_event(osss, b, m, lp);
            break;
		case LOCAL:
			handle_oss_local_event(osss, b, m, lp);
			break;
        case KICKOFF:
            printf("\n In OSS there is no KICKOFF sign coming in..");
            assert(0);
            break;
        default:
            printf("\n Invalid message type %d ", m->m_header.event_type);
            assert(0);
        break;
    }
}

static void oss_rev_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            printf("[UNEXPECTED] OSS %ld REV EVENT : REQ - handle_oss_req_rev_event\n", osss->id);
            handle_oss_req_rev_event(osss, b, m, lp);
            break;
        case ACK:
            printf("[UNEXPECTED] OSS %ld REV EVENT : ACK - handle_oss_ack_rev_event\n", osss->id);
            handle_oss_ack_rev_event(osss, b, m, lp);
            break;
		case LOCAL:
			printf("[UNEXPECTED] OSS %ld REV EVENT : LOCAL - handle_oss_local_rev_event\n", osss->id);
            handle_oss_local_rev_event(osss, b, m, lp);
			break;
        case KICKOFF:
            printf("[UNEXPECTED] OSS REV EVENT : KICKOFF - should NOT be happened\n");
            assert(0);
            break;
        default:
            printf("[UNEXPECTED] OSS REV EVENT : UNKNOWN - should NOT be happened\n");
            assert(0);
            break;
    }

    return;
}

/* once the simulation is over, show some comment. */
static void oss_finalize(oss_state * osss, tw_lp * lp)
{
    printf("OSS %ld (Global ID : %ld) done\n", osss->id, lp->gid);
//  mq_end();
    return;
}

static void handle_oss_req_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    int mod;
    const char * dest_group;
    tw_lpid dest_id;
    all_msg m_to_strg;
    all_msg * dequeue_m;

	mod = num_OS_storage;
    dest_group = "OS_storage";
	int tmp_array_num = m->rff[0]-(osss->id * (num_OS_storage / num_OS_server));

    mq_enqueue(&osss->fcqueue[tmp_array_num], m);

    if (osss->locks[tmp_array_num]) {
        printf("OSS %ld : [LOCKED] - Queue %d : left - %d\n", osss->id, tmp_array_num, mq_get_size(&osss->fcqueue[tmp_array_num]));
        if (osss->try_again[tmp_array_num]) {
			osss->try_again[tmp_array_num] = 0;
            printf("OSS %ld : try again - Queue %d\n", osss->id, tmp_array_num);
            dequeue_m = mq_dequeue(&osss->fcqueue[tmp_array_num]);

            m_to_strg = *dequeue_m;

            // for debug
            printf("OSS %ld : check dequeue array number - %d, rff[0] = %d, osss->id = %ld, cleanup count = %d\n", osss->id, tmp_array_num, m->rff[0], osss->id, m_to_strg.fid_values.cleanup_count);
            // get the destination ID
            dest_id = codes_mapping_get_lpid_from_relative(dequeue_m->rff[0], dest_group, "ost", NULL, 0);

            // rewrite only header
            msg_set_header(magic_ost, REQ, lp->gid, &m_to_strg.m_header);

            if (m_to_strg.work_type == READ) {
                m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(m_to_strg), &m_to_strg, 0, NULL, lp);
                printf("OSS %ld : [%s, REQ] -> OST %d, REV, others first \n", osss->id, i_to_s_wt(m_to_strg.work_type), dequeue_m->rff[0]);
            }
			else if (m_to_strg.work_type == WRITE || m_to_strg.work_type == WRITE_APPEND ) {
                if (m_to_strg.fid_values.cleanup_count == 0) {
                    m->ret = model_net_event(net_id, "req", dest_id, payload_sz + (m_to_strg.rff[1] * 1024 * 1024), 0.0, sizeof(m_to_strg), &m_to_strg, 0, NULL, lp);
                    printf("OSS %ld : [%s, REQ] -> OST %d, File Size : %d MB\n", osss->id, i_to_s_wt(m_to_strg.work_type), dequeue_m->rff[0], m_to_strg.rff[1]);
                }
                else {
                    m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(m_to_strg), &m_to_strg, 0, NULL, lp);
                    printf("OSS %ld : [%s, REQ] -> OST %d, Clean up fragments\n", osss->id, i_to_s_wt(m_to_strg.work_type), dequeue_m->rff[0]);

                }
			}
            else {
                printf("OSS [ERROR] - Wrong work type : %d\n", m_to_strg.work_type);
                return;
            }
        }
    }
	else {
    	osss->locks[tmp_array_num] = 1;
        dequeue_m = mq_dequeue(&osss->fcqueue[tmp_array_num]);

        m_to_strg = *dequeue_m;

        // for debug
        printf("OSS %ld : check dequeue array number - %d, rff[0] = %d, osss->id = %ld, cleanup count = %d\n", osss->id, tmp_array_num, m->rff[0], osss->id, m_to_strg.fid_values.cleanup_count);
        // get the destination ID
        dest_id = codes_mapping_get_lpid_from_relative(dequeue_m->rff[0], dest_group, "ost", NULL, 0);

        // rewrite only header
        msg_set_header(magic_ost, REQ, lp->gid, &m_to_strg.m_header);

        if (m_to_strg.work_type == READ) {
            m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(m_to_strg), &m_to_strg, 0, NULL, lp);

            // for debug
            printf("OSS %ld : [%s, REQ] -> OST %d\n", osss->id, i_to_s_wt(m_to_strg.work_type), dequeue_m->rff[0]);
			return;
        }
        else if (m_to_strg.work_type == WRITE || m_to_strg.work_type == WRITE_APPEND ) {
            if (m_to_strg.fid_values.cleanup_count == 0) {
                m->ret = model_net_event(net_id, "req", dest_id, payload_sz + (m_to_strg.rff[1] * 1024 * 1024), 0.0, sizeof(m_to_strg), &m_to_strg, 0, NULL, lp);

                // for debug
                printf("OSS %ld : [%s, REQ] -> OST %d, File Size : %d MB\n", osss->id, i_to_s_wt(m_to_strg.work_type), dequeue_m->rff[0], m_to_strg.rff[1]);
				return;
            }
            else {
                m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(m_to_strg), &m_to_strg, 0, NULL, lp);

                // for debug
                printf("OSS %ld : [%s, REQ] -> OST %d, Clean up fragments\n", osss->id, i_to_s_wt(m_to_strg.work_type), dequeue_m->rff[0]);
				return;
            }
        }
        else {
            printf("OSS [ERROR] - Wrong work type : %d\n", m_to_strg.work_type);
            return;
        }
    }
	return;
}

static void handle_oss_ack_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    tw_lpid dest_client_relative_id, dest_id;
    all_msg m_to_cli, local_m;

    // Copy all contents to new message
    m_to_cli = *m;
    // Rewrite header
    msg_set_header(magic_client, ACK, lp->gid, &m_to_cli.m_header);
    // Get destination client relative id
    dest_client_relative_id = m->client_id;
    dest_id = codes_mapping_get_lpid_from_relative(dest_client_relative_id, "client", "client", NULL, 0);

	int tmp_array_num = m->rff[0]-(osss->id * (num_OS_storage / num_OS_server));

	// Set up local msg
    msg_set_header(magic_oss, LOCAL, lp->gid, &local_m.m_header);
    local_m.tmp_array_num = tmp_array_num;

	if (osss->locks[tmp_array_num]) {
		if (!is_empty(&osss->fcqueue[tmp_array_num])) { // if Queue has values..
			if (m_to_cli.work_type == READ) {
                /* send message back to client & myself*/
                m->ret = model_net_event(net_id, "ack", dest_id, payload_sz + m_to_cli.file_size * 1024 * 1024, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

                printf("OSS %ld : [%s, ACK] -> CLT %ld, Fragment size : %d MB, Queue has VALUE\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
            }
            else if ((m_to_cli.work_type == WRITE) || (m_to_cli.work_type == WRITE_APPEND)) {
                /* send message back to client */
                m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

                if (m->fid_values.cleanup_count == 0) {
                    printf("OSS %ld : [%s, ACK] -> CLT %ld, Written file fragment size : %d MB, Queue has VALUE\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
                }
                else {
                    printf("OSS %ld : [%s, ACK] -> CLT %ld, CLEANUP complete, Queue has VALUE\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
                }
            }
            else {
                printf("OSS %ld : [ERROR] - Wrong work type = %d\n", osss->id, m_to_cli.work_type);
                return;
            }
			return;
		}
		else { // if Queue is empty
			if (m_to_cli.work_type == READ) {
				if (osss->bk_status[tmp_array_num]) { // In this case, I can guess that if LOCAL and ACK can be crashed simultaneously. So send local also..
					/* send message back to client */
                    m->ret = model_net_event(net_id, "ack", dest_id, payload_sz + m_to_cli.file_size * 1024 * 1024, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

                    // for debug
                    printf("OSS %ld : [%s, ACK] -> CLT %ld, Fragment size : %d MB, Queue EMPTY, BUT CHECK BACKUP!\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
				}
				else {
					/* send message back to client */
            		m->ret = model_net_event(net_id, "ack", dest_id, payload_sz + m_to_cli.file_size * 1024 * 1024, 0.0, sizeof(m_to_cli), &m_to_cli, 0, NULL, lp);

            		// for debug
            		printf("OSS %ld : [%s, ACK] -> CLT %ld, Fragment size : %d MB, Queue EMPTY\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
				}
        	}
        	else if ((m_to_cli.work_type == WRITE) || (m_to_cli.work_type == WRITE_APPEND)) {
				if (osss->bk_status[tmp_array_num]) { // In this case, I can guess that if LOCAL and ACK can be crashed simultaneously. So send local also..
					/* send message back to client */
                    m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

                    if (m->fid_values.cleanup_count == 0) {
                        printf("OSS %ld : [%s, ACK] -> CLT %ld, Written file fragment size : %d MB, Queue EMPTY, BUT CHECK BACKUP!\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
                    }
                    else {
                        printf("OSS %ld : [%s, ACK] -> CLT %ld, CLEANUP complete, Queue EMPTY, BUT CHECK BACKUP!\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
                    }
				}
				else {
					/* send message back to client */
            		m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, 0, NULL, lp);

            		if (m->fid_values.cleanup_count == 0) {
                		printf("OSS %ld : [%s, ACK] -> CLT %ld, Written file fragment size : %d MB, Queue EMPTY\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
            		}
            		else {
                		printf("OSS %ld : [%s, ACK] -> CLT %ld, CLEANUP complete, Queue EMPTY\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
            		}
				}
        	}
        	else {
            	printf("OSS %ld : [ERROR] - Wrong work type = %d\n", osss->id, m_to_cli.work_type);
				return;
        	}
			osss->locks[tmp_array_num] = 0; // Because queue is empty, need to unlock..
			return;
		}
	}
	else { // If there's no lock - which means queue is empty.. so send only to CLT...
		if (m_to_cli.work_type == READ) {
			if (osss->bk_status[tmp_array_num]) { // In this case, I can guess that if LOCAL and ACK can be crashed simultaneously. So send local also..
				/* send message back to client and itself*/
                m->ret = model_net_event(net_id, "ack", dest_id, payload_sz + m_to_cli.file_size * 1024 * 1024, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

                // for debug
                printf("OSS %ld : [%s, ACK] -> CLT %ld, Fragment size : %d MB, no LOCK BUT LOCAL AGAIN!!!!!\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
                return;
			}
			else {
        		/* send message back to client */
        		m->ret = model_net_event(net_id, "ack", dest_id, payload_sz + m_to_cli.file_size * 1024 * 1024, 0.0, sizeof(m_to_cli), &m_to_cli, 0, NULL, lp);

        		// for debug
        		printf("OSS %ld : [%s, ACK] -> CLT %ld, Fragment size : %d MB, no LOCK\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
    			return;
			}
		}
    	else if ((m_to_cli.work_type == WRITE) || (m_to_cli.work_type == WRITE_APPEND)) {
			if (osss->bk_status[tmp_array_num]) { // In this case, I can guess that if LOCAL and ACK can be crashed simultaneously. So send local also..
				/* send message back to client */
                m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, sizeof(local_m), &local_m, lp);

                if (m->fid_values.cleanup_count == 0) {
                    printf("OSS %ld : [%s, ACK] -> CLT %ld, Written file fragment size : %d MB, no LOCK BUT LOCAL AGAIN!!!!!\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
                }
                else {
                    printf("OSS %ld : [%s, ACK] -> CLT %ld, CLEANUP complete, no LOCK BUT LOCAL AGAIN!!!!!\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
                }
                return;
			}
			else {
        		/* send message back to client */
        		m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_cli), &m_to_cli, 0, NULL, lp);

        		if (m->fid_values.cleanup_count == 0) {
            		printf("OSS %ld : [%s, ACK] -> CLT %ld, Written file fragment size : %d MB, no LOCK\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id, m_to_cli.file_size);
        		}
        		else {
            		printf("OSS %ld : [%s, ACK] -> CLT %ld, CLEANUP complete, no LOCK\n", osss->id, i_to_s_wt(m_to_cli.work_type), dest_client_relative_id);
        		}
				return;
			}
    	}
    	else {
        	printf("OSS %ld : [ERROR] - Wrong work type = %d\n", osss->id, m_to_cli.work_type);
        	return;
    	}
	}
	return;
}

static void handle_oss_local_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
	int mod;
    const char * dest_group;
	tw_lpid dest_client_relative_id, dest_id;
	all_msg * dequeue_m;

	int tmp_array_num = m->tmp_array_num;
	if (osss->bk_status[tmp_array_num]) {
		osss->bk_status[tmp_array_num] = 0;
		if (!is_empty(&osss->bk_fcqueue[tmp_array_num])) {
			printf("OSS %ld : use backup data - QUEUE %d\n", osss->id, tmp_array_num);
			dequeue_m = mq_dequeue(&osss->bk_fcqueue[tmp_array_num]);
		}
		else {
			printf("OSS %ld : NO backup data - QUEUE %d\n", osss->id, tmp_array_num);
			return;
		}
	}
	else {
		if (!is_empty(&osss->bk_fcqueue[tmp_array_num]))
			mq_dequeue(&osss->bk_fcqueue[tmp_array_num]); // need to empty back up data
		dequeue_m = mq_dequeue(&osss->fcqueue[tmp_array_num]);          // dequeue			
	}
	mq_enqueue(&osss->bk_fcqueue[tmp_array_num], dequeue_m);	// for unexpected situation..
    //printf("OSS %ld dequeue from queue[%d] - event value check: work type : %d, client_id : %ld, file name : %d, file size : %d\n", osss->id, tmp_array_num, dequeue_m->work_type, dequeue_m->client_id, dequeue_m->file_name, dequeue_m->file_size);

    mod = num_OS_storage;
    dest_group = "OS_storage";

    // get the destination ID
    dest_id = codes_mapping_get_lpid_from_relative(dequeue_m->rff[0], dest_group, "ost", NULL, 0);

    // rewrite only header
    msg_set_header(magic_ost, REQ, lp->gid, &dequeue_m->m_header);  // & operator here is same as &(ptr_dequeue_m->m_header), it means pointer of inner struct "m_header"

    if (dequeue_m->work_type == READ) {
        m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(*dequeue_m), dequeue_m, 0, NULL, lp);

        printf("OSS %ld : [%s, REQ] -> OST %d, Send DEQUEUED message (from Queue[%d])\n", osss->id, i_to_s_wt(dequeue_m->work_type), dequeue_m->rff[0], tmp_array_num);
    }
    else if ((dequeue_m->work_type == WRITE) || (dequeue_m->work_type == WRITE_APPEND)) {
        if (dequeue_m->fid_values.cleanup_count == 0) {
            m->ret = model_net_event(net_id, "req", dest_id, payload_sz + dequeue_m->rff[1] * 1024 * 1024, 0.0, sizeof(*dequeue_m), dequeue_m, 0, NULL, lp);

            printf("OSS %ld : [%s, REQ] -> OST %d, Send DEQUEUED file (from Queue[%d]), File size is %d MB\n", osss->id, i_to_s_wt(dequeue_m->work_type), dequeue_m->rff[0], tmp_array_num, dequeue_m->rff[1]);
        }
        else {
            m->ret = model_net_event(net_id, "req", dest_id, payload_sz, 0.0, sizeof(*dequeue_m), dequeue_m, 0, NULL, lp);

            printf("OSS %ld : [%s, REQ] -> OST %d, Send DEQUEUED CLEANUP (from Queue[%d])\n", osss->id, i_to_s_wt(dequeue_m->work_type), dequeue_m->rff[0], tmp_array_num);
        }
    }
    else {
        printf("OSS %ld : [ERROR] - Wrong work type = %d\n", osss->id, dequeue_m->work_type);
        return;
    }
	return;
}

// reverse handler for req
static void handle_oss_req_rev_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
	int tmp_array_num = m->rff[0]-(osss->id * (num_OS_storage / num_OS_server));
    osss->try_again[tmp_array_num] = 1;
    model_net_event_rc2(lp, &m->ret);
    return;
}

// reverse handler for ack
static void handle_oss_ack_rev_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp)
{
    model_net_event_rc2(lp, &m->ret);
    return;
}

static void handle_oss_local_rev_event(oss_state * osss, tw_bf * b, all_msg * m, tw_lp * lp) 
{
	osss->bk_status[m->tmp_array_num] = 1;
	model_net_event_rc2(lp, &m->ret);
    return;
}

//////////////////////////////////////////////////////
//                  4. MDT                          //
//////////////////////////////////////////////////////

static void mdt_init(mdt_state * mdts, tw_lp * lp)
{
    // Initialize all
    mdts->id = codes_mapping_get_lp_relative_id(lp->gid, 1, 0);
    mdts->last_saved_OST = 0;

    mdts->strg_tbl = qhash_init(mdt_hash_compare, mdt_hash_func, HASH_TABLE_SIZE);

    if(!mdts->strg_tbl)
        tw_error(TW_LOC, "\n Hash table is not initialized!\n");

    return;
}

static void mdt_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            handle_mdt_req_event(mdts, b, m, lp);
            break;
        case ACK:
            printf("\n In MDT there is no ACK sign coming in..");
            break;
        case KICKOFF:
            printf("\n In MDT there is no KICKOFF sign coming in..");
            assert(0);
            break;
        default:
            printf("\n MDT Invalid message type %d ", m->m_header.event_type);
            assert(0);
        break;
    }
}

static void mdt_rev_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            printf("[UNEXPECTED] MDT %ld REV EVENT : REQ - handle_mdt_req_rev_event\n", mdts->id);
            handle_mdt_req_rev_event(mdts, b, m, lp);
            break;
        case ACK:
            printf("[UNEXPECTED] MDT REV EVENT : ACK - should NOT be happened\n");
            assert(0);
            break;
        case KICKOFF:
            printf("[UNEXPECTED] MDT REV EVENT : KICKOFF - should NOT be happened\n");
            assert(0);
            break;
        default:
            printf("[UNEXPECTED] MDT REV EVENT : UNKNOWN - should NOT be happened\n");
            assert(0);
            break;
    }

    return;
}

/* once the simulation is over, show comment */
static void mdt_finalize(mdt_state * mdts, tw_lp * lp)
{
    // Free hash table
    qhash_finalize(mdts->strg_tbl);

    printf("MDT %ld (Global id : %ld) done\n", mdts->id, lp->gid);
    return;
}

static void handle_mdt_req_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    tw_lpid dest_id, dest_relative_id;
    all_msg m_to_svr;
    int current_ost = mdts->last_saved_OST;
    int ori_file_size, tmp_cnt;
    int tmp_cleanup_count = 0;
    int tmp_cleanup_place[MAX_STRIPE_COUNT];
    for (int j = 0; j < MAX_STRIPE_COUNT; j++) {
        tmp_cleanup_place[j] = 0;
    }

    struct qhash_head * hash_link = NULL;

    // get the destination global ID
    dest_id = m->m_header.src;
    dest_relative_id = codes_mapping_get_lp_relative_id(dest_id, 1, 0);

    // Add all message contents from transmitted message
    m_to_svr = *m;

	// rewrite only header
    msg_set_header(magic_mds, ACK, lp->gid, &m_to_svr.m_header);

	// if we receive req in MDT, then send FID back to client
	struct jh_mdt_hash_key key;
    key.f_name = m->file_name;

    struct mdt_qhash_entry * tmp = NULL;
    hash_link = qhash_search(mdts->strg_tbl, &key);

    if ( m->open_file == 1 ) {          // if it is just opening the file.. 
        if (hash_link) // if already exists, then send all the infos back to client
        {
            // get hash table
            tmp = qhash_entry(hash_link, struct mdt_qhash_entry, hash_link);
            // copy values from hash table into message (int for assign, array for memcpy) 
            m_to_svr.fid_values.FID_value = tmp->uid;
            m_to_svr.fid_values.stripe_count = tmp->stripe_count;
            memcpy(m_to_svr.fid_values.stripe_place, tmp->stripe_place, sizeof(tmp->stripe_place));
            memcpy(m_to_svr.fid_values.stripe_stored_size, tmp->stripe_stored_size, sizeof(tmp->stripe_stored_size));
        }
        else // if it does NOT exist, then in this status, just send back the result.. 
        {
            m_to_svr.fid_values.FID_value = 0;
        }
    }
	else {  // This would be only in case of WRITE or WRITE_APPEND
        if ( m->work_type == WRITE ) {
            if (hash_link) { // If data already exist, then delete contents and write it from beginning.
                ori_file_size = m->file_size;
                tmp_cnt = stripe_count;

                tmp = qhash_entry(hash_link, struct mdt_qhash_entry, hash_link);
                tmp->size = ori_file_size; // Save file size first..

                if ( ((((tmp->stripe_count -1) * stripe_size) < ori_file_size) && ((tmp->stripe_count * stripe_size) >= ori_file_size))
                    || ( (((tmp->stripe_count -1) * stripe_size) < ori_file_size) && (tmp->stripe_count == stripe_count) ) ){ // This is coverable with current status
                    while ( (ori_file_size > 0) && (tmp_cnt > 0) ) {
                        //printf("MDT(WRITE - OVERWRITE, case1) : Stripe will be saved on OST %d\n", tmp->stripe_place[stripe_count-tmp_cnt]);
                        if (ori_file_size <= stripe_size)
                            tmp->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                        else {
                            if (tmp_cnt == 1)
                                tmp->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                            else
                                tmp->stripe_stored_size[stripe_count-tmp_cnt] = stripe_size;
                        }
                        //printf("MDT(WRITE - OVERWRITE, case1) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[stripe_count-tmp_cnt]);
                        ori_file_size = ori_file_size - stripe_size;
                        tmp_cnt--;
                    }
                }
                else if ( ((tmp->stripe_count -1) * stripe_size) >= ori_file_size ) {  // This is when file smaller than saved file
                    int small_cnt = tmp->stripe_count;
                    while ( (ori_file_size > 0) && (tmp_cnt > 0) ) {
                        //printf("MDT(WRITE - OVERWRITE, case2) : Stripe will be saved on OST %d\n", tmp->stripe_place[stripe_count-tmp_cnt]);
                        if (ori_file_size <= stripe_size)
                            tmp->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                        else
                            tmp->stripe_stored_size[stripe_count-tmp_cnt] = stripe_size;
                        //printf("MDT(WRITE - OVERWRITE, case2) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[stripe_count-tmp_cnt]);
                        ori_file_size = ori_file_size - stripe_size;
                        tmp_cnt--;
                    }
                    // Now delete previous saved file stripe
                    for ( int i = stripe_count-tmp_cnt; i < small_cnt; i++ ) {
                        //printf("MDT(WRITE - OVERWRITE, case2) : clean up - previously saved STRIPE %d : OST %d with size : %d\n", i, tmp->stripe_place[i], tmp->stripe_stored_size[i]);
                        // Save cleanup fragment in tmp
                        tmp_cleanup_place[tmp_cleanup_count] = tmp->stripe_place[i];
                        tmp_cleanup_count++;
                        // Clean up in hash
                        tmp->stripe_stored_size[i] = 0;
                        tmp->stripe_place[i] = 0;
                    }
                    // modify stripe_count in hash
                    tmp->stripe_count = stripe_count - tmp_cnt;
                    //printf("MDT(WRITE - OVERWRITE, case2) : Total %d stripe will be saved..\n", tmp->stripe_count);
                }
				else { // This is when file bigger than saved file
                    int big_cnt = tmp->stripe_count;
                    while ( (ori_file_size > 0) && (big_cnt > 0) ) {
                        //printf("MDT(WRITE - OVERWRITE, case3) : Stripe will be saved on OST %d\n", tmp->stripe_place[stripe_count-tmp_cnt]);
                        tmp->stripe_stored_size[stripe_count-tmp_cnt] = stripe_size;
                        //printf("MDT(WRITE - OVERWRITE, case3) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[stripe_count-tmp_cnt]);
                        ori_file_size = ori_file_size - stripe_size;
                        big_cnt--;
                    }
                    // Now fill the rest of file with stripe
                    int rest_stripe_cnt = stripe_count - (tmp->stripe_count);
                    while ( (ori_file_size > 0) && (rest_stripe_cnt > 0) ) {
                        current_ost = calc_current_ost(current_ost);
                        tmp->stripe_place[stripe_count-rest_stripe_cnt] = current_ost;
                        // for debug
                        //printf("MDT(WRITE - OVERWRITE, case3) : ADDITIONAL Stripe will be saved on OST %d\n", tmp->stripe_place[stripe_count-rest_stripe_cnt]);

                        if (ori_file_size <= stripe_size)
                            tmp->stripe_stored_size[stripe_count-rest_stripe_cnt] = ori_file_size;
                        else {
                            if (rest_stripe_cnt == 1)
                                tmp->stripe_stored_size[stripe_count-rest_stripe_cnt] = ori_file_size;
                            else
                                tmp->stripe_stored_size[stripe_count-rest_stripe_cnt] = stripe_size;
                        }
                        //printf("MDT(WRITE - OVERWRITE, case3) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[stripe_count-rest_stripe_cnt]);
                        ori_file_size = ori_file_size - stripe_size;
                        rest_stripe_cnt--;
                    }
                    // modify stripe_count in hash
                    tmp->stripe_count = stripe_count - rest_stripe_cnt;
                    //printf("MDT(WRITE - OVERWRITE, case3) : Total %d stripe will be saved..\n", tmp->stripe_count);
                }
            }
            else {  // File size divide and save the chosen place.. This data would be saved in hash table..

                // create new hash struct
                struct mdt_qhash_entry * new_entry = malloc(sizeof (struct mdt_qhash_entry));
                new_entry->key = key;
                new_entry->uid = tw_rand_integer(lp->rng, 30, 1000); // Assign new FID 
                ori_file_size = m->file_size;
                new_entry->size = ori_file_size;
                tmp_cnt = stripe_count;
                for (int i=0; i < MAX_STRIPE_COUNT; i++) {
                    new_entry->stripe_place[i] = 0;
                    new_entry->stripe_stored_size[i] = 0;
                }

                while ( (ori_file_size > 0) && (tmp_cnt > 0) ) {
                    // for debug
                    //printf("remained ori file size = %d, tmp count = %d\n", ori_file_size, tmp_cnt);
                    current_ost = calc_current_ost(current_ost);
                    new_entry->stripe_place[stripe_count-tmp_cnt] = current_ost;
                    // for debug
                    //printf("MDT(WRITE - NEW) : Stripe will be saved on OST %d\n", new_entry->stripe_place[stripe_count-tmp_cnt]);

                    if (ori_file_size <= stripe_size)
                        new_entry->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                    else {
                        if (tmp_cnt == 1)
                            new_entry->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                        else
                            new_entry->stripe_stored_size[stripe_count-tmp_cnt] = stripe_size;
                    }
                    //printf("MDT(WRITE - NEW) : in OST, size of %d Stripe will be saved\n", new_entry->stripe_stored_size[stripe_count-tmp_cnt]);
                    ori_file_size = ori_file_size - stripe_size;
                    tmp_cnt--;
                }
                new_entry->stripe_count = stripe_count - tmp_cnt;
                //printf("MDT(WRITE - NEW) : Total %d stripe will be saved..\n", new_entry->stripe_count);

                // Save it to hash table.... 
                qhash_add(mdts->strg_tbl, &key, &(new_entry->hash_link));
                hash_link = &(new_entry->hash_link);
            }
        }
		else if ( m->work_type == WRITE_APPEND ) {
            if (hash_link) {
                ori_file_size = m->file_size;
                tmp_cnt = stripe_count;

                tmp = qhash_entry(hash_link, struct mdt_qhash_entry, hash_link);
                tmp->size = tmp->size + ori_file_size;      // Save file size first... but it should be added!!!!!

                int exist_cnt = tmp->stripe_count;
                if ( exist_cnt == stripe_count ) {
                    //printf("MDT(WRITE_APPEND - APPEND, case1) : All data will be appended on OST %d\n", tmp->stripe_place[stripe_count-1]);
                    tmp->stripe_stored_size[stripe_count-1] = tmp->stripe_stored_size[stripe_count-1] + ori_file_size;
                    //printf("MDT(WRITE_APPEND - APPEND, case1) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[stripe_count-1]);
                }
				else {
                    int exist_cnt = tmp->stripe_count;
                    int partial_stripe = tmp->stripe_stored_size[exist_cnt-1];
                    if ( partial_stripe < stripe_size ) {
                        if ( (partial_stripe + ori_file_size) <= stripe_size ) {
                            tmp->stripe_stored_size[exist_cnt-1] = partial_stripe + ori_file_size;
                            //printf("MDT(WRITE_APPEND - APPEND, case2-1) : All data will be appended on OST %d\n", tmp->stripe_place[exist_cnt-1]);
                            //printf("MDT(WRITE_APPEND - APPEND, case2-1) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[exist_cnt-1]);
                            ori_file_size = 0;
                        }
                        else {
                            int filled_data = stripe_size - partial_stripe;
                            tmp->stripe_stored_size[exist_cnt-1] = stripe_size;
                            ori_file_size = ori_file_size - filled_data;
                            //printf("MDT(WRITE_APPEND - APPEND, case2-2) : Partial data will be appended on OST %d\n", tmp->stripe_place[exist_cnt-1]);
                            //printf("MDT(WRITE_APPEND - APPEND, case2-2) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[exist_cnt-1]);
                        }
                    }
                    tmp_cnt = tmp_cnt - exist_cnt; // until exist_cnt has been already filled, I need to care of others..
                    while ( (ori_file_size > 0) && (tmp_cnt > 0) ) {
                        current_ost = calc_current_ost(current_ost);
                        tmp->stripe_place[stripe_count-tmp_cnt] = current_ost;
                        // for debug
                        //printf("MDT(WRITE_APPEND - APPEND, case2) : Stripe will be saved on OST %d\n", tmp->stripe_place[stripe_count-tmp_cnt]);

                        if (ori_file_size <= stripe_size)
                            tmp->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                        else {
                            if (tmp_cnt == 1)
                                tmp->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                            else
                                tmp->stripe_stored_size[stripe_count-tmp_cnt] = stripe_size;
                        }
                        //printf("MDT(WRITE_APPEND - APPEND, case2) : in OST, size of %d Stripe will be saved\n", tmp->stripe_stored_size[stripe_count-tmp_cnt]);
                        ori_file_size = ori_file_size - stripe_size;
                        tmp_cnt--;
                    }
                    // modify stripe_count in hash
                    tmp->stripe_count = stripe_count - tmp_cnt;
                    //printf("MDT(WRITE_APPEND - APPEND, case2) : Total %d stripe will be saved..\n", tmp->stripe_count);
                }
            }
			else { // do it same as new WRITE
                // create new hash struct
                struct mdt_qhash_entry * new_entry = malloc(sizeof (struct mdt_qhash_entry));
                new_entry->key = key;
                new_entry->uid = tw_rand_integer(lp->rng, 30, 1000); // Assign new FID
                ori_file_size = m->file_size;
                new_entry->size = ori_file_size;
                tmp_cnt = stripe_count;
                for (int i=0; i < MAX_STRIPE_COUNT; i++) {
                    new_entry->stripe_place[i] = 0;
                    new_entry->stripe_stored_size[i] = 0;
                }

                while ( (ori_file_size > 0) && (tmp_cnt > 0) ) {
                    current_ost = calc_current_ost(current_ost);
                    new_entry->stripe_place[stripe_count-tmp_cnt] = current_ost;
                    // for debug
                    //printf("MDT(WRITE_APPEND - NEW) : Stripe will be saved on OST %d\n", new_entry->stripe_place[stripe_count-tmp_cnt]);

                	if (ori_file_size <= stripe_size)
                        new_entry->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                    else {
                        if (tmp_cnt == 1)
                            new_entry->stripe_stored_size[stripe_count-tmp_cnt] = ori_file_size;
                        else
                            new_entry->stripe_stored_size[stripe_count-tmp_cnt] = stripe_size;
                    }
                    //printf("MDT(WRITE_APPEND - NEW) : in OST, size of %d Stripe will be saved\n", new_entry->stripe_stored_size[stripe_count-tmp_cnt]);
                    ori_file_size = ori_file_size - stripe_size;
                    tmp_cnt--;
                }
                new_entry->stripe_count = stripe_count - tmp_cnt;
                //printf("MDT(WRITE_APPEND - NEW) : Total %d stripe will be saved..\n", new_entry->stripe_count);

                // Save it to hash table.... 
                qhash_add(mdts->strg_tbl, &key, &(new_entry->hash_link));
                hash_link = &(new_entry->hash_link);
            }
        }
        else {
            printf("MDT %ld : [ERROR] - Wrong work type = %d\n", mdts->id, m->work_type);
            return;
        }

		// get all proceed final data of hash table
        struct mdt_qhash_entry * final_tmp = NULL;
        hash_link = qhash_search(mdts->strg_tbl, &key);
        final_tmp = qhash_entry(hash_link, struct mdt_qhash_entry, hash_link);

        // copy values from hash table into message (int for assign, array for memcpy) 
        m_to_svr.fid_values.FID_value = final_tmp->uid;
        m_to_svr.file_name = final_tmp->key.f_name;
        m_to_svr.file_size = final_tmp->size;
        m_to_svr.fid_values.stripe_count = final_tmp->stripe_count;
        //m_to_svr.open_file = 0;
        memcpy(m_to_svr.fid_values.stripe_place, final_tmp->stripe_place, sizeof(final_tmp->stripe_place));
        memcpy(m_to_svr.fid_values.stripe_stored_size, final_tmp->stripe_stored_size, sizeof(final_tmp->stripe_stored_size));
        printf("MDT %ld : Final sending value - to CLT %ld, FID : %d, file name : %d, file size : %d\n", mdts->id, m_to_svr.client_id, m_to_svr.fid_values.FID_value, m_to_svr.file_name, m_to_svr.file_size);
        printf("MDT %ld : Final sending value - stripe_place : [%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]\n", mdts->id,
                    m_to_svr.fid_values.stripe_place[0], m_to_svr.fid_values.stripe_place[1], m_to_svr.fid_values.stripe_place[2], m_to_svr.fid_values.stripe_place[3],
                    m_to_svr.fid_values.stripe_place[4], m_to_svr.fid_values.stripe_place[5], m_to_svr.fid_values.stripe_place[6], m_to_svr.fid_values.stripe_place[7],
                    m_to_svr.fid_values.stripe_place[8], m_to_svr.fid_values.stripe_place[9], m_to_svr.fid_values.stripe_place[10],m_to_svr.fid_values.stripe_place[11],
                    m_to_svr.fid_values.stripe_place[12],m_to_svr.fid_values.stripe_place[13],m_to_svr.fid_values.stripe_place[14],m_to_svr.fid_values.stripe_place[15],
                    m_to_svr.fid_values.stripe_place[16],m_to_svr.fid_values.stripe_place[17],m_to_svr.fid_values.stripe_place[18],m_to_svr.fid_values.stripe_place[19]);
        printf("MDT %ld : Final sending value - stripe_stored_size : [%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]\n", mdts->id,
                 m_to_svr.fid_values.stripe_stored_size[0], m_to_svr.fid_values.stripe_stored_size[1], m_to_svr.fid_values.stripe_stored_size[2], m_to_svr.fid_values.stripe_stored_size[3],
                 m_to_svr.fid_values.stripe_stored_size[4], m_to_svr.fid_values.stripe_stored_size[5], m_to_svr.fid_values.stripe_stored_size[6], m_to_svr.fid_values.stripe_stored_size[7],
                 m_to_svr.fid_values.stripe_stored_size[8], m_to_svr.fid_values.stripe_stored_size[9], m_to_svr.fid_values.stripe_stored_size[10],m_to_svr.fid_values.stripe_stored_size[11],
                 m_to_svr.fid_values.stripe_stored_size[12],m_to_svr.fid_values.stripe_stored_size[13],m_to_svr.fid_values.stripe_stored_size[14],m_to_svr.fid_values.stripe_stored_size[15],
                 m_to_svr.fid_values.stripe_stored_size[16],m_to_svr.fid_values.stripe_stored_size[17],m_to_svr.fid_values.stripe_stored_size[18],m_to_svr.fid_values.stripe_stored_size[19]);
        if (tmp_cleanup_count != 0) {
            m_to_svr.fid_values.cleanup_count = tmp_cleanup_count;
            memcpy(m_to_svr.fid_values.cleanup_place, tmp_cleanup_place, sizeof(tmp_cleanup_place));
            printf("MDT %ld : Final sending value - Cleanup : %d stripe(s)\n", mdts->id, m_to_svr.fid_values.cleanup_count);
            printf("MDT %ld : Final sending value - Cleanup place : [%d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d %d]\n", mdts->id,
                    m_to_svr.fid_values.cleanup_place[0], m_to_svr.fid_values.cleanup_place[1], m_to_svr.fid_values.cleanup_place[2], m_to_svr.fid_values.cleanup_place[3],
                    m_to_svr.fid_values.cleanup_place[4], m_to_svr.fid_values.cleanup_place[5], m_to_svr.fid_values.cleanup_place[6], m_to_svr.fid_values.cleanup_place[7],
                    m_to_svr.fid_values.cleanup_place[8], m_to_svr.fid_values.cleanup_place[9], m_to_svr.fid_values.cleanup_place[10],m_to_svr.fid_values.cleanup_place[11],
                    m_to_svr.fid_values.cleanup_place[12],m_to_svr.fid_values.cleanup_place[13],m_to_svr.fid_values.cleanup_place[14],m_to_svr.fid_values.cleanup_place[15],
                    m_to_svr.fid_values.cleanup_place[16],m_to_svr.fid_values.cleanup_place[17],m_to_svr.fid_values.cleanup_place[18],m_to_svr.fid_values.cleanup_place[19]);
        }
    }
    m_to_svr.come_from_MD = 1;
    mdts->last_saved_OST = current_ost;   // always save current_ost!!!
    m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_svr), &m_to_svr, 0, NULL, lp);
    // for debug
    //printf("MDT %ld : [%s, ACK] -> MDS %ld, Send FID %d\n", mdts->id, i_to_s_wt(m_to_svr.work_type), dest_relative_id, m_to_svr.fid_values.FID_value);
	
	return;
}

/* reverse handler for ack - do nothing and roll back*/
static void handle_mdt_req_rev_event(mdt_state * mdts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    model_net_event_rc2(lp, &m->ret);
    return;
}

//////////////////////////////////////////////////////
//                  5. OST                          //
//////////////////////////////////////////////////////

static void ost_init(ost_state * osts, tw_lp * lp)
{
    // Initialize all
    osts->id = codes_mapping_get_lp_relative_id(lp->gid, 1, 0);
    osts->saved_size = 0;

    osts->strg_tbl = qhash_init(ost_hash_compare, ost_hash_func, HASH_TABLE_SIZE);

    if(!osts->strg_tbl)
        tw_error(TW_LOC, "\n Hash table is not initialized!\n");

    return;
}

static void ost_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            handle_ost_req_event(osts, b, m, lp);
            break;
        case ACK:
            printf("\n In OST there is no ACK sign coming in..");
            break;
        case KICKOFF:
            printf("\n In OST there is no KICKOFF sign coming in..");
            assert(0);
            break;
        default:
            printf("\n OST - Invalid message type %d ", m->m_header.event_type);
            assert(0);
        break;
    }
}

static void ost_rev_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    switch (m->m_header.event_type)
    {
        case REQ:
            printf("[UNEXPECTED] OST %ld REV EVENT : REQ - handle_ost_req_rev_event\n", osts->id);
            handle_ost_req_rev_event(osts, b, m, lp);
            break;
        case ACK:
            printf("[UNEXPECTED] OST REV EVENT : ACK - should NOT be happened\n");
            assert(0);
            break;
        case KICKOFF:
            printf("[UNEXPECTED] OST REV EVENT : KICKOFF - should NOT be happened\n");
            assert(0);
            break;
        default:
            printf("[UNEXPECTED] OST REV EVENT : UNKNOWN - should NOT be happened\n");
            assert(0);
            break;
    }

    return;
}

/* once the simulation is over, do some output */
static void ost_finalize(ost_state * osts, tw_lp * lp)
{
    // Free hash table
    qhash_finalize(osts->strg_tbl);

	//  printf("OST %ld (Global id : %ld) done\n", osts->id, lp->gid);
    printf("OST %ld (Global id : %ld) : Total %d MB data is saved..\n", osts->id, lp->gid, osts->saved_size);
    return;
}

static void handle_ost_req_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    tw_lpid dest_id, dest_relative_id;
    all_msg m_to_svr;
    int ori_file_size, tmp_cnt;
    int tmp_cleanup_count = 0;
    int tmp_cleanup_place[MAX_STRIPE_COUNT];
    for (int j = 0; j < MAX_STRIPE_COUNT; j++) {
        tmp_cleanup_place[j] = 0;
    }

    struct qhash_head * hash_link = NULL;

    // get the destination global ID
    dest_id = m->m_header.src;
    dest_relative_id = codes_mapping_get_lp_relative_id(dest_id, 1, 0);

    // Add all message contents from transmitted message
    m_to_svr = *m;

    // rewrite only header
    msg_set_header(magic_oss, ACK, lp->gid, &m_to_svr.m_header);

	struct jh_ost_hash_key key;
    key.f_name = m->file_name;
    key.uid = m->fid_values.FID_value;

    struct ost_qhash_entry * tmp = NULL;
    hash_link = qhash_search(osts->strg_tbl, &key);

    if (m->work_type == READ) {
        if (!hash_link) {
            printf("OST %ld : [ERROR, RD] - It shouldn't happen!!\n", osts->id);
            return;
        }

        tmp = qhash_entry(hash_link, struct ost_qhash_entry, hash_link);
        m_to_svr.file_size = tmp->stripe_stored_size;
        if (m_to_svr.file_size == m->rff[1]) { // correct data
            m->ret = model_net_event(net_id, "ack", dest_id, payload_sz + m_to_svr.file_size * 1024 * 1024, 0.0, sizeof(m_to_svr), &m_to_svr, 0, NULL, lp);
            printf("OST %ld : [%s, ACK] -> OSS %ld, File fragment size is %d MB\n", osts->id, i_to_s_wt(m_to_svr.work_type), dest_relative_id, m_to_svr.file_size);
        }
        else {
            printf("OST %ld : [ERROR, %s] - expected saved size in MDT = %d, real saved size in OST = %d is different!\n", osts->id, i_to_s_wt(m->work_type), m->rff[1], m_to_svr.file_size);
   			return;
        }
    }
    else if (m->work_type == WRITE || m->work_type == WRITE_APPEND) {
        if (m->fid_values.cleanup_count == 0) {
            if (hash_link) {  // If already saved..
                tmp = qhash_entry(hash_link, struct ost_qhash_entry, hash_link);
                int old_value = tmp->stripe_stored_size;
                tmp->stripe_stored_size = m->rff[1];

                osts->saved_size = osts->saved_size - old_value + tmp->stripe_stored_size;
                printf("OST %ld OVERWRITE or APPEND : old data size = %d MB, new data size = %d MB, total saved data on OST = %d MB \n", osts->id, old_value, tmp->stripe_stored_size, osts->saved_size);
                m_to_svr.file_size = tmp->stripe_stored_size;
            }
            else { // If not exist..
                struct ost_qhash_entry * new_entry = malloc(sizeof (struct ost_qhash_entry));
                new_entry->key = key;
                new_entry->stripe_stored_size = m->rff[1];

                // Save it to hash table....
                qhash_add(osts->strg_tbl, &key, &(new_entry->hash_link));
                hash_link = &(new_entry->hash_link);

                // To check how much data is saved on this server, add this number in OST status
                osts->saved_size = osts->saved_size + m->rff[1];

                printf("OST %ld WRITE NEW : new data size = %d MB, total saved data on OST = %d MB \n", osts->id, new_entry->stripe_stored_size, osts->saved_size);
                m_to_svr.file_size = new_entry->stripe_stored_size;
            }

            m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_svr), &m_to_svr, 0, NULL, lp);
            printf("OST %ld : [%s, ACK] -> OSS %ld, Saved file fragment size is %d MB\n", osts->id, i_to_s_wt(m_to_svr.work_type), dest_relative_id, m_to_svr.file_size);
        }
		else {  // cleanup process
            if (hash_link) {
                // delete this from hash table..
                hash_link = qhash_search_and_remove(osts->strg_tbl, &key);
                tmp = qhash_entry(hash_link, struct ost_qhash_entry, hash_link);
                int tmp_stored_size = tmp->stripe_stored_size;
                free(tmp);

                // Because it is removed, so update current saved storage size
                osts->saved_size = osts->saved_size - tmp_stored_size;

                printf("OST %ld CLEANUP : total saved data on OST = %d MB \n", osts->id, osts->saved_size);

                m->ret = model_net_event(net_id, "ack", dest_id, payload_sz, 0.0, sizeof(m_to_svr), &m_to_svr, 0, NULL, lp);

                printf("OST %ld : [%s, ACK] -> OSS %ld, CLEANUP complete\n", osts->id, i_to_s_wt(m_to_svr.work_type), dest_relative_id);
            }
            else {
                printf("OST %ld : [ERROR, %s(CLEANUP)] - It shouldn't happen!!\n", osts->id, i_to_s_wt(m->work_type));
                return;
            }
        }
    }
    else {
        printf("OST %ld : [ERROR] - Wrong work type = %d\n", osts->id, m->work_type);
        return;
    }
	return;
}

/* reverse handler for ack*/
static void handle_ost_req_rev_event(ost_state * osts, tw_bf * b, all_msg * m, tw_lp * lp)
{
    model_net_event_rc2(lp, &m->ret);
    return;
}

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ft=c ts=8 sts=4 sw=4 expandtab
 */


