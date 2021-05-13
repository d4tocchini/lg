#ifndef _LG_HEADER
#define _LG_HEADER
// #define assert(x)
#include "std.h"
#include "db.h"

#define DBS (sizeof(DB_INFO)/sizeof(*DB_INFO))
#define DB_LOG          0
#define DB_KEY          1
#define DB_KEY_IDX      2
#define DB_SCALAR       3
#define DB_SCALAR_IDX   4
#define DB_NODE_IDX     5
#define DB_EDGE_IDX     6
#define DB_PROP_IDX     7
#define DB_SRCNODE_IDX  8
#define DB_TGTNODE_IDX  9
#define DB_KV          10
#define DB_KVBM        11
#define DB_TXNLOG      12

// #define LG_graph LG_graph
#define LG_Node ggnode_t
#define LG_Edge ggedge_t
#define LG_Prop ggprop_t

// data
// typedef uint32_t    LG_id;
typedef uint64_t    LG_id;
typedef size_t      LG_size;
typedef LG_id       LG_str;
typedef LG_id       LG_log;
typedef LG_id       LG_blob;
typedef LG_log      LG_node;
typedef LG_log      LG_edge;
typedef LG_log      LG_prop;
// typedef LG_id       LG_txn
// TODO:
typedef LG_id    pod_id_t;
typedef LG_id    logID_t;
typedef LG_id    strID_t;
typedef LG_id    txnID_t;

typedef struct
LG_Blob {
    LG_blob id;
    union {
        buffer_t buffer;
        struct {
            LG_size size;
            void*   data;
        };
        struct {
            LG_size len;
            char*   string;
        };
    };
} LG_Blob;

typedef struct
pod_t {
    #define  pod_size_t     LG_size
    #define pod_data_t     void*
    pod_size_t    size;   // size of the data item
    pod_data_t    data;   // address of the data item
    pod_id_t 	    id;
} pod_t;

#define pod_num(x) 	    ((pod_t){ .id=(pod_id_t)(x) })
#define pod_buf(x, sz) 	((pod_t){ .data=(pod_data_t)(x), .size=(pod_size_t)(sz) })
#define _pod_str(x)       pod_buf(x, strlen(x))
#define pod_str(x)        _pod_str((x))

#define ggstr           pod_str;
#define ggbuf           pod_buf;

typedef struct
LG_txn {
    // status codes
    // #define G_NOTFOUND  DB_NOTFOUND
    // #define G_SUCCESS   DB_SUCCESS
// everything after 'txn' is copied to a parent txn on commit success
    struct txn_t txn;
    LG_str next_strID;
    LG_log next_logID;
    LG_log begin_nextID;
    int64_t node_delta;
    int64_t edge_delta;
// everything from prev_id down may be copied to a parent txn on commit fail/abort
// (if the parent didn't already have it)
// NOTE: prev_* fields are only valid if prev_start is non-zero
    txnID_t prev_id;
    LG_log  prev_start;
    LG_log  prev_count;
    LG_size prev_nodes;
    LG_size prev_edges;
} LG_txn;

#define TXN_DB(txn) ((txn_t)(txn))->db
#define TXN_RW(txn) ((txn)->txn.rw)
#define TXN_RO(txn) ((txn)->txn.ro)
#define TXN_PARENT(txn) ((LG_txn*)(((txn_t)(txn))->parent))


LG_id ggdb_next_id(LG_txn* txn, const int consume, LG_id * const cache, const int db1, const int venc);
int pod_resolve(LG_txn* txn, pod_t* d, int readonly);
char* graph_string(LG_txn* txn, LG_str id, size_t *len);
static INLINE int ggblob_resolve(LG_txn* txn, LG_str *ret, void const *data, const size_t len, int readonly);
static INLINE char* graph_string_enc(LG_txn* txn, void *id_enc, size_t *slen);
static INLINE int ggdb_resolve_blob(LG_txn* txn, LG_id *ret, char const *data, const size_t len, const int readonly, LG_id * const cache, int db1, int db2);
    // TODO: ugh, returns 1 for success, 0 for failure (only for readonly)



// edge directions
#define GRAPH_DIR_IN   0x1
#define GRAPH_DIR_OUT  0x2
#define GRAPH_DIR_BOTH 0x3

// log entry types
#define GRAPH_DELETION 0x0
#define GRAPH_NODE     0x1
#define GRAPH_EDGE     0x2
#define GRAPH_PROP     0x3

// max log entry size is for edge_t
#define LOG_MAX_BUF_SIZE (1 + esizeof(LG_str) * 2 + esizeof(LG_log) * 3)

// For deletions, the 'next' field points to the top-level entry that was the target of the delete.
// As a deletion may cascade to multiple children, I don't think it makes any sense to reserve it for pointing to a future entry.

typedef struct
ggentry_t {
    LG_log id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    LG_log next;
} ggentry_t;

typedef struct
ggnode_t {
    LG_log id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    LG_log next;
    LG_str type;
    LG_str val;
} ggnode_t;

typedef struct
ggedge_t {
    LG_log id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    LG_log next;
    LG_str type;
    LG_str val;
    LG_log src;
    LG_log tgt;
} ggedge_t;

typedef struct
ggprop_t {
    LG_log id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    LG_log next;
    LG_log pid;
    LG_str key;
    LG_str val;
} ggprop_t;

typedef struct
LG_iter {
    struct iter_t iter;
    LG_txn* txn;
    LG_log beforeID;
    struct LG_iter* next;
    int head_active;
} LG_iter;

//

typedef struct
pool_t {
    heap_t* heap;
    int count;
    int stride;
} pool_t;

// pool_


// graph

typedef struct
LG_graph {
    struct db_t db;
} LG_graph;


// context

typedef struct
ggctx_t {
    #ifndef GGtxnc
        #define GGtxnc 8
    #endif
    #ifndef GGnodec
        #define GGnodec 8
    #endif
    #ifndef GGedgec
        #define GGedgec 8
    #endif
    #ifndef GGgraphc
        #define GGgraphc 8
    #endif

    LG_graph root;
    pool_t graph_pool;

    LG_txn txns[GGtxnc];
    int txni;
    int txnc;
    ggnode_t nodes[GGnodec];
    int nodei;
    int nodec;
    ggedge_t edge[GGedgec];
    int edgei;
    int edgec;
    LG_graph graphs[GGgraphc];
    int graphi;
    int graphc;

    int pid;
    int basepath_len;
    char basepath[256];
} ggctx_t;

// lg api

int     lg_init(const char* path);
int     lg_open(LG_graph* g, const char * const subpath, const int flags);
void    lg_close(LG_graph* g);
int     lg_rm(LG_graph* g);
//      lg_mktemp
//      lg_exists

// scoped sugar api

// ugh, commas in macros bs
#ifndef _
    #define _ ,
#endif

#define Node_(ref)        lg_node_hydrate(txn, ref,  0, 0)
#define Edge_(ref)        lg_edge_hydrate(txn, ref,  0, 0)
#define Prop_(ref)        lg_prop_hydrate(txn, ref,  0, 0)
#define Blob_(ref)        lg_blob_hydrate(txn, ref,     0)
#define Node_b4_(ref, b4) lg_node_hydrate(txn, ref, b4, 1)
#define Edge_b4_(ref, b4) lg_edge_hydrate(txn, ref, b4, 1)
#define Prop_b4_(ref, b4) lg_prop_hydrate(txn, ref, b4, 1)
#define Blob_ro_(ref)     lg_blob_hydrate(txn, ref,     1)

#define node_(type, val)                  lg_node_resolve(txn, type, val,  0, 0)
#define node_ro_(type, val)               lg_node_resolve(txn, type, val,  0, 1)
#define node_b4_(type, val, b4)           lg_node_resolve(txn, type, val, b4, 1)
#define edge_(src, tgt, type, val)        lg_edge_resolve(txn, src, tgt, type, val,  0, 0)
#define edge_ro_(src, tgt, type, val)     lg_edge_resolve(txn, src, tgt, type, val,  0, 1)
#define edge_b4_(src, tgt, type, val, b4) lg_edge_resolve(txn, src, tgt, type, val, b4, 1)

#define graph_set_(key, val)              lg_prop_resolve(txn,   0, key, val,  0, 0)
#define graph_get_(key)                   lg_prop_resolve(txn,   0, key,   0,  0, 1)
#define graph_get_b4_(key, b4)            lg_prop_resolve(txn,   0, key,   0,  b4, 1)
#define set_(pid, key, val)               lg_prop_resolve(txn, pid, key, val,  0, 0)
#define get_(pid, key)                    lg_prop_resolve(txn, pid, key,   0,  0, 1)
#define get_b4_(pid, key, b4)             lg_prop_resolve(txn, pid, key,   0, b4, 1)
/* TODO:
#define graph_get_val_
#define graph_get_val_b4
#define graph_get_str_
#define graph_get_str_b4
#define get_val_
#define get_val_b4
#define get_str_
#define get_str_b4
*/

#define blob_(dat, len)                   lg_blob_resolve(txn, dat, len, 0)
#define blob_ro_(dat, len)                lg_blob_resolve(txn, dat, len, 1)
#define str_(str)                         lg_blob_resolve(txn, str, strlen(str), 0)
#define str_ro_(str)                      lg_blob_resolve(txn, str, strlen(str), 1)

#define delete_(id) lg_delete(txn, id)

#define node_read_(id, ref) lg_node_read(txn, id, ref)
#define edge_read_(id, ref) lg_edge_read(txn, id, ref)
#define prop_read_(id, ref) lg_prop_read(txn, id, ref)
#define blob_read_(id, ref) lg_blob_read(txn, id, ref)

#define nodes_count_()      lg_nodes_count(txn, 0)
#define edges_count_()      lg_edges_count(txn, 0)
#define nodes_count_b4_(b4) lg_nodes_count(txn, b4)
#define edges_count_b4_(b4) lg_edges_count(txn, b4)

#define props_(itr, id)         lg_props(txn, itr, id, 0)
#define props_b4_(itr, id, b4)  lg_props(txn, itr, id, b4)

#define nodes_(itr)                     lg_nodes(txn, itr, 0)
#define nodes_b4_(itr, b4)              lg_nodes(txn, itr, b4)
#define nodes_type_(itr, type)          lg_nodes_type(txn, itr, type, 0)
#define nodes_type_b4_(itr, type, b4)   lg_nodes_type(txn, itr, type, b4)

#define edges_(itr)                            lg_edges(txn, itr, 0)
#define edges_b4_(itr, b4)                     lg_edges(txn, itr, b4)
#define edges_type_(itr, type)                 lg_edges_type(txn, itr, type, 0)
#define edges_type_b4_(itr, type, b4)          lg_edges_type(txn, itr, type, b4)
#define edges_type_val_(itr, type, val)        lg_edges_type_val(txn, itr, type, val, 0)
#define edges_type_val_b4_(itr, type, val, b4) lg_edges_type_val(txn, itr, type, val, b4)

#define node_edges_in_(itr, tgt)                   lg_node_edges_in(txn, itr, tgt, 0)
#define node_edges_in_b4_(itr, tgt, b4)            lg_node_edges_in(txn, itr, tgt, b4)
#define node_edges_out_(itr, src)                  lg_node_edges_out(txn, itr, src, 0)
#define node_edges_out_b4_(itr, src, b4)           lg_node_edges_out(txn, itr, src, b4)
#define node_edges_type_in_(itr, tgt, type)        lg_node_edges_type_in(txn, itr, tgt, type, 0)
#define node_edges_type_in_b4_(itr, tgt, type, b4) lg_node_edges_type_in(txn, itr, tgt, type, b4)
#define node_edges_type_out_(itr, src, type)       lg_node_edges_type_out(txn, itr, src, type, 0)
#define node_edges_type_out_b4(itr, src, type, b4) lg_node_edges_type_out(txn, itr, src, type, b4)


LG_log  lg_delete(LG_txn* txn, LG_log id);

LG_size lg_nodes_count(LG_txn* txn, LG_log b4);
LG_size lg_edges_count(LG_txn* txn, LG_log b4);
LG_size lg_props_count(LG_txn* txn, LG_log b4);

#define lg_props(txn, itr, id, b4)                      lg_iter_p1_init(txn, itr, DB_PROP_IDX, id,    b4)
#define lg_nodes(txn, itr, b4)                          lg_iter_init(   txn, itr, DB_NODE_IDX, "", 0, b4)
#define lg_edges(txn, itr, b4)                          lg_iter_init(   txn, itr, DB_EDGE_IDX, "", 0,     b4)
#define lg_nodes_type(txn, itr, type, b4)               lg_iter_p1_init(txn, itr, DB_NODE_IDX, type,  b4)
#define lg_edges_type(txn, itr, type, b4)               lg_iter_p1_init(txn, itr, DB_EDGE_IDX, type,      b4)
#define lg_node_edges_in(txn, itr, tgt, b4)             lg_iter_p1_init(txn, itr, DB_TGTNODE_IDX, tgt,       b4)
#define lg_node_edges_out(txn, itr, src, b4)            lg_iter_p1_init(txn, itr, DB_SRCNODE_IDX, src,       b4)
#define lg_edges_type_val(txn, itr, type, val, b4)      lg_iter_p2_init(txn, itr, DB_EDGE_IDX, type, val, b4)
#define lg_node_edges_type_in(txn, itr, tgt, type, b4)  lg_iter_p2_init(txn, itr, DB_TGTNODE_IDX, tgt, type, b4)
#define lg_node_edges_type_out(txn, itr, src, type, b4) lg_iter_p2_init(txn, itr, DB_SRCNODE_IDX, src, type, b4)
// #define  lg_node_edges(txn, iter)
// #define  lg_node_edges_dir(txn, iter)
// #define  lg_node_edges_type(txn, iter)
// #define  lg_node_edges_type_dir(txn, iter)

int     lg_node_hydrate(LG_txn* txn, LG_Node* ref, LG_log b4, int ro);
int     lg_edge_hydrate(LG_txn* txn, LG_Edge* ref, LG_log b4, int ro);
int     lg_prop_hydrate(LG_txn* txn, LG_Prop* ref, LG_log b4, int ro);
int     lg_blob_hydrate(LG_txn* txn, LG_Blob* ref, int ro);

LG_node lg_node_resolve(LG_txn* txn, LG_id type, LG_id val,                            LG_log b4, int ro);
LG_edge lg_edge_resolve(LG_txn* txn, LG_node src, LG_node tgt, LG_id type, LG_id val,  LG_log b4, int ro);
LG_prop lg_prop_resolve(LG_txn* txn, LG_id pid, LG_id key, LG_id val,                  LG_log b4, int ro);
LG_blob lg_blob_resolve(LG_txn* txn, void const *data, const size_t size,                         int ro);

int     lg_log_read(LG_txn* txn, const LG_log id, buffer_t* val);
int     lg_node_read(LG_txn* txn, const LG_node id, LG_Node* ref);
int     lg_edge_read(LG_txn* txn, const LG_edge id, LG_Edge* ref);
int     lg_prop_read(LG_txn* txn, const LG_prop id, LG_Prop* ref);
int     lg_blob_read(LG_txn* txn, const LG_blob id, LG_Blob* ref);


// iterators

// LG_iter* lg_iter_alloc();
LG_iter* lg_iter_new(    LG_txn* txn,                int dbi, void *pfx, size_t pfxlen, LG_log b4);
LG_iter* lg_iter_p1_new( LG_txn* txn,                int dbi, LG_id id,                 LG_log b4);
int      lg_iter_init(   LG_txn* txn, LG_iter* iter, int dbi, void *pfx, size_t pfxlen, LG_log b4);
int      lg_iter_p1_init(LG_txn* txn, LG_iter* iter, int dbi, LG_id id,                 LG_log b4);
int      lg_iter_p2_init(LG_txn* txn, LG_iter* iter, int dbi, LG_id id, LG_id id2,      LG_log b4);
void     lg_iter_close(LG_iter* iter);
LG_id    lg_iter_next(LG_iter* iter);
LG_iter* lg_iter_concat(unsigned int count, ...);

#define LG_FOR_EACH(it, VNAME, CODE) \
{ \
    LG_log VNAME; \
    while (VNAME = lg_iter_next(it)) { \
        CODE \
    } \
    lg_iter_close(it); \
}


int lg_graph_open(LG_graph* g, const char * const path, const int flags, const int mode, const int db_flags);

int     lg_txn_begin(LG_txn* txn, LG_graph* g, unsigned int flags);
int     lg_txn_begin_child(LG_txn* txn, LG_graph* g, LG_txn* parent, unsigned int flags);
int     lg_txn_commit(LG_txn* txn);
void    lg_txn_abort(LG_txn* txn);
//TODO: int     lg_txn_reset(LG_txn* txn)

#define LG_WRITE(graph, CODE) \
do { \
    LG_txn txn_d; \
    LG_txn* txn = &txn_d; \
    lg_txn_begin(txn, graph, 0); \
    CODE \
    LG_COMMIT \
} while (0);

#define LG_READ(graph, CODE) \
do { \
    LG_txn txn_d; \
    LG_txn* txn = &txn_d; \
    lg_txn_begin(txn, graph, DB_RDONLY); \
    CODE \
    LG_ABORT \
} while (0);

#define LG_COMMIT \
    lg_txn_commit(txn); \
    break;
#define LG_ABORT \
    lg_txn_abort(txn); \
    break;

#define log_next_() lg_log_next(txn)
#define log_last_() lg_log_last(txn)

/*
    ZZZ: graph_log_nextID
*/
#define lg_log_next(txn) \
    ggdb_next_id(txn, 0, &txn->next_logID, DB_LOG, 1)

#define lg_log_last(txn) \
    (lg_log_next(txn) - 1)


// #define gglog_consume() \
//     ggdb_next_id(txn, 1, &txn->next_logID, DB_LOG, 1)

    // #define ggWRITE(CODE) \
    // do { \
    //     LG_txn* txn = lg_txn_begin(graph, NULL, 0); \
    //     ggtxn_push(txn);
    //     CODE \
    // } while (0);

    // #define gCOMMIT() \
    //     lg_txn_commit(txn); \
    //     ggtxn_pop(txn); \
    //     break;

    // #define ggABORT()

    /*

    #define $kv()

    #define $with_txn(graph, flags, CODE) { \
        graph_t $graph
        g_txn_t txn = g_txn_begin(graph, NULL, flags); \
        CODE \
    }

    */

#define GG_PRINT_ERRNO() fprintf(stderr, "lg error: %s\n", db_strerror(errno));



// ============================================================================
#ifndef LG_IMPLEMENTATION

extern ggctx_t ggctx;

#include "./kv.h"
#include "./serdes.h"
#include "./msgpack.h"


// ============================================================================
#else

ggctx_t ggctx;

#include <unistd.h>
    // getpid
    // pid_t is int32, uid_t and gid_t are uint32

#define INCLUDE_SOURCE
#include "c4/fs.h"

#include "./serdes.h"
#include "./msgpack.h"
#include "./db.c"
#include "./kv.h"
#include "./lg_internal.c"

static INLINE int       _lg_init_basepath(const char *argv0);
static INLINE LG_log    _lg_log_append(LG_txn* txn, buffer_t* log_buf, LG_log delID);
static INLINE void      __lg_log_append_deletion(LG_txn* txn, const LG_log newrecID, const LG_log oldrecID, uint8_t *mem);
static INLINE LG_log    _lg_iter_next_id(LG_iter* iter);
static INLINE LG_log    __lg_iter_blarf_id(LG_iter* iter);
typedef enum
ggerr_t {
    // https://github.com/criptych/physfs/blob/1646de2459cab42fb9e2f06a70b50148e80b3f4c/src/physfs.h#L3395
    GGERR_OK,               /**< Success; no error.                    */
    GGERROR,
    GGERR_OUT_OF_MEMORY,
    GGERR_INVALID_ARG,
    GGERR_INVALID_BASEPATH
} ggerr_t;

// PHYSFS_setErrorCode https://github.com/criptych/physfs/blob/1646de2459cab42fb9e2f06a70b50148e80b3f4c/src/physfs.c#L756

void _lg_set_errcode(ggerr_t err)
{
    // TODO:
    GG_PRINT_ERRNO()
}

#define GG_BAIL(errcode, ret) \
{ \
        _lg_set_errcode(errcode); \
        return ret; \
}

#define GG_BAIL_IF(cond, errcode, ret) \
    if (cond) GG_BAIL((errcode), (ret))

#define GG_PATH_SEP '/'

int
lg_init(const char* path)
{
    const LG_graph* graph;
    LG_txn txn;
    // gg = lg_malloc(sizeof(lg_t));
    // GG_BAIL_IF(
    //     gg == NULL, GGERR_OUT_OF_MEMORY, GGERR_OUT_OF_MEMORY)
    GG_BAIL_IF(
        _lg_init_basepath(path)
    , GGERR_INVALID_BASEPATH, GGERR_INVALID_BASEPATH)
    // open root graph
    {
        graph = &ggctx.root;
        const int os_flags = O_RDWR | O_CREAT;
        const int mode = 0760;
        const int db_flags = 0;
        lg_graph_open(graph, path, os_flags, mode, db_flags);
        if (graph == NULL) {
            GG_PRINT_ERRNO()
            return 1;
        }
    }
    // setup & clear kv for current pid
    int pid = getpid();
    ggctx.pid = pid;
    // LG_WRITE(graph,
    //     LG_kv proc_kv;
    //     lg_kv_init(&proc_kv, &txn, str_("proc"), 0);

    //     /*
    //     ggkv_key_str(&proc_kv, "len");
    //     // pod_t val;
    //     // ggkv_get(&proc_kv, &key, &val);
    //     int len = ggkv_get_i32(&proc_kv, &key);
    //     while (len--) {
    //         ggkv_key_strf("%i/pid",len);
    //         int pid = ggkv_get_i32(&proc_kv);
    //         ggkv_key_strf("%i/path_crc",len);
    //         int path_crc = ggkv_get_i32(&path_crc);
    //         if (path_crc != pid_get_path_crc) {
    //             break;
    //         }
    //     }

    //     // kv_clear(&proc_kv);
    //     // kv_deref this frees
    //     */
    //     // lg_txn_commit(&txn);
    // )
    return 0;
}

static int
_lg_init_basepath(const char *argv0)
{
    char* path = NULL;

    // /* Give the platform layer first shot at this. */
    // path = __PHYSFS_platformCalcBaseDir(argv0);
    // if (path != NULL)
    //     return path;

    path = ggctx.basepath;
    char* dirsep = NULL;

    GG_BAIL_IF(
        argv0 == NULL, GGERR_INVALID_ARG, GGERR_INVALID_ARG)

    dirsep = strrchr(argv0, GG_PATH_SEP);
    if (dirsep != NULL)
    {
        const size_t size = ((size_t) (dirsep - argv0)) + 1;
        // path = (char *) lg_malloc(size + 1); // GG_BAIL_IF(!path, GGERR_OUT_OF_MEMORY, 1);
        memcpy(path, argv0, size);
        path[size] = '\0';
        mkdirp(path);
        ggctx.basepath_len = size;
        return 0;
    } /* if */

    /* argv0 wasn't helpful. */
    GG_BAIL(GGERR_INVALID_ARG, GGERR_INVALID_ARG)
}


int
lg_open(LG_graph* g, const char * const subpath, const int flags)
/*
    TODO:
        * ensure graph subpath only opened once?
        * mkdirpat ?
*/
{
    // ++ggctx.graphi;
    // LG_graph g = ggctx.graphs[ggctx.graphi];
    char *path = ggctx.basepath;
    const int basepath_len = ggctx.basepath_len;
    char *dirsep = NULL;

    // reset basepath
    path[basepath_len] = '\0';

    // check if subpath contains directory, if so mkdrip under basepath
    dirsep = strrchr(subpath, GG_PATH_SEP);
    if (dirsep != NULL) {
        const size_t size = ((size_t) (dirsep - subpath)) + 1;
        memcpy(path + basepath_len, subpath, size);
        mkdirp(path);
        path[basepath_len] = '\0';
    }

    // open graph at subpath
    strcat(path, subpath);

    const int os_flags = O_RDWR | O_CREAT;
    const int mode = 0760;
    const int db_flags = 0;
    GG_BAIL_IF(
        lg_graph_open(g, path, os_flags, mode, db_flags)
    , GGERROR, errno);
    return 0;
}

void
lg_close(LG_graph* g)
{
    if (g)
		db_close((db_t)g);
}

int
lg_rm(LG_graph* g)
{
    if (!g)
        return -1;
    char* _path;
    int r;
    db_get_path((db_t)g, &_path);
    const int len = strlen(_path) + 5 + 1;
    char path[len];
    strcpy(path, _path);
    r = rm(path);
    if UNLIKELY(r)
        return r;
    strcat(path, "-lock");
    db_close((db_t)g);
    r = rm(path);
    return r;
}

// void* ggpool_next(ggpoot_t pool)
// {}

// LG_graph* _ggraph_next()
// {
//     // ggctx.graphs// }


int
lg_graph_open(LG_graph* g, const char * const path, const int os_flags, const int mode, const int db_flags)
{
    int r;
    // fixme? padsize hardcoded to 1gb
    // TODO: explicitly disable DB_WRITEMAP - graph_txn_reset current depends on nested write txns
    r = db_init((db_t)g, path, os_flags, mode,
        db_flags // & ~DB_WRITEMAP
        , DBS, DB_INFO, 1<<30);
    if (r) {
        // free(g);
        // g = NULL;
        errno = r;
    }
    return r;
}

/*
    ZZZ: _entry_delete
*/
LG_log
lg_delete(LG_txn* txn, LG_log id)
{
    uint8_t log_data[1 + esizeof(id)];
	buffer_t log = { 0, log_data };
	log_data[log.size++] = GRAPH_DELETION;
	encode(id, log_data, log.size);
	return _lg_log_append(txn, &log, id);
}

LG_node
lg_node_resolve(LG_txn* txn, LG_id type, LG_id val,
    LG_log b4, int ro)
{
    LG_Node rec = {.type=type, .val=val};
    lg_node_hydrate(txn, &rec, b4, ro);
    return rec.id;
}

LG_edge
lg_edge_resolve(LG_txn* txn, LG_node src, LG_node tgt, LG_id type, LG_id val,
    LG_log b4, int ro)
{
    LG_Edge rec = {.type=type, .val=val, .src=src, .tgt=tgt};
    lg_edge_hydrate(txn, &rec, b4, ro);
    return rec.id;
}

LG_prop
lg_prop_resolve(LG_txn* txn, LG_id pid, LG_id key, LG_id val,
    LG_log b4, int ro)
{
    LG_Prop rec = {.pid=pid, .key=key, .val=val};
    lg_prop_hydrate(txn, &rec, b4, ro);
    return rec.id;
}

LG_blob
lg_blob_resolve(LG_txn* txn, void const *data, const size_t size, int ro)
{
    LG_blob id;
    ggdb_resolve_blob(txn, &id, data, size,
        ro, &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
    return id;
}

int
lg_blob_hydrate(LG_txn* txn, LG_Blob* ref, int ro)
{
    const int found = ggdb_resolve_blob(txn, &(ref->id), ref->data, ref->size,
        ro, &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
    return !found;
}

int
lg_node_hydrate(LG_txn* txn, LG_Node* rec, LG_log b4, int ro)
{
    rec->rectype = GRAPH_NODE;
    // if (_node_lookup(...) || readonly) return ;
    {
        ro = ro || b4 || TXN_RO(txn);
        uint8_t kbuf[esizeof(rec->type) + esizeof(rec->val) + esizeof(rec->id)];
        size_t klen = 0;
        encode(rec->type, kbuf, klen);
        encode(rec->val,  kbuf, klen);
        // NOTE: rec->id = 0 if not found
        _lg_rec_lookup(txn, (ggentry_t*)rec, DB_NODE_IDX, kbuf, klen, b4);
        if (rec->id || ro)
            return !(rec->id);
    }
    rec->next = 0;
	rec->is_new = 1;
    txn->node_delta++;
    // _node_append(txn, rec, rec->id);
    {
        uint8_t log_data[1 + esizeof(rec->next) + esizeof(rec->type) + esizeof(rec->val)];
        buffer_t log = { 0, log_data };
        log_data[log.size++] = rec->rectype;
        encode(rec->next, log_data, log.size);
        encode(rec->type, log_data, log.size);
        encode(rec->val,  log_data, log.size);
        rec->id = _lg_log_append(txn, &log, 0); //rec->id);
    }
    // _node_index(txn, rec);
    {
        uint8_t kbuf[esizeof(rec->type) + esizeof(rec->val) + esizeof(rec->id)];
        buffer_t key = { 0, kbuf };
        buffer_t data = { 0, NULL };
        int r;
        encode(rec->type, kbuf, key.size);
        encode(rec->val,  kbuf, key.size);
        encode(rec->id,   kbuf, key.size);
        r = db_put((txn_t)txn, DB_NODE_IDX, &key, &data, 0);
            assert(DB_SUCCESS == r);
    }
    return !(rec->id);
}

int
lg_edge_hydrate(LG_txn* txn, LG_Edge* rec, LG_log b4, int ro)
{
    rec->rectype = GRAPH_EDGE;
    // if (_edge_lookup(...) || ro) return ;
    {
        ro = ro || b4 || TXN_RO(txn);
        uint8_t kbuf[esizeof(rec->type) + esizeof(rec->val) + esizeof(rec->src) + esizeof(rec->tgt) + esizeof(rec->id)];
        size_t klen = 0;
        encode(rec->type, kbuf, klen);
        encode(rec->val,  kbuf, klen);
        encode(rec->src,  kbuf, klen);
        encode(rec->tgt,  kbuf, klen);
        // NOTE: rec->id = 0 if not found
        _lg_rec_lookup(txn, (ggentry_t*)rec, DB_EDGE_IDX, kbuf, klen, b4);
        if (rec->id || ro)
            return !(rec->id);
    }
    rec->next = 0;
	rec->is_new = 1;
	txn->edge_delta++;
    // _edge_append
    {
        uint8_t log_data[1 + esizeof(rec->next) + esizeof(rec->type) + esizeof(rec->val) + esizeof(rec->src) + esizeof(rec->tgt)];
        buffer_t log = { 0, log_data };
        log_data[log.size++] = rec->rectype;
        encode(rec->next, log_data, log.size);
        encode(rec->type, log_data, log.size);
        encode(rec->val,  log_data, log.size);
        encode(rec->src,  log_data, log.size);
        encode(rec->tgt,  log_data, log.size);
        rec->id = _lg_log_append(txn, &log, 0);
    }
    // _edge_index
    {
        uint8_t kbuf[esizeof(rec->type) + esizeof(rec->val) + esizeof(rec->src) + esizeof(rec->tgt) + esizeof(rec->id)];
        buffer_t key = { 0, kbuf };
        buffer_t data = { 0, NULL };
        int r;
        encode(rec->type, kbuf, key.size);
        encode(rec->val,  kbuf, key.size);
        encode(rec->src,  kbuf, key.size);
        encode(rec->tgt,  kbuf, key.size);
        encode(rec->id,   kbuf, key.size);
        r = db_put((txn_t)txn, DB_EDGE_IDX, &key, &data, 0);
            assert(DB_SUCCESS == r);
        key.size = 0;
        encode(rec->src,  kbuf, key.size);
        encode(rec->type, kbuf, key.size);
        encode(rec->id,   kbuf, key.size);
        r = db_put((txn_t)txn, DB_SRCNODE_IDX, &key, &data, 0);
            assert(DB_SUCCESS == r);
        key.size = 0;
        encode(rec->tgt,  kbuf, key.size);
        encode(rec->type, kbuf, key.size);
        encode(rec->id,   kbuf, key.size);
        r = db_put((txn_t)txn, DB_TGTNODE_IDX, &key, &data, 0);
            assert(DB_SUCCESS == r);
    }
    return !(rec->id);
}

int
lg_prop_hydrate(LG_txn* txn, LG_Prop* rec, LG_log b4, int ro)
{
    rec->rectype = GRAPH_PROP;
    // if ((_prop_lookup(txn, e, b4) && val == rec->val) || readonly)
    {
        ro = ro || b4 || TXN_RO(txn);
        uint8_t* logbuf;
        uint8_t kbuf[esizeof(rec->pid) + esizeof(rec->key) + esizeof(rec->id)];
        size_t klen = 0;
        encode(rec->pid, kbuf, klen);
        encode(rec->key, kbuf, klen);
        // NOTE: rec->id = 0 if not found
        logbuf = _lg_rec_lookup(txn, (ggentry_t*)rec, DB_PROP_IDX, kbuf, klen, b4);
        if (logbuf != NULL) { // && rec->id
            klen = 0;
            klen += enclen(logbuf, klen); // skip pid
            klen += enclen(logbuf, klen); // skip key
            if (ro) { // if readonly then write val into rec
                decode(rec->val, logbuf, klen); // write current value
                return !(rec->id);
            }
            else { // else check old val == rec val
                LG_str val;
                decode(val, logbuf, klen); // pull current value
                if (val == rec->val)
                    return !(rec->id);
            }
        }
        else if (ro)
            return !(rec->id);
    }
    rec->next = 0;
	rec->is_new = 1;
	// prop_delta++
    // _prop_append
    {
        uint8_t log_data[1 + esizeof(rec->next) + esizeof(rec->pid) + esizeof(rec->key) + esizeof(rec->val)];
        buffer_t log = { 0, log_data };
        log_data[log.size++] = rec->rectype;
        encode(rec->next, log_data, log.size);
        encode(rec->pid,  log_data, log.size);
        encode(rec->key,  log_data, log.size);
        encode(rec->val,  log_data, log.size);
        rec->id = _lg_log_append(txn, &log, rec->id); // NOTE: ->id
    }
    // _prop_index
    {
        uint8_t kbuf[esizeof(rec->pid) + esizeof(rec->key) + esizeof(rec->id)];
        buffer_t key = { 0, kbuf };
        buffer_t data = { 0, NULL };
        int r;
        encode(rec->pid, kbuf, key.size);
        encode(rec->key, kbuf, key.size);
        encode(rec->id,  kbuf, key.size);
        r = db_put((txn_t)txn, DB_PROP_IDX, &key, &data, 0);
            assert(DB_SUCCESS == r);
    }
    return !(rec->id);
}

/*
    ZZZ: _log_append
*/
static INLINE LG_log
_lg_log_append(LG_txn* txn, buffer_t* log_buf, LG_log delID)
{
    LG_log id;
	uint8_t kbuf[esizeof(id)];
	buffer_t key = { 0, kbuf };
    int r;
	id = ggdb_next_id(txn, 1, &txn->next_logID, DB_LOG, 1);
	if (delID) {
		uint8_t tmp[LOG_MAX_BUF_SIZE];
		__lg_log_append_deletion(txn, id, delID, tmp);
	}
	encode(id, kbuf, key.size);
	r = db_put((txn_t)txn, DB_LOG, &key, log_buf, DB_APPEND);
	if UNLIKELY(DB_SUCCESS != r) {
		fprintf(stderr, "err: _lg_log_append: %s\n", db_strerror(r));
        assert(DB_SUCCESS == r); // ugh
    }
	return id;
}

/*
    ZZZ: _delete
*/
static INLINE void
__lg_log_append_deletion(LG_txn* txn, const LG_log newrecID, const LG_log oldrecID, uint8_t *mem)
{
	uint8_t kbuf[esizeof(newrecID)];
	buffer_t key = { 0, kbuf };
    buffer_t olddata;
    buffer_t newdata = { 1, mem };
    int tail;
    int tlen;
    int r;
	// update existing log entry - first fetch current
	encode(oldrecID, kbuf, key.size);
	r = db_get((txn_t)txn, DB_LOG, &key, &olddata);
	assert(DB_SUCCESS == r);
	// copy rectype (size already set to 1)
	const uint8_t rectype = *mem = *(uint8_t *)olddata.data;
	// fill in new nextID
	encode(newrecID, mem, newdata.size);
	// append remainder of original record
	tail = 1 + enclen(olddata.data, 1);
	tlen = olddata.size - tail;
	memcpy(&mem[newdata.size], &((uint8_t *)olddata.data)[tail], tlen);
	newdata.size += tlen;
	// store
	r = db_put((txn_t)txn, DB_LOG, &key, &newdata, 0);
	assert(DB_SUCCESS == r);
	// recursively delete item properties, and edges if item is a node
    LG_iter* iter;
	if (GRAPH_NODE == rectype) {
        /*
        TODO: malloc-free solution ?
            ...
            ggiter iters[3];
            _lg_entry_iter(txn, &iters[0], DB_PROP_IDX, oldrecID, 0);
            ...
        */
		iter = lg_iter_concat(3,
			lg_iter_p1_new(txn, DB_PROP_IDX, oldrecID, 0),
			lg_iter_p1_new(txn, DB_SRCNODE_IDX, oldrecID, 0),
			lg_iter_p1_new(txn, DB_TGTNODE_IDX, oldrecID, 0)
        );
		txn->node_delta--;
	} else {
		if (GRAPH_EDGE == rectype)
			txn->edge_delta--;
		iter = lg_iter_p1_new(txn, DB_PROP_IDX, oldrecID, 0);
	}
    // recurse children deletion
    LG_id child_id;
	while ( (child_id = lg_iter_next(iter)) ) { // while ( (child = graph_iter_next(iter)) ) {
		__lg_log_append_deletion(txn, newrecID, child_id, mem);
	}
	lg_iter_close(iter);
}

LG_size
lg_nodes_count(LG_txn* txn, LG_log b4)
{
    const LG_log next = lg_log_next(txn);
    if (!b4 || b4 > next)
		b4 = next;
	if UNLIKELY(1 == b4)
		return 0;
	struct txn_info_t info;
    int r = _lg_txn_info_read(txn, &info, b4);
	if (DB_SUCCESS == r)
		return info.nodes;
	// fallback to scanning
    LG_size count = 0;
	LG_iter iter;
    lg_nodes(txn, &iter, b4);
	while (lg_iter_next(&iter))
		count++;
    lg_iter_close(&iter);
    return count;
}

LG_size
lg_edges_count(LG_txn* txn, LG_log b4)
{
    const LG_log next = lg_log_next(txn);
    if (!b4 || b4 > next)
		b4 = next;
	if UNLIKELY(1 == b4)
		return 0;
	struct txn_info_t info;
	int r = _lg_txn_info_read(txn, &info, b4);
	if (DB_SUCCESS == r)
		return info.edges;
	// fallback to scanning
    LG_size count = 0;
	LG_iter iter;
    lg_edges(txn, &iter, b4);
	while (lg_iter_next(&iter))
		count++;
    lg_iter_close(&iter);
    return count;
}

/*
    ZZZ: graph_iter_new
*/
LG_iter*
lg_iter_new(LG_txn* txn, int dbi, void *pfx, size_t pfxlen, LG_log b4)
{
	LG_iter* itr = smalloc(sizeof(*itr));
	if LIKELY(itr) {
		int r = lg_iter_init(txn, itr, dbi, pfx, pfxlen, b4);
		if UNLIKELY(r) {
			free(itr);
			itr = NULL;
		}
	}
	return itr;
}

int
lg_iter_init(LG_txn* txn, LG_iter* iter,
    int dbi, void *pfx, size_t pfxlen, LG_log b4)
{
    int r = txn_iter_init((iter_t)iter, (txn_t)txn, dbi, pfx, pfxlen);
    if LIKELY(DB_SUCCESS == r) {
        iter->beforeID = _cleanse_beforeID(txn, b4);
        iter->txn = txn;
        iter->next = NULL;
        iter->head_active = 1;
    }
    else
        errno = r;
    return r;
}

/*
    ZZZ: _graph_entry_idx
*/
LG_iter*
lg_iter_p1_new(LG_txn* txn,
    int dbi, LG_id id, LG_log b4)
{
	uint8_t pfx[esizeof(id)];
	size_t pfxlen = 0;
	encode(id, pfx, pfxlen);
	return lg_iter_new(txn, dbi, pfx, pfxlen, b4);
}

int
lg_iter_p1_init(LG_txn* txn, LG_iter* iter,
    int dbi, LG_id id, LG_log b4)
{
	uint8_t pfx[esizeof(id)];
	size_t pfxlen = 0;
	encode(id, pfx, pfxlen);
	return lg_iter_init(txn, iter, dbi, pfx, pfxlen, b4);
}

int
lg_iter_p2_init(LG_txn* txn, LG_iter* iter,
    int dbi, LG_id id, LG_id id2, LG_log b4)
{
	uint8_t pfx[esizeof(id) + esizeof(id2)];
	size_t pfxlen = 0;
	encode(id, pfx, pfxlen);
    encode(id2, pfx, pfxlen);
	return lg_iter_init(txn, iter, dbi, pfx, pfxlen, b4);
}

void
lg_iter_close(LG_iter* iter)
{
	while (iter) {
		LG_iter* next = iter->next;
		iter_close((iter_t)iter); // TODO: move to malloc-free internals...
		iter = next;
	}
}

/*
    ZZZ: graph_iter_concat
*/
LG_iter*
lg_iter_concat(unsigned int count, ...)
{
	LG_iter* head = NULL;
    LG_iter* tail = NULL;
	va_list ap;
	va_start(ap, count);
	while (count--) {
		LG_iter* current = va_arg(ap, LG_iter*);
		if UNLIKELY(!current)
			continue;
		if (tail)
			tail->next = current;
		else
			head = tail = current;
		while (tail->next)
			tail = tail->next;
	}
	va_end(ap);
	return head;
}

/*
    ZZZ: graph_iter_next
*/
LG_id
lg_iter_next(LG_iter* iter)
{
    if UNLIKELY(iter == NULL)
        return 0;
    LG_log id;
    while ((id = _lg_iter_next_id(iter))) {
        // ! now we filter out overwritten IDs
        LG_log next;
        buffer_t logbuf;
        if UNLIKELY(lg_log_read(iter->txn, id, &logbuf))
            continue;
        int declen = 1;
        decode(next, logbuf.data, declen);
        if (next == 0 || (iter->beforeID && next >= iter->beforeID))
            return id;
        /*
        ZZZ:
            ggentry_t* e = graph_entry(gi->txn, id);
            if(e->next == 0 || (gi->beforeID && e->next >= gi->beforeID))
                return e;
            free(e);
        */
    }
    return id;
}

/*
    scans index and returns logIDs < beforeID (if beforeID applies)
    ! caller is responsible for filtering out overwritten IDs
    ZZZ: _iter_idx_nextID
*/
static INLINE LG_log
_lg_iter_next_id(LG_iter* iter)
{
	LG_log id = 0;
	if (iter->head_active) {
		// head is still active - try it
		if ((id = __lg_iter_blarf_id(iter)))
			return id;
		// exhaused - deactivate head
		iter->head_active = 0;
		iter->txn = (iter->next) ? iter->next->txn : NULL;
	}
	while (iter->next) {
		if ((id = __lg_iter_blarf_id(iter->next)))
			return id;
		// exhausted - remove chained iterator
		LG_iter* tmp = iter->next;
		iter->next = tmp->next;
		iter_close((iter_t)tmp);
		iter->txn = (iter->next) ? iter->next->txn : NULL;
	}
    return id;
}

static INLINE LG_log
__lg_iter_blarf_id(LG_iter* iter)
{
    LG_log b4 = iter->beforeID;
    iter_t _iter = (iter_t)iter;
    while (iter_next_key(_iter) == DB_SUCCESS) {
        LG_log id;
        // id = _parse_idx_logID(_iter->key.data, _iter->key.size);
        {
            uint8_t*  buf = _iter->key.data;
            size_t buflen = _iter->key.size;
            //
            size_t i = 0, len = 0;
            do {
                i += len;
                len = enclen(buf, i);
            }
            while (i + len < buflen);
            assert(i + len == buflen);
            decode(id, buf, i);
        }
        if (0 == b4 || id < b4)
            return id;
    }
    return 0;
}


/*
    ZZZ: extracted from `graph_entry`
*/
int
lg_log_read(LG_txn* txn, const LG_log id, buffer_t* buf)
{
    // TODO: ? val.size = 0; val->data = NULL;
    if UNLIKELY(!id)
        return 1;
    uint8_t key_data[esizeof(id)];
	buffer_t key = { 0, key_data };
    int r;
    encode(id, key_data, key.size);
    r = db_get((txn_t)txn, DB_LOG, &key, buf);
    return r;
}

int
lg_node_read(LG_txn* txn, const LG_node id, LG_Node* ref)
{
    buffer_t buf;
    // TODO: ref->id = 0;
    int r = lg_log_read(txn, (LG_log)id, &buf);
    if LIKELY(DB_SUCCESS == r) {
        const uint8_t rectype = *((uint8_t *)buf.data);
        if UNLIKELY(GRAPH_NODE != rectype)
            return 1; // TODO: errno wrong rectype
        ref->id = id;
		ref->rectype = rectype;
        int klen = 1;
        decode(ref->next, buf.data, klen);
        decode(ref->type, buf.data, klen);
		decode(ref->val,  buf.data, klen);
    }
    return r;
}

int
lg_edge_read(LG_txn* txn, const LG_edge id, LG_Edge* ref)
{
    buffer_t buf;
    // TODO: ref->id = 0;
    int r = lg_log_read(txn, (LG_log)id, &buf);
    if LIKELY(DB_SUCCESS == r) {
        const uint8_t rectype = *(uint8_t *)buf.data;
        if UNLIKELY(GRAPH_EDGE != rectype)
            return 1; // TODO: errno wrong rectype
        ref->id = id;
		ref->rectype = rectype;
        int klen = 1;
        decode(ref->next, buf.data, klen);
        decode(ref->type, buf.data, klen);
        decode(ref->val,  buf.data, klen);
        decode(ref->src,  buf.data, klen);
        decode(ref->tgt,  buf.data, klen);
    }
    return r;
}

int
lg_prop_read(LG_txn* txn, const LG_prop id, LG_Prop* ref)
{
    buffer_t buf;
    // TODO: ref->id = 0;
    int r = lg_log_read(txn, (LG_log)id, &buf);
    if LIKELY(DB_SUCCESS == r) {
        const uint8_t rectype = *(uint8_t *)buf.data;
        if UNLIKELY(GRAPH_PROP != rectype)
            return 1; // TODO: errno wrong rectype
        ref->id = id;
		ref->rectype = rectype;
        int klen = 1;
        decode(ref->next, buf.data, klen);
        decode(ref->pid,  buf.data, klen);
        decode(ref->key,  buf.data, klen);
        decode(ref->val,  buf.data, klen);
    }
    return r;
}

int
lg_blob_read(LG_txn* txn, const LG_blob id, LG_Blob* ref)
{
    if UNLIKELY(!id) {
        ref->id = 0;
        return 1;
    }
    buffer_t key = { sizeof(id), &id };
    int r = db_get((txn_t)txn, DB_SCALAR, &key, &(ref->buffer));
    if LIKELY(DB_SUCCESS == r)
        ref->id = id;
    return r;
}

/*
ggentry_t* graph_entry(graph_txn_t txn, const LG_log id){
	static const int recsizes[] = {
		[GRAPH_DELETION] = sizeof(ggentry_t),
		[GRAPH_NODE]     = sizeof(ggnode_t),
		[GRAPH_EDGE]     = sizeof(ggedge_t),
		[GRAPH_PROP]     = sizeof(ggprop_t),
	};
	uint8_t buf[esizeof(id)];
	buffer_t key = { 0, buf };
    buffer_t data;
	ggentry_t* e = NULL;
	int r;
	encode(id, buf, key.size);
	r = db_get((txn_t)txn, DB_LOG, &key, &data);
	if (DB_SUCCESS == r) {
		const uint8_t rectype = *(uint8_t *)data.data;
		assert(rectype < sizeof(recsizes) / sizeof(*recsizes));
		int klen = 1;
		e = smalloc(recsizes[rectype]);
		e->id = id;
		e->rectype = rectype;
		decode(e->next, data.data, klen);
		switch(rectype){
			case GRAPH_NODE:
				decode(((node_t)e)->type, data.data, klen);
				decode(((node_t)e)->val,  data.data, klen);
				break;
			case GRAPH_EDGE:
				decode(((edge_t)e)->type, data.data, klen);
				decode(((edge_t)e)->val,  data.data, klen);
				decode(((edge_t)e)->src,  data.data, klen);
				decode(((edge_t)e)->tgt,  data.data, klen);
				break;
			case GRAPH_PROP:
				decode(((prop_t)e)->pid,  data.data, klen);
				decode(((prop_t)e)->key,  data.data, klen);
				decode(((prop_t)e)->val,  data.data, klen);
				break;
		}
	}
	return e;
}
*/


#endif // ifdef LG_IMPLEMENTATION
#endif