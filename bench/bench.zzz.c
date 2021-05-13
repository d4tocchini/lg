/*
TODO:
        + graph_set_mapsize(g, (1<<30) * 10); // as in bench.py, but not working here...
        + graph_close is seg faulting...

*/
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include "lemongraph.h"
#define	O_RDWR	0x0002
#define	O_CREAT 0x0200
#define CATCH_ERRNO \
    if (errno) {\
        fprintf(stderr, "ERR: %s\n", graph_strerror(errno));\
        exit(-1);\
    }
void bench();
void bench_fast();
int bench_nodes(graph_t);
int bench_nodes_fast(graph_t);
int bench_props(graph_t);
int bench_edges(graph_t);
int commit(graph_txn_t);


#define BENCH_START(name) \
    logID_t logid = graph_log_nextID(txn); \
    printf("* %s\n", name); \
    clock_t _t = clock();

#define BENCH_FINISH(name) \
    double _elapsed = ((double)(clock() - _t))/CLOCKS_PER_SEC; \
    logID_t updates = graph_log_nextID(txn) - logid; \
    printf("    - %s total time: %f sec \n", name, _elapsed); \
    if (updates) { \
        printf("    - %s insert rate: %f / sec \n", name, (updates / _elapsed)); \
        logid += updates; \
        printf("    - commit updates: %lli\n", updates); \
    } \
    else { \
        printf("    - %s op rate: %f / sec \n", name, (COUNT / _elapsed)); \
    }


#include <ftw.h>
#include <unistd.h>
// int rmrf(char *);

#define COUNT 1000000
node_t NODES[COUNT];
logID_t NODE_IDS[COUNT];
char dirtmp[] = "/tmp/bench.XXXXXX";

int main(int argc, const char *argv[]) {

    // legacy...
    // bench();
    // bench_fast();

    lg_init("/tmp/lg_bench/");
    {
        LG_node n[COUNT];
        LG_str n_type;
        LG_str p_key;
        LG_str e_type;
        LG_graph g;
        lg_open(&g, "graph",
            0
            // DB_NOLOCK // single threaded
            // DB_NOMETASYNC
            // DB_WRITEMAP|DB_MAPASYNC
            // DB_NOSYNC|DB_NORDAHEAD
        );
        LG_WRITE(&g,
            BENCH_START("nodes")
            n_type = str_("node");
            int i = 0; while (i < COUNT)
                n[i] = node_(n_type, i++);
            BENCH_FINISH("nodes")
        )
        LG_WRITE(&g,
            BENCH_START("props")
            p_key = str_("prop");
            int i = 0; while (i < COUNT)
                set_(n[i], p_key, i++);
            BENCH_FINISH("props")
        )
        LG_WRITE(&g,
            BENCH_START("edges")
            e_type = str_("edge");
            int i = 0; while (i < COUNT)
                edge_(n[i++], n[i++], e_type, i);
            BENCH_FINISH("edges")
        )
        lg_rm(&g);
    }
    {
        LG_graph g;
        lg_open(&g, "kv",
            0
            // DB_NOLOCK // single threaded
            // DB_NOMETASYNC
            // DB_WRITEMAP|DB_MAPASYNC
            // DB_NOSYNC|DB_NORDAHEAD
        );
        LG_WRITE(&g,
            BENCH_START("kv_put x 1M")
            LG_kv kv;
            lg_kv_init(&kv, txn, str_("my-kv"), 0);
            int i = 0; while (i < COUNT) {
                lg_kv_key(&kv, &(LG_buf){4, &i});
                lg_kv_put(&kv, &(LG_buf){4, &i});
                ++i;
            }
            BENCH_FINISH("kv_put")
        )
        LG_READ(&g,
            BENCH_START("kv_get x 1M")
            LG_kv kv;
            lg_kv_init(&kv, txn, str_("my-kv"), 0);
            LG_buf val;
            int i = 0; while (i < COUNT) {
                lg_kv_key(&kv, &(LG_buf){4, &i});
                lg_kv_get(&kv, &val);
                ++i;
            }
            BENCH_FINISH("kv_get")
        )
        lg_rm(&g);
    }
    return 0;
}

void bench() {
    char name[] = "bench";
    char *dir = mkdtemp(dirtmp);
    char *path = malloc(strlen(dir) + strlen(name) + 2);
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, name);
    printf("==================\n%s\n",path);

    graph_t g = graph_open(path, O_RDWR|O_CREAT, 0760,
        // DB_NOLOCK // single threaded
        // DB_WRITEMAP|DB_MAPASYNC // no child txns
        DB_NOMETASYNC
        //DB_NOTLS // DB_NOSYNC|DB_NORDAHEAD
        // http://www.lmdb.tech/doc/group__internal.html#ga52dd98d0c542378370cd6b712ff961b5
    );
    CATCH_ERRNO

    bench_nodes(g);
    bench_props(g);
    bench_edges(g);

    graph_close(g);

    rmrf(dir);
    free(path);
    int i = 0;
    while (i < COUNT)
        free(NODES[i++]);
}
void bench_fast() {
    char name[] = "bench_fast";
    char *dir = mkdtemp(dirtmp);
    char *path = malloc(strlen(dir) + strlen(name) + 2);
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, name);
    printf("==================\n%s\n",path);

    graph_t g = graph_open(path, O_RDWR|O_CREAT, 0760, DB_NOSYNC|DB_NORDAHEAD);
    CATCH_ERRNO

    bench_nodes_fast(g);
    bench_props_fast(g);
    bench_edges_fast(g);

    graph_close(g);

    rmrf(dir);
    free(path);
}


int bench_nodes(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    CATCH_ERRNO
    BENCH_START("nodes")
    int i = 0; while (i < COUNT)
        NODES[i++] = graph_node_resolve(txn, "node",4, &i,sizeof(int));
    BENCH_FINISH("nodes")
    commit(txn);
    return 0;
}
int bench_props(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    CATCH_ERRNO
    BENCH_START("props")
    int i = 0; while (i < COUNT)
        free(graph_node_set(txn, NODES[i++], "prop",4, &i,sizeof(int)));
    BENCH_FINISH("props")
    commit(txn);
    return 0;
}
int bench_edges(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    CATCH_ERRNO
    BENCH_START("edges")
    int i = 0; while (i < COUNT)
        free(graph_edge_resolve(txn, NODES[i++], NODES[i++], "edge",4, &i,sizeof(int)));
    BENCH_FINISH("edges");
    commit(txn);
    return 0;
}

int bench_nodes_fast(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    CATCH_ERRNO
    BENCH_START("nodes")

    // node_t e = malloc(sizeof(*e));
    // if (!graph_string_resolve(txn, &e->type, "node",4))
        // exit(1);
    strID_t type;
    if (!graph_string_resolve(txn, &type, "node",4))
        exit(1);
    uint64_t i = 0; while (i < COUNT) {
        // e->val = i;
        NODE_IDS[i++] = graph_nodeID_resolve(txn, type, i);
    }
	// free(e);

    BENCH_FINISH("nodes")
    commit(txn);
    return 0;
}
int bench_props_fast(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    CATCH_ERRNO
    BENCH_START("props")

    strID_t key;
    if (!graph_string_resolve(txn, &key, "prop",4))
        exit(1);
    uint64_t i = 0; while (i < COUNT)
        graph_ID_set(txn, NODE_IDS[i++], key, i);

    BENCH_FINISH("props")
    commit(txn);
    return 0;
}
int bench_edges_fast(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    CATCH_ERRNO
    BENCH_START("edges")

    strID_t type;
    if (!graph_string_resolve(txn, &type, "edge",4))
        exit(1);
    uint64_t i = 0; while (i < COUNT)
        graph_edgeID_resolve(txn, NODES[i++], NODES[i++], type, i);

    BENCH_FINISH("edges");
    commit(txn);
    return 0;
}

int commit(graph_txn_t txn) {
    CATCH_ERRNO
    if (graph_txn_updated(txn)) {
        graph_txn_commit(txn);
        return 0;
    }
    graph_txn_abort(txn);
    return 1;
}

// int _rmrf_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
//     int rv = remove(fpath);
//     if (rv) {
//         perror(fpath);
//     }
//     return rv;
// }

// int rmrf(char *path) {
//     return nftw(path, _rmrf_cb, 64, FTW_DEPTH | FTW_PHYS);
// }
