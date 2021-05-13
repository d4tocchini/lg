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

#define COUNT 1000000

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

int main(int argc, const char *argv[]) {
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
        LG_READ(&g,
            BENCH_START("nodes iterate")
            LG_iter it;
            nodes_(&it);
            int i = 0; while (lg_iter_next(&it)) {
                ++i;
            }
            lg_iter_close(&it);
            BENCH_FINISH("nodes iterate")
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
        LG_READ(&g,
            BENCH_START("kv_iterate x 1M")
            LG_kv kv;
            lg_kv_init(&kv, txn, str_("my-kv"), 0);
            LG_kv_iter it;
            LG_buf val;
            int i = 0; LG_KV_EACH(&kv, &it,
                ++i;
            )
            BENCH_FINISH("kv_iterate")
        )
        lg_rm(&g);
    }
    return 0;
}

