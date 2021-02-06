#include "lemongraph.h"


// #define SUITE__BEFORE   puts("s0\n");
// #define SUITE__AFTER    puts("s1\n");
#include "csuite/test.h"
#include <assert.h>

#define DATA_NODE_COUNT 3
#define DATA_PROP_COUNT 3
#define DATA_EDGE_COUNT 2
#define DATA_LOG_COUNT DATA_NODE_COUNT+DATA_PROP_COUNT+DATA_EDGE_COUNT

void load_data(ggtxn_t* txn)
{
    LG_node n[3];
    LG_edge e[2];
    n[0] = node_(str_("foo"), str_("bar"));
    n[1] = node_(str_("foo"), str_("baz"));
    n[2] = node_(str_("goo"), str_("gaz"));
    set_(n[0], str_("np1k"), str_("np1v"));
    set_(n[1], str_("np2k"), str_("np2v"));
    set_(n[2], str_("np3k"), str_("np3v"));
    e[0] = edge_(n[0], n[1], str_("edge"),  str_("e1"));
    e[1] = edge_(n[1], n[2], str_("edge2"), str_("e2"));
}

// typedef struct
// LG_node_desc {
//     void* type;
//     void* val;
//     uint32_t tlen;
//     uint32_t vlen;
//     LG_prop_desc props[];
// }

// typedef struct
// LG_graph_batch {
//     LG_node_desc nodes[];
//     LG_edge_desc edges[];
//     LG_prop_desc props[];
// } LG_graph_batch;

// void load_batch(ggtxn_t* txn)
// {
//     lg_graph_batch_load(&(LG_graph_batch){
//         .nodes = {
//             [0] = {"foo", "bar", .props={
//                 {"np1k","np1v"}
//             }},
//             [1] = {"foo", "baz", .props={
//                 {"np2k", "np2k"}
//             }},
//             [2] = {"foo", "baz", .props={
//                 {"np3k", "np3k"}
//             }}
//         },
//         .edges = {
//             {0, 1, "edge", "e1"},
//             {1, 2, "edge2", "e2"},
//         }
//     })
// }

// void load_json(void)
// {
//     ggJSON({
//         "nodes": [
//             ["foo","bar",{'np1k': 'np1v'}],
//             ["foo","bar",{'np1k': 'np1v'}],
//             ["foo","bar",{'np1k': 'np1v'}]
//         ],
//         "edges": [
//             [0,1,"edge","e1"],
//             [1,2,"edge2","e2"]
//         ]
//     })
// }

int main(int argc, char** argv)
{
    SUITE__(hello,
        TEST__( world,
            EQ__(1,1)
            // EQ__(1,0)
        )
    )
    SUITE__(lg,
        TEST__(lg_init,
            lg_init(".gg/");
            printf("    basepath = %s\n",ggctx.basepath);
            EQ__(ggctx.basepath_len,4)
        )
    )

    ggraph_t g;

    #undef  TEST__BEFORE
    #define TEST__BEFORE    lg_open(&g, "test", 0);
    #undef  TEST__AFTER
    #define TEST__AFTER     lg_rm(&g);

    SUITE__( graph_suite,
        TEST__( test_open_then_rm,
        {
            // nothing
        })
        TEST__( test_commit,
        {
            LG_node n;
            LG_WRITE(&g,
                EQ__(1,  log_next_() );
                EQ__(1,  n = node_(str_("foo"), str_("bar")) )
                EQ__(2,  log_next_());
                LG_COMMIT
                assert(false);
            )
            LG_READ(&g,
                EQ__(log_next_(), 2);
                LG_ABORT
                assert(false);
            )
        })
        TEST__( test_abort,
        {
            LG_node n;
            LG_WRITE(&g,
                EQ__(log_next_(), 1);
                EQ__(n = node_(str_("foo"), str_("bar")), 1);
                EQ__(log_next_(), 2);
                LG_ABORT
                assert(false);
            )
            LG_READ(&g,
                EQ__(log_next_(), 1);
            )
        })
        TEST__( test_read,
        {
            LG_node n;
            LG_blob typ;
            LG_blob val;
            LG_WRITE(&g,
                typ = str_("foo");
                val = str_("bar");
                EQ__(log_next_(), 1);
                EQ__(n = node_(typ, val), 1);
                EQ__(log_next_(), 2);
            )
            LG_READ(&g,
                // EQ__(log_next_(), 2);
                LG_Node node;
                LG_Blob blob;
                EQ__(str_("foo"), typ);
                EQ__(str_("bar"), val);
                EQ__(node_read_(n, &node), 0);
                EQ__(blob_read_(node.type, &blob), 0);
                EQ__(blob.size, 3);
                EQ__(blob.string[0], 'f');
                EQ__(blob.string[2], 'o');
                EQ__(blob_read_(node.val, &blob), 0);
                EQ__(blob.size, 3);
                EQ__(blob.string[0], 'b');
                EQ__(blob.string[2], 'r');
                EQ__(log_next_(), 2);
            )
        })
        TEST__( load_data,
        {
            LG_WRITE(&g,
                EQ__(log_next_(), 1);
                // EQ__(gg_nodes_count(), 0);
                // EQ__(gg_edges_count(), 0);
                load_data(txn);
                // EQ__(gg_nodes_count(), DATA_NODE_COUNT);
                // EQ__(gg_edges_count(), DATA_EDGE_COUNT);
                EQ__(log_next_(), 1+DATA_LOG_COUNT);
            )
            LG_WRITE(&g,
                EQ__(log_next_(), 1+DATA_LOG_COUNT);
                // EQ__(gg_nodes_count(), 0);
                // EQ__(gg_edges_count(), 0);
                load_data(txn);
                // EQ__(gg_nodes_count(), DATA_NODE_COUNT);
                // EQ__(gg_edges_count(), DATA_EDGE_COUNT);
                EQ__(log_next_(), 1+DATA_LOG_COUNT);
            )
        })
    )
    // // def test_counts(self):
    // //     with self.g.transaction(write=True) as txn:
    // //         self.assertEqual(txn.nodes_count(), 0)
    // //         self.assertEqual(txn.edges_count(), 0)

    // //         n1 = txn.node(type="foo", value="bar")
    // //         self.assertEqual(txn.nodes_count(), 1)

    // //         n2 = txn.node(type="foo", value="baz")
    // //         self.assertEqual(txn.nodes_count(), 2)

    // //         self.assertEqual(txn.nodes_count(beforeID=n2.ID), 1)

    // //         txn.edge(src=n1, tgt=n2, type="foo")
    // //         self.assertEqual(txn.edges_count(), 1)

    // //     with self.g.transaction(write=True) as txn:
    // //         self.assertEqual(txn.nodes_count(beforeID=n2.ID), 1)
    // //         txn.node(type="foo", value="blah")
    // //         self.assertEqual(txn.nodes_count(), 3)
    // //         n1 = txn.node(type="foo", value="bar")
    // //         n1.delete()
    // //         self.assertEqual(txn.nodes_count(), 2)
    // //         self.assertEqual(txn.edges_count(), 0)

    // //     with self.g.transaction(write=False) as txn:
    // //         self.assertEqual(txn.nodes_count(), 2)
    // //         self.assertEqual(txn.edges_count(), 0)
    // //         self.assertEqual(txn.nodes_count(beforeID=txn.lastID), 3)



    // )
}