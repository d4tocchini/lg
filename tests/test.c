#include "lemongraph.h"


// #define SUITE__BEFORE   puts("s0\n");
// #define SUITE__AFTER    puts("s1\n");
#include "csuite/test.h"
#include <assert.h>

#define DATA_NODE_COUNT 3
#define DATA_PROP_COUNT 3
#define DATA_EDGE_COUNT 2
#define DATA_LOG_COUNT DATA_NODE_COUNT+DATA_PROP_COUNT+DATA_EDGE_COUNT

LG_node n_data[3];
LG_edge e_data[2];

void load_data(ggtxn_t* txn)
{
    LG_node* n = n_data;
    LG_edge* e = e_data;
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
        TEST__(lg_serdes_uint,
            LG_buf buf = LG_UINT_BUF;
            LG_ser ser;
            LG_des des;
            LG_uint x = 0;
            lg_ser_init(&ser, &buf);
            lg_ser_uint(&ser, x);
            lg_des_init(&des, &ser);
            EQ__(x, lg_des_uint(&des));
            int n = 63;
            int i = 0;
            while (i < n) {
                x = 1ULL << i;
                lg_ser_init(&ser, &buf);
                lg_ser_uint(&ser, x);
                lg_des_init(&des, &ser);
                EQ__(1ULL << i, lg_des_uint(&des));
                ++i;
            }
            i = 0;
            while (i < n) {
                x = 2ULL << i;
                lg_ser_reset(&ser);
                lg_ser_uint(&ser, x);
                lg_des_init(&des, &ser);
                EQ__(2ULL << i, lg_des_uint(&des));
                ++i;
            }
        )
        TEST__(lg_serdes_n_uint,
            LG_buf buf = LG_UINT_N_BUF(3);
            LG_ser ser;
            LG_des des;
            LG_uint x1 = 1;
            LG_uint x2 = 1 << 16;
            LG_uint x3 = 1ULL << 48;
            lg_ser_init(&ser, &buf);
            lg_ser_uint(&ser, x1);
            lg_ser_uint(&ser, x2);
            lg_ser_uint(&ser, x3);
            lg_des_init(&des, &ser);
            EQ__(x1, lg_des_uint(&des));
            EQ__(x2, lg_des_uint(&des));
            EQ__(x3, lg_des_uint(&des));
        )
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
        TEST__( test_load_and_count,
        {
            LG_WRITE(&g,
                EQ__(log_next_(), 1);
                EQ__(nodes_count_(), 0);
                EQ__(edges_count_(), 0);
                load_data(txn);
                EQ__(nodes_count_(), DATA_NODE_COUNT);
                EQ__(edges_count_(), DATA_EDGE_COUNT);
                EQ__(log_next_(), 1+DATA_LOG_COUNT);
            )
            LG_WRITE(&g,
                EQ__(log_next_(), 1+DATA_LOG_COUNT);
                EQ__(nodes_count_(), DATA_NODE_COUNT);
                EQ__(edges_count_b4_(0), DATA_EDGE_COUNT);
                load_data(txn);
                EQ__(nodes_count_(), DATA_NODE_COUNT);
                EQ__(edges_count_(), DATA_EDGE_COUNT);
                EQ__(log_next_(), 1+DATA_LOG_COUNT);
            )
            LG_WRITE(&g,
                LG_node n[2];
                n[0] = node_(str_("xxx"), str_("yyy"));
                n[1] = node_(str_("xxx"), str_("zzz"));
                edge_(n[0], n[1], str_("edge"),  str_("e1"));
                edge_(n[1], n[2], str_("edge2"), str_("e2"));
                EQ__(nodes_count_(), 2+DATA_NODE_COUNT);
                EQ__(edges_count_(), 2+DATA_EDGE_COUNT);
            )
            LG_READ(&g,
                EQ__(nodes_count_(), 2+DATA_NODE_COUNT);
                EQ__(edges_count_(), 2+DATA_EDGE_COUNT);
            )
        })
        TEST__( test_counts,
        {
            LG_node n1;
            LG_node n2;
            LG_WRITE(&g,
                EQ__(nodes_count_(), 0);
                EQ__(edges_count_(), 0);
                n1 = node_(str_("foo"), str_("bar"));
                EQ__(nodes_count_(), 1);
                n2 = node_(str_("foo"), str_("baz"));
                EQ__(nodes_count_(), 2);
                EQ__(nodes_count_b4_(n2), 1);
                edge_(n1, n2, str_("foo"), 0);
                EQ__(edges_count_(), 1);
            )
            LG_WRITE(&g,
                EQ__(nodes_count_b4_(n2), 1);
                node_(str_("foo"), str_("blah"));
                EQ__(nodes_count_(), 3);
                n1 = node_(str_("foo"), str_("bar"));
                delete_(n1);
                EQ__(nodes_count_(), 2);
                EQ__(edges_count_(), 0);
            )
            LG_READ(&g,
                EQ__(nodes_count_(), 2);
                EQ__(edges_count_(), 0);
                EQ__(nodes_count_b4_(log_last_()), 3);
            )
        })
        TEST__( test_node_edges_by_type,
        {
            LG_WRITE(&g,
                int all_edges = 0;
                int type0_edges = 0;
                int type1_edges = 0;

                load_data(txn);
                LG_node* n = n_data;
                LG_edge* e = e_data;
                EQ__(n[0], 1);

                LG_Iter it;
                // all_edges = sum(1 for x in n1.iterlinks())
                node_edges_in_(&it, n[1]);
                    while (lg_iter_next(&it)) {++all_edges;}
                    lg_iter_close(&it);
                node_edges_out_(&it, n[1]);
                    while (lg_iter_next(&it)) {++all_edges;}
                    lg_iter_close(&it);
                // fewer_edges = sum(1 for x in n1.iterlinks(types=(edge(1)['type'],)))
                LG_Edge edge;
                //type0_edges
                EQ__(edge_read_(e[0], &edge), 0);
                EQ__(edge.type, str_("edge"));
                node_edges_type_in_(&it, n[1], edge.type);
                    while (lg_iter_next(&it)) {++type0_edges;}
                    lg_iter_close(&it);
                node_edges_type_out_(&it, n[1], edge.type);
                    while (lg_iter_next(&it)) {++type0_edges;}
                    lg_iter_close(&it);
                //type1_edges
                EQ__(edge_read_(e[1], &edge), 0);
                EQ__(edge.type, str_("edge2"));
                node_edges_type_in_(&it, n[1], edge.type);
                    while (lg_iter_next(&it)) {++type1_edges;}
                    lg_iter_close(&it);
                node_edges_type_out_(&it, n[1], edge.type);
                    while (lg_iter_next(&it)) {++type1_edges;}
                    lg_iter_close(&it);

                EQ__(all_edges, 2);
                EQ__(type0_edges, 1);
                EQ__(type1_edges, 1);
            )
        })
        TEST__skip( test_edge_dirs,
        {
            LG_WRITE(&g,
                load_data(txn);
            )
            LG_READ(&g,
            /*
                n1 = node(1)
                n1 = txn.node(**n1)
                all_edges = set(e.ID for e in n1.edges)
                out_edges = set(e.ID for e in n1.edges(dir="out"))
                in_edges = set(e.ID for e in n1.edges(dir="in"))
                self.assertTrue(all_edges)
                self.assertTrue(out_edges)
                self.assertTrue(in_edges)
                self.assertTrue(all_edges - in_edges)
                self.assertTrue(all_edges - out_edges)
                self.assertTrue(in_edges.isdisjoint(out_edges))
            */
            )
        })
        TEST__skip( test_query,
        {
            LG_WRITE(&g,
                load_data(txn);
            )
            LG_READ(&g,
            /*
                chains = 0
                for _ in txn.query("n(type='foo')->e()-n()"):
                chains += 1
                self.assertTrue(chains)
            */
            )
        })
        TEST__( test_graph_props,
        {
            LG_WRITE(&g,
                NO__( graph_get_(str_("foo")) );
                OK__( graph_set_(str_("foo"),str_("bar")) );
                OK__( graph_get_(str_("foo")) );
            )
            // TODO: improve ergonomics
            LG_READ(&g,
                LG_prop p = graph_get_(str_("foo"));
                LG_Prop prop;
                prop_read_(p, &prop);
                OK__( prop.val && prop.val == str_("bar") );
            )
            LG_WRITE(&g,
                LG_Prop pw = {
                    .key = str_("boo") _
                    .val = str_("bar")
                };
                Prop_(&pw);
                LG_Prop pr = {.key=str_("boo")};
                Prop_b4_(&pr,0);
                OK__( pw.id && pw.val && pw.id == pr.id && pw.val == pr.val );
            )
        })
        TEST__( test_kv_basic,
        {
            LG_kv kv;
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("my-domain"), 0);
                lg_kv_key(&kv, &(buffer_t){3, "foo"});
                EQ__(0, lg_kv_put(&kv, &(buffer_t){3, "fon"}) );
                buffer_t val;
                EQ__(0, lg_kv_get(&kv, &val) );
                EQ__(3, val.size);
                EQ__(0, memcmp( "fon", val.data, val.size));
            )
            LG_READ(&g,
                lg_kv_init(&kv, txn, str_("my-domain"), 0);
                buffer_t val;
                // NOTE: key is retained...
                EQ__(0, lg_kv_get(&kv, &val) );
                EQ__(3, val.size);
                EQ__(0, memcmp( "fon", val.data, val.size));
            )
            LG_READ(&g,
                lg_kv_init(&kv, txn, str_("my-domain"), 0);
                buffer_t val;
                lg_kv_key(&kv, &(buffer_t){3, "bar"});
                EQ__(0, !lg_kv_get(&kv, &val) );
                lg_kv_key(&kv, &(buffer_t){3, "foo"});
                EQ__(0, lg_kv_get(&kv, &val) );
                EQ__(3, val.size);
                EQ__(0, memcmp( "fon", val.data, val.size));
            )
        })
        TEST__( test_kv_pfx,
        {
            LG_kv kv;
            LG_kv_iter it;
            const int count = 5;
            buffer_t keys[] = {
                {3 _ "fon"} _
                {3 _ "foo"} _
                {6 _ "foobar"} _
                {6 _ "foobaz"} _
                {4 _ "from"}
            };
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("my-domain"), 0);
                // put all
                int i = 0;
                buffer_t val;
                val.size = 4;
                val.data = &i;
                while (i < count) {
                    lg_kv_key(&kv, &keys[i]);
                    lg_kv_put(&kv, &val);
                    ++i;
                }
                // iterate foo*
                i = 1;
                lg_kv_iter_pfx(&kv, &it, &(buffer_t){3, "foo"});
                while (lg_kv_iter_next(&it)) {
                    EQ__(it.key.size, keys[i].size);
                    EQ__(0, memcmp(it.key.data, keys[i].data, keys[i].size));
                    EQ__(0, memcmp(it.val.data, &i, 4));
                    ++i;
                }
                lg_kv_iter_close(&it);
                EQ__(i, count - 1);
                LG_COMMIT
            )
            LG_WRITE(&g,
                int i;
                lg_kv_init(&kv, txn, str_("my-domain"), 0);
                // count all
                i = 0;
                LG_KV_EACH(&kv, &it,
                    ++i;
                )
                EQ__(i, count);
                // count foob*
                i = 0;
                LG_KV_EACH_PFX(&kv, &it, &(buffer_t){4 _ "foob"},
                    ++i;
                )
                EQ__(i, 2);
                // del 2 foob*
                EQ__(2, lg_kv_clear_pfx(&kv, &(buffer_t){4 _ "foob"}) );
                // iterate remaining foo*
                i = 1;
                LG_KV_EACH_PFX(&kv, &it, &(buffer_t){3 _ "foo"},
                    EQ__(0, memcmp(it.key.data, keys[i].data, keys[i].size));
                    ++i;
                )
                EQ__(i, 1+1);
                // del all remaining (3)
                EQ__(3, lg_kv_clear(&kv) );
                // count (0)
                i = 0;
                LG_KV_EACH(&kv, &it,
                    ++i;
                )
                EQ__(i, 0);
            )
        })
        TEST__( test_kv_next,
        {
            LG_kv kv;
            const int count = 3;
            buffer_t key;
            buffer_t val;
            buffer_t keys[] = {
                {1 _ "a"} _
                {1 _ "b"} _
                {1 _ "c"}
            };
            buffer_t vals[] = {
                {1 _ "0"} _
                {1 _ "1"} _
                {1 _ "2"}
            };
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("foo"), 0);
                int i = 0;
                while (i < count) {
                    lg_kv_key(&kv, &keys[i]);
                    lg_kv_put(&kv, &vals[i]);
                    ++i;
                }
            )
            int i = 0;
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("foo"), 0);
                lg_kv_next(&kv, &key, &val);
                assert(lg_buf_eq_safe(&key, &keys[i]));
                assert(lg_buf_eq_safe(&val, &vals[i]));
                ++i;
            )
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("foo"), 0);
                lg_kv_next(&kv, &key, &val);
                assert(lg_buf_eq_safe(&key, &keys[i]));
                assert(lg_buf_eq_safe(&val, &vals[i]));
                ++i;
            )
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("foo"), 0);
                lg_kv_next(&kv, &key, &val);
                assert(lg_buf_eq_safe(&key, &keys[i]));
                assert(lg_buf_eq_safe(&val, &vals[i]));
                ++i;
            )
            i = 0;
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("foo"), 0);
                lg_kv_next(&kv, &key, &val);
                ++i;
                lg_kv_next(&kv, &key, &val);
                assert(lg_buf_eq_safe(&key, &keys[i]));
                assert(lg_buf_eq_safe(&val, &vals[i]));
                ++i;
                assert(!lg_kv_next_reset(&kv));
            )
            i = 0;
            LG_WRITE(&g,
                lg_kv_init(&kv, txn, str_("foo"), 0);
                assert(lg_kv_next(&kv, &key, &val));
                assert(lg_buf_eq_safe(&key, &keys[i]));
                assert(lg_buf_eq_safe(&val, &vals[i]));
            )
        })
        TEST__(test_fifo_buf_api,
        {
            LG_WRITE(&g,
                LG_kv fifo;
                LG_kv* f = &fifo;
                lg_fifo_init(f, txn, str_("foo"), 0);
                SHOULD__("push buf",
                    LG_buf buf = LG_UINT_BUF;
                    LG_ser val;
                    LG_ser* v = &val;
                    lg_ser_init(v, &buf);
                    lg_ser_uint(v, 1);
                    assert(1 == lg_fifo_push(f, v) );
                    lg_ser_init(v, &buf);
                    lg_ser_uint(v, 2);
                    assert(1 == lg_fifo_push(f, v) );
                )
                SHOULD__("push n bufs",
                    LG_buf bufs[2] = {LG_UINT_BUF _ LG_UINT_BUF};
                    LG_ser vals[2];
                    // lg_ser_init_n(vals, bufs, 2);
                    lg_ser_init(&vals[0], &bufs[0]);
                    lg_ser_init(&vals[1], &bufs[1]);
                    lg_ser_uint(&vals[0], 3);
                    lg_ser_uint(&vals[1], 4);
                    assert(2 == lg_fifo_push_n(f, vals, 2) );
                )
                SHOULD__("!empty",
                    LG_uint len;
                    assert(false == lg_fifo_empty(f) );
                    assert(0 == lg_fifo_len(f, &len) );
                    EQ__(4, len);
                )
                SHOULD__("peek buf",
                    LG_buf buf;
                    LG_des des;
                    EQ__(1, lg_fifo_peek(f, &buf) );
                    lg_des_init(&des, &buf);
                    EQ__(1, lg_des_uint(&des) );
                )
                SHOULD__("pop n bufs (peek then del)",
                    LG_buf bufs[10] ;
                    LG_des des;
                    EQ__(lg_fifo_peek_n(f, bufs, 10), 4);
                    lg_des_init(&des, &bufs[0]);
                    assert(1 == lg_des_uint(&des));
                    lg_des_init(&des, &bufs[1]);
                    assert(2 == lg_des_uint(&des));
                    lg_des_init(&des, &bufs[2]);
                    assert(3 == lg_des_uint(&des));
                    lg_des_init(&des, &bufs[3]);
                    assert(4 == lg_des_uint(&des));
                    // del
                    lg_fifo_del(f);
                    lg_fifo_del(f);
                    // len
                    LG_uint len;
                    assert(0 == lg_fifo_len(f, &len) );
                    EQ__(2, len);
                )
                SHOULD__("pop n bufs (peek then del n)",
                    LG_buf bufs[10] ;
                    LG_des des;
                    EQ__(2, lg_fifo_peek_n(f, bufs, 10) );
                    lg_des_init(&des, &bufs[0]);
                    EQ__(3, lg_des_uint(&des) );
                    lg_des_init(&des, &bufs[1]);
                    EQ__(4, lg_des_uint(&des) );
                    // del n
                    assert(2 == lg_fifo_del_n(f, 99) );
                )
                SHOULD__("empty",
                    LG_uint len;
                    assert(true == lg_fifo_empty(f) );
                    assert(0 == lg_fifo_len(f, &len) );
                    assert(0 == len );
                )
            )
        })
        TEST__(test_fifo_uint_api,
        {
            LG_kv fifo;
            LG_kv* f = &fifo;
            LG_WRITE(&g,
                lg_fifo_init(f, txn, str_("foo"), 0);
                SHOULD__("push uints",
                    lg_fifo_push_uint(f, 0);
                    lg_fifo_push_uint(f, 100);
                    lg_fifo_push_uint_n(f, (LG_uint[]){200,300}, 2);
                )
            )
            LG_READ(&g,
                lg_fifo_init(f, txn, str_("foo"), 0);
                SHOULD__("peek uints",
                    assert(false == lg_fifo_empty(f) );
                    LG_uint vals[10];
                    assert(4 == lg_fifo_peek_uint_n(f, &vals[0], 9) );
                    assert(0 == vals[0]);
                    assert(100 == vals[1]);
                    assert(200 == vals[2]);
                    assert(300 == vals[3]);
                )
            )
            LG_WRITE(&g,
                lg_fifo_init(f, txn, str_("foo"), 0);
                SHOULD__("pop uints",
                    LG_uint vals[10];
                    assert(1 == lg_fifo_pop_uint(f, &vals[0]) );
                    assert(3 == lg_fifo_pop_uint_n(f, &vals[1], 9) );
                    assert(0 == vals[0]);
                    assert(100 == vals[1]);
                    assert(200 == vals[2]);
                    assert(300 == vals[3]);
                    assert(true == lg_fifo_empty(f) );
                )
            )
        })
    )
}