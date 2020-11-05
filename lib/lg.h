#ifndef _LG_HEADER
    #define _LG_HEADER

    #include <string.h>
    #include <inttypes.h>
    #include <sys/types.h>
    #include "db.h"

    // data
    typedef uint64_t    d_id_t;
    typedef d_id_t      logID_t;
    typedef d_id_t      strID_t;
    typedef d_id_t      txnID_t;
    #define             d_size_t     size_t
    #define             d_data_t     void*
    typedef struct d_t {
        d_size_t    size;   // size of the data item
        d_data_t    data;   // address of the data item
        d_id_t 	    id;
    } d_t;
    #define d_id(x) 	    ((d_t){ .id=(d_id_t)(x) })
    #define d_buf(x, sz) 	((d_t){ .data=(d_data_t)(x), .size=(d_size_t)(sz) })
    #define _d_str(x)       d_buf(x, strlen(x))
    #define d_str(x)        _d_str((x))


    // graph db
    typedef struct g_t {
        struct db_t db;
    } g_t;


    // graph transaction

    // status codes
    // #define G_NOTFOUND  DB_NOTFOUND
    // #define G_SUCCESS   DB_SUCCESS

    typedef struct gtxn_t {
    // everything after 'txn' is copied to a parent txn on commit success
        struct txn_t txn;
        strID_t next_strID;
        logID_t next_logID;
        logID_t begin_nextID;
        int64_t node_delta;
        int64_t edge_delta;
    // everything from prev_id down may be copied to a parent txn on commit fail/abort
    // (if the parent didn't already have it)
        txnID_t prev_id;
        logID_t prev_start;
        logID_t prev_count;
        uint64_t prev_nodes;
        uint64_t prev_edges;
    } gtxn_t;
    #define TXN_DB(txn) ((txn_t)(txn))->db
    #define TXN_RW(txn) ((txn)->txn.rw)
    #define TXN_RO(txn) ((txn)->txn.ro)
    #define TXN_PARENT(txn) ((gtxn_t*)(((txn_t)(txn))->parent))

    typedef struct giter_t {
        struct iter_t iter;
        gtxn_t* txn;
        logID_t beforeID;
        struct giter_t* next;
        int head_active;
    } giter_t;

    // graph key-val storage

    // kv flags
    #define LG_KV_RO       0x1
    #define LG_KV_MAP_KEYS 0x2
    #define LG_KV_MAP_DATA 0x4

    typedef struct gkv_t {
        gtxn_t* txn;
        buffer_t key, val;
        int flags;
        unsigned int refs, klen;
        uint8_t kbuf[511];
    } gkv_t;

    typedef struct gkv_iter_t {
        struct iter_t iter;
        gkv_t* kv;
    } gkv_iter_t;


    int d_resolve(gtxn_t* txn, d_t* d, int readonly);

    void gkv_init(gtxn_t* txn, gkv_t* kv, d_t domain, const int flags);
    int gkv_get(gkv_t* kv, d_t key, d_t* val);



    /*

    #define $kv()

    #define $with_txn(graph, flags, CODE) { \
        graph_t $graph
        g_txn_t txn = g_txn_begin(graph, NULL, flags); \
        CODE \
    }

    */
#endif

// ============================================================================
#ifdef LG_IMPLEMENTATION


    #define INLINE __attribute__((always_inline)) inline

    #define MAX(x, y) ((x) > (y) ? (x) : (y))

    // max log entry size is for edge_t
    #define MAX_LOGBUF (1 + esizeof(strID_t) * 2 + esizeof(logID_t) * 3)

    //#define debug(args...) do{ fprintf(stderr, "%d: ", __LINE__); fprintf(stderr, args); }while(0)
    //#define debug(args...) while(0);

    // provide type-agnostic clz wrapper, and return a more useful value for clz(0)
    #define __clz_wrapper(x) (unsigned int)((x) ? (sizeof(x) == sizeof(long) ? (unsigned)__builtin_clzl(x) : (sizeof(x) == sizeof(long long) ? (unsigned)__builtin_clzll(x) : (unsigned)__builtin_clz((int)(x)))) : sizeof(x) * 8)

    // quickly take unsigned numeric types and count minimum number of bytes needed to represent - for varint encoding
    #define intbytes(x) (sizeof(x) - __clz_wrapper(x) / 8)

    // encode unsigned values into buffer, advancing iter
    // ensure you have a least 9 bytes per call
    #define encode(x, buffer, iter) do{ \
        int _shift; \
        ((uint8_t *)(buffer))[iter] = intbytes(x); \
        for (_shift = (((uint8_t *)(buffer))[iter++] - 1) * 8; _shift >= 0; iter++, _shift -= 8) \
            ((uint8_t *)(buffer))[iter] = ((x) >> _shift) & 0xff; \
    } while (0)

    // corresponding decode
    #define decode(x, buffer, iter) do{ \
        uint8_t _count = ((uint8_t *)(buffer))[iter++]; \
        assert(sizeof(x) >= _count); \
        x = 0; \
        while(_count--) \
            x = (x<<8) + ((uint8_t *)(buffer))[iter++]; \
    }while(0)

    #define enclen(buffer, offset) (1 + ((uint8_t *)(buffer))[offset])

    #define esizeof(x) (sizeof(x)+1)

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

    // here's the deal - the actual key in the db is comprised of 3 serialized uints:
    //	txnID: incrementing sequence of transaction numbers, starting at 1
    //	       each write txn that caused the log to grow will get it's own txnID
    //	start: first logID in the txn, will be >= 1
    //	count: number of logIDs accumulated in the txn, will be >= 1
    //
    // when this function is called, at least one of the params will be an actual key as above
    // if the txnID for the other decodes to zero, then it is a query operation. Decoding the second uint determines
    //	whether the 3rd uint should be tested against (0) the txnID or (non-zero) the start/count range
    //
    // with one btree, this lets us quickly:
    //	map txnID to logID range
    //	map logID to containing txnID
    static int magic_txnlog_cmp(const buffer_t *a, const buffer_t *b)
    {
        int ia = 0, ib = 0;
        txnID_t ta, tb;
        decode(ta, a->data, ia);
        decode(tb, b->data, ib);
        if(!ta){
            assert(tb);
            // a is query, b is actual key in db
            decode(ta, a->data, ia);
            if(ta){
                decode(ta, a->data, ia);
                uint64_t start, count;
                decode(start, b->data, ib);
                if(ta < start)
                    return -1;
                decode(count, b->data, ib);
                return ta >= (start + count);
            }
            // txnID query
            decode(ta, a->data, ia);
        }else if(!tb){
            // I don't believe this happens today, but just in case ...
            return - magic_txnlog_cmp(b, a);
        }
        return ta < tb ? -1 : ta > tb ? 1 : 0;
    }

    static dbi_t DB_INFO[] = {
        // strID_t strID => bytes (append-only)
        [DB_SCALAR] = { "scalar", DB_INTEGERKEY, NULL },

        // uint32_t crc => strID_t strIDs[]
        [DB_SCALAR_IDX] = { "scalar_idx", DB_DUPSORT|DB_INTEGERKEY|DB_DUPFIXED|DB_INTEGERDUP, NULL },

        // varint_t logID => entry_t (appends & updates)
        [DB_LOG] = { "log", 0, NULL },

        // varint_t [type, val, logID] => ''
        [DB_NODE_IDX] = { "node_idx", 0, NULL },

        // varint_t [type, val, src, tgt, logID]
        [DB_EDGE_IDX] = { "edge_idx", 0, NULL },

        // varint_t pid, key, logID => ''
        [DB_PROP_IDX] = { "prop_idx", 0, NULL },

        // varint_t node, type, edge => ''
        [DB_SRCNODE_IDX] = { "srcnode_idx", 0, NULL },

        // varint_t node, type, edge => ''
        [DB_TGTNODE_IDX] = { "tgtnode_idx", 0, NULL },

        // varint_t domain, key => varint_t val
        [DB_KV] = { "kv", 0, NULL },

        // varint_t domain, key => varint_t val
        [DB_KVBM] = { "kvbm", 0, NULL },

        // varint_t [txnID, start, count] => varint_t [node_count, edge_count] (append only)
        [DB_TXNLOG] = { "txnlog", 0, magic_txnlog_cmp }
    };


    // return 0 on error
    static INLINE uint64_t _nextID(gtxn_t* txn, const int consume, uint64_t * const cache, const int db1, const int venc){
        if(consume && TXN_RO(txn)){
            errno = EINVAL;
            return 0;
        }

        int r;
        unsigned int i;
        uint64_t id = *cache;
        if(0 == id){
            struct cursor_t c;
            buffer_t key;
            r = txn_cursor_init(&c, (txn_t)txn, db1);
            if(DB_SUCCESS == r)
                r = cursor_last_key(&c, &key, NULL, 0);
            cursor_close(&c);
            switch(r){
                case DB_SUCCESS:
                    if(venc){
                        i = 0;
                        decode(id, key.data, i);
                        assert(i == key.size);
                    }else{
                        assert(sizeof(id) == key.size);
                        memcpy(&id, key.data, sizeof(id));
                    }
                    // passthrough
                case DB_NOTFOUND:
                    *cache = ++id;
                    break;
                default:
                    errno = r;
                    return 0;
            }
        }
        if(consume && id && 0 == ++(*cache)){
            *cache = id;
            errno = EOVERFLOW;
            id = 0;
        }
        return id;
    }


    // returns 1 for success, 0 for failure (only for readonly)
    static INLINE int __resolve_blob(gtxn_t* txn, uint64_t *ret, char const *data, const size_t len, const int readonly, uint64_t * const cache, int db1, int db2)
    {
        assert(data);
        int r;
        size_t count;
        uint64_t id;
        uint32_t chk;

        struct cursor_t c, idx;
        buffer_t val, vkey, ival, ikey = { .data = &chk, .size = sizeof(chk) };

        r = txn_cursor_init(&c, (txn_t)txn, db1);
        assert(DB_SUCCESS == r);
        r = txn_cursor_init(&idx, (txn_t)txn, db2);
        assert(DB_SUCCESS == r);

        // fill in checksum
        chk = crc32(0, (void *)data, len);

        int retval = 1;

        r = cursor_get(&idx, &ikey, &ival, DB_SET_KEY);
        if (DB_SUCCESS == r) {
            r = cursor_count(&idx, &count);
            assert(DB_SUCCESS == r);
            while (1) {
                memcpy(&vkey, &ival, sizeof(ival));
                // query main db
                r = cursor_get(&c, &vkey, &val, DB_SET_KEY);
                assert(DB_SUCCESS == r);
                if (val.size == len && memcmp(val.data, data, len) == 0) {
                    assert(sizeof(*ret) == vkey.size);
                    memcpy(ret, vkey.data, sizeof(*ret));
                    goto done;
                }
                if (0 == --count)
                    break;
                r = cursor_get(&idx, &ikey, &vkey, DB_NEXT_DUP);
                assert(DB_SUCCESS == r);
            }
            r = DB_NOTFOUND;
        }
        assert(DB_NOTFOUND == r);

        // no key at all, or no matching strings

        // bail out now for read-only requests
        if (readonly) {
            retval = 0;
            *ret = 0;
            goto done;
        }

        // figure out next ID to use
        *ret = id = _nextID(txn, 1, cache, db1, 0);
        assert(id);

        // store new string in db
        vkey.size = sizeof(id);
        vkey.data = &id;
        val.size = len;
        r = cursor_put(&c, &vkey, &val, DB_APPEND|DB_RESERVE);
        assert(DB_SUCCESS == r);
        memcpy(val.data, data, len);

        // and add index entry
        ikey.data = &chk;
        ikey.size = sizeof(chk);
        assert(&id == vkey.data);
        r = cursor_put(&idx, &ikey, &vkey, DB_APPENDDUP);
        assert(DB_SUCCESS == r);

    done:
        cursor_close(&c);
        cursor_close(&idx);
        return retval;
    }

    int d_resolve(gtxn_t* txn, d_t* d, int readonly)
    {
        // static INLINE int _string_resolve(gtxn_t* txn, strID_t *ret, void const *data, const size_t len, int readonly){
        if (0 < d->id)              // already resolved
            return 1;
        if (NULL == d->data) {      // unresolvable TODO:
            assert(0 == d->size);
            d->id = 0;
            return 1;
        }
        return __resolve_blob(txn, &d->id, d->data, d->size, readonly, &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
    }




    char *__blob(gtxn_t* txn, uint64_t id, size_t *len, int db1){
        assert(id);
        buffer_t key = { sizeof(id), &id }, data;
        int r = db_get((txn_t)txn, db1, &key, &data);
        assert(DB_SUCCESS == r);
        if(len)
            *len = data.size;
        return data.data;
    }
    char *graph_string(gtxn_t* txn, strID_t id, size_t *len){
        return id ? __blob(txn, id, len, DB_SCALAR) : ((*len = 0), NULL);
    }


    static INLINE int _string_resolve(gtxn_t* txn, strID_t *ret, void const *data, const size_t len, int readonly){
        if(NULL == data){
            assert(0 == len);
            *ret = 0;
            return 1;
        }
        return __resolve_blob(txn, ret, data, len, readonly, &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
    }

    int graph_string_lookup(gtxn_t* txn, strID_t *id, void const *data, const size_t len){
        return _string_resolve(txn, id, data, len, 1);
    }

    int graph_string_resolve(gtxn_t* txn, strID_t *id, void const *data, const size_t len){
        return _string_resolve(txn, id, data, len, 0);
    }

    // fetch string by encoded ID
    static INLINE char *graph_string_enc(gtxn_t* txn, void *id_enc, size_t *slen)
    {
        strID_t id;
        int len = 0;
        decode(id, id_enc, len);
        return graph_string(txn, id, slen);
    }

    void gkv_init(gtxn_t* txn, gkv_t* kv, d_t domain, const int flags)
    {
        kv->txn = txn;
        kv->flags = flags;
        kv->refs = 1;
        kv->klen = 0;
        // TODO: docs on gkv as perf-oriented/unsafe
        // if ( !d_resolve(txn, domain, (TXN_RO(txn) || (flags & LG_KV_RO))) )
        //     return 0;
        encode(domain.id, kv->kbuf, kv->klen);
        // return 1;
    }

    static INLINE int _gkv_setup_key(gkv_t* kv, void *key, size_t klen, int readonly)
    {
        strID_t id;
        kv->key.data = kv->kbuf;
        kv->key.size = kv->klen;
        if (kv->flags & LG_KV_MAP_KEYS) {
            if (!_string_resolve(kv->txn, &id, key, klen, readonly))
                return 0;
            encode(id, kv->kbuf, kv->key.size);
        } else {
            assert(klen <= sizeof(kv->kbuf) - kv->klen);
            memcpy(&kv->kbuf[kv->klen], key, klen);
            kv->key.size += klen;
        }
        return 1;
    }

    int gkv_get(gkv_t* kv, d_t key, d_t* val)
    {
        val->id = 0;
        if (!_gkv_setup_key(kv, key.data, key.size, 1))
            goto NOTFOUND;
        if (DB_SUCCESS != db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val))
            goto NOTFOUND;
        if (kv->flags & LG_KV_MAP_DATA) {
            val->data = graph_string_enc(kv->txn, kv->val.data, &(val->size));
        } else {
            val->data = kv->val.data;
            val->size = kv->val.size;
        }
        return DB_SUCCESS;
    NOTFOUND:
        val->size = 0;
        val->data = NULL;
        return DB_NOTFOUND;
    }


#endif // ifdef LG_IMPLEMENTATION