


#ifndef _LGDB_HEADER
#define _LGDB_HEADER




// LG_id ggdb_next_id(ggtxn_t* txn, const int consume, LG_id * const cache, const int db1, const int venc);
// int pod_resolve(ggtxn_t* txn, pod_t* d, int readonly);
// char* graph_string(ggtxn_t* txn, strID_t id, size_t *len);
// static INLINE int ggblob_resolve(ggtxn_t* txn, strID_t *ret, void const *data, const size_t len, int readonly);
// static INLINE char* graph_string_enc(ggtxn_t* txn, void *id_enc, size_t *slen);
// static INLINE int ggdb_resolve_blob(ggtxn_t* txn, LG_id *ret, char const *data, const size_t len, const int readonly, LG_id * const cache, int db1, int db2);
    // TODO: ugh, returns 1 for success, 0 for failure (only for readonly)
static INLINE logID_t _graph_log_nextID(ggtxn_t* txn, int consume);
static INLINE logID_t _cleanse_beforeID(ggtxn_t* txn, logID_t beforeID);
static INLINE int _ggtxn_update_info(ggtxn_t* txn);
static int __magic_txnlog_cmp(const buffer_t *a, const buffer_t *b);

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
    [DB_TXNLOG] = { "txnlog", 0, __magic_txnlog_cmp }
};


// ============================================================================
#ifdef LG_IMPLEMENTATION


int ggtxn_begin(ggtxn_t* txn, ggraph_t* g, unsigned int flags)
{
	// TODO: ggtxn_t* txn = smalloc(sizeof(*txn));
	int r = db_txn_init((txn_t)txn, (db_t)g, NULL, flags);
    if (r) return (r = errno);
    // for parent write txns, we need to harvest the nextID
    txn->next_strID = 0;
    txn->next_logID = 0;
    txn->node_delta = 0;
    txn->edge_delta = 0;
    txn->begin_nextID = TXN_RW(txn) ? _graph_log_nextID(txn, 0) : 0;
    // other prev_* fields are only valid if prev_start is non-zero
    txn->prev_start = 0;
    return r;
}

int ggtxn_begin_child(ggtxn_t* txn, ggraph_t* g, ggtxn_t* parent, unsigned int flags)
{
    // TODO: if (parent == NULL)
    // TODO: ggtxn_t* txn = smalloc(sizeof(*txn));
	int r = db_txn_init((txn_t)txn, (db_t)g, (txn_t)parent, flags);
    if (r) return (r = errno);
    // for child write txns, take snapshot of parent data
    memcpy(sizeof(txn->txn) + (unsigned char *)txn,
            sizeof(txn->txn) + (unsigned char *)parent, sizeof(*txn) - sizeof(txn->txn));
	return r;
}

int ggtxn_commit(ggtxn_t* txn)
{
	int r;
	ggtxn_t* parent;
	txnID_t txnID = 0;
	if (!txn->txn.updated) {
		// nothing happened
		ggtxn_abort(txn);
		r = DB_SUCCESS;
	} else if UNLIKELY((parent = TXN_PARENT(txn))) {
		// nested write txn
		r = txn_commit((txn_t)txn);
		if(DB_SUCCESS == r){
			memcpy(sizeof(txn->txn) + (unsigned char *)parent,
			       sizeof(txn->txn) + (unsigned char *)txn,
                   sizeof(*txn) - sizeof(txn->txn));
		} else if (txn->prev_start != parent->prev_start){
			memcpy(&parent->prev_id, &txn->prev_id,
                   sizeof(*txn) - (intptr_t)&((ggtxn_t*)NULL)->prev_id);
		}
		memset(txn, 0, sizeof(*txn));
	}else if(_ggtxn_update_info(txn) && txn->next_logID > txn->begin_nextID){
		// write txn w/ valid txnlog table
		logID_t nextID = txn->begin_nextID;
		logID_t count = txn->next_logID - nextID;
		uint64_t nodes = txn->prev_nodes + txn->node_delta;
		uint64_t edges = txn->prev_edges + txn->edge_delta;
		uint8_t kbuf[esizeof(txnID) + esizeof(nextID) + esizeof(count)];
		uint8_t dbuf[esizeof(nodes) + esizeof(edges)];
		buffer_t key = { 0, kbuf }, data = { 0, dbuf };

		txnID = txn->prev_id + 1;

		encode(txnID,  kbuf, key.size);
		encode(nextID, kbuf, key.size);
		encode(count,  kbuf, key.size);
		encode(nodes,  dbuf, data.size);
		encode(edges,  dbuf, data.size);

		r = db_put((txn_t)txn, DB_TXNLOG, &key, &data, DB_APPEND);
		if LIKELY(DB_SUCCESS == r) {
			r = txn_commit((txn_t)txn);
		} else {
			txn_abort((txn_t)txn);
		}
	} else {
		// write txn w/ invalid txnlog table
		r = txn_commit((txn_t)txn);
	}

    return r;
}

void ggtxn_abort(ggtxn_t* txn)
{
	ggtxn_t* parent = TXN_PARENT(txn);
	if(parent)
		memcpy(&parent->prev_id, &txn->prev_id,
            sizeof(*txn) - (intptr_t)&((ggtxn_t*)NULL)->prev_id);
	txn_abort((txn_t)txn);
}

static int __magic_txnlog_cmp(const buffer_t *a, const buffer_t *b)
/*
    here's the deal - the actual key in the db is comprised of 3 serialized uints:
        txnID: incrementing sequence of transaction numbers, starting at 1
            each write txn that caused the log to grow will get it's own txnID
        start: first logID in the txn, will be >= 1
        count: number of logIDs accumulated in the txn, will be >= 1

    when this function is called, at least one of the params will be an actual key as above
    if the txnID for the other decodes to zero, then it is a query operation.
    Decoding the second uint determines whether the 3rd uint should be tested against (0)
    the txnID or (non-zero) the start/count range

    with one btree, this lets us quickly:
        map txnID to logID range
        map logID to containing txnID
*/
{
    int ia = 0, ib = 0;
    txnID_t ta, tb;
    decode(ta, a->data, ia);
    decode(tb, b->data, ib);
    if (!ta) {
        assert(tb);
        // a is query, b is actual key in db
        decode(ta, a->data, ia);
        if(ta){
            decode(ta, a->data, ia);
            LG_id start, count;
            decode(start, b->data, ib);
            if(ta < start)
                return -1;
            decode(count, b->data, ib);
            return ta >= (start + count);
        }
        // txnID query
        decode(ta, a->data, ia);
    } else if (!tb) {
        // I don't believe this happens today, but just in case ...
        return - __magic_txnlog_cmp(b, a);
    }
    return ta < tb ? -1 : ta > tb ? 1 : 0;
}



int pod_resolve(ggtxn_t* txn, pod_t* d, int readonly)
{
    // static INLINE int _string_resolve(ggtxn_t* txn, strID_t *ret, void const *data, const size_t len, int readonly){
    if (0 < d->id)              // already resolved
        return 1;
    if (NULL == d->data) {      // unresolvable TODO:
        assert(0 == d->size);
        d->id = 0;
        return 1;
    }
    return ggdb_resolve_blob(txn,
        &d->id, d->data, d->size,
        (readonly || TXN_RO(txn)),
        &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
}

static INLINE int
ggblob_resolve(ggtxn_t* txn, strID_t *ret, void const *data, const size_t len, int readonly)
{
    if UNLIKELY(NULL == data) {
        assert(0 == len);
        *ret = 0;
        return 1;
    }
    return ggdb_resolve_blob(txn,
        ret, data, len,
        readonly,
        &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
}

char*
graph_string(ggtxn_t* txn, strID_t id, size_t *len)
{
    // return id ? __blob(txn, id, len, DB_SCALAR) : ((*len = 0), NULL);
    if UNLIKELY(!id) {
        *len = 0;
        return NULL;
    }
// char *__blob(ggtxn_t* txn, LG_id id, size_t *len, int db1) {
    // assert(id);
    buffer_t key = { sizeof(id), &id };
    buffer_t data;
    int r = db_get((txn_t)txn, DB_SCALAR, &key, &data);
    assert(DB_SUCCESS == r);
    if (len)
        *len = data.size;
    return data.data;
// }
}

// fetch string by encoded ID
static INLINE char*
graph_string_enc(ggtxn_t* txn, void *id_enc, size_t *slen)
{
    strID_t id;
    int len = 0;
    decode(id, id_enc, len);
    return graph_string(txn, id, slen);
}

static INLINE int ggdb_resolve_blob(ggtxn_t* txn, LG_id *ret, char const *data, const size_t len, const int readonly, LG_id * const cache, int db1, int db2)
// returns 1 for success, 0 for failure (only for readonly)
{
    assert(data);
    int r;
    size_t count;
    LG_id id;
    uint32_t chk;

    struct cursor_t c, idx;
    buffer_t val;
    buffer_t vkey;
    buffer_t ival;
    buffer_t ikey = { .data= &chk, .size= sizeof(chk) };

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
    *ret = id = ggdb_next_id(txn, 1, cache, db1, 0);
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




static INLINE logID_t _graph_log_nextID(ggtxn_t* txn, int consume)
{
	return ggdb_next_id(txn, consume, &txn->next_logID, DB_LOG, 1);
}

static INLINE logID_t _cleanse_beforeID(ggtxn_t* txn, logID_t beforeID)
{
	return (beforeID && _graph_log_nextID(txn, 0) > beforeID) ? beforeID : 0;
}

// return 0 on error
LG_id ggdb_next_id(ggtxn_t* txn, const int consume, LG_id * const cache, const int db1, const int venc)
{
    if(consume && TXN_RO(txn)){
        errno = EINVAL;
        return 0;
    }

    int r;
    unsigned int i;
    LG_id id = *cache;
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

static INLINE int _ggtxn_update_info(ggtxn_t* txn)
{
	if (!txn->prev_start) {
		struct cursor_t c;
		int r = txn_cursor_init(&c, (txn_t)txn, DB_TXNLOG);
		assert(DB_SUCCESS == r);
		r = cursor_get(&c, NULL, NULL, DB_LAST);
		if(DB_SUCCESS == r){
			buffer_t data, key;
			r = cursor_get(&c, &key, &data, DB_GET_CURRENT);
			assert(DB_SUCCESS == r);
			size_t i = 0;
			decode(txn->prev_id, key.data, i);
			decode(txn->prev_start, key.data, i);
			decode(txn->prev_count, key.data, i);
			assert(i == key.size);

			i = 0;
			decode(txn->prev_nodes, data.data, i);
			decode(txn->prev_edges, data.data, i);
			assert(i == data.size);
		}else if(DB_NOTFOUND == r){
			txn->prev_start = 1; // fudged to make the return statement easy
			txn->prev_id = txn->prev_count = 0;
			txn->prev_nodes = txn->prev_edges = 0;
		}else{
			assert(DB_SUCCESS == r);
		}
		cursor_close(&c);
	}
	return txn->prev_start + txn->prev_count == txn->begin_nextID;
}

/*
    ZZZ: uint8_t *__lookup(graph_txn_t txn, entry_t e, const int db_idx, uint8_t *kbuf, size_t klen, const logID_t beforeID)
*/
static INLINE uint8_t*
_lg_rec_lookup(ggtxn_t* txn, ggentry_t* e, const int db_idx, uint8_t *kbuf, size_t klen, const LG_log beforeID)
{
    e->id = 0;

	struct cursor_t idx;
	int r = txn_cursor_init(&idx, (txn_t)txn, db_idx);
	assert(DB_SUCCESS == r);

	buffer_t key = { klen, kbuf };
	if(beforeID) // use beforeID to seek just past our target
		encode(beforeID, kbuf, key.size);
	else
		kbuf[key.size++] = 0xff;
    buffer_t data = { 0, NULL };
    uint8_t* logbuf = NULL;

    db_cursor_op op = -1;
	r = cursor_get(&idx, &key, &data, DB_SET_RANGE);
	if (DB_SUCCESS == r) { // back up one record
		op = DB_PREV;
	} else if (DB_NOTFOUND == r) { // no records larger than target - try last record
		op = DB_LAST;
	} else {
		assert(DB_SUCCESS == r);
	}
	r = cursor_get(&idx, &key, &data, op);
	if (DB_SUCCESS == r) {
		r = cursor_get(&idx, &key, &data, DB_GET_CURRENT);
		assert(DB_SUCCESS == r);
		if (memcmp(key.data, kbuf, klen) == 0) {
			decode(e->id, key.data, klen); // harvest id
			// now pull log entry to fill in .next
            uint8_t buf[esizeof(e->id)];
			key.size = 0;
			key.data = buf;
			encode(e->id, buf, key.size);
			r = db_get((txn_t)txn, DB_LOG, &key, &data);
			assert(DB_SUCCESS == r);
			assert(e->rectype == *(uint8_t *)data.data);
			klen = 1;
			decode(e->next, data.data, klen);
			if (e->next && (0 == beforeID || e->next < beforeID)) {
				e->id = 0;
			} else {
				logbuf = &((uint8_t *)data.data)[klen];
				e->is_new = 0;
			}
		}
	}
	cursor_close(&idx);
	return logbuf;
}

#endif
#endif