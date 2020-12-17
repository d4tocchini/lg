// suppress assert() elimination, as we are currently heavily relying on it
#ifdef NDEBUG
#undef NDEBUG
#endif

#include<assert.h>

#include<errno.h>
#include<dirent.h>
#include<fcntl.h>
#include<inttypes.h>
#include<limits.h>
#include<pthread.h>
#include<stdarg.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/stat.h>
#include<zlib.h>

#define LG_IMPLEMENTATION
#include<lemongraph.h>

#include"static_assert.h"
#include"counter.h"

STATIC_ASSERT(sizeof(uint64_t) == sizeof(txnID_t), "");
STATIC_ASSERT(sizeof(uint64_t) == sizeof(logID_t), "");
STATIC_ASSERT(sizeof(uint64_t) == sizeof(strID_t), "");



int pack_uints(int count, uint64_t *ints, void *buffer){
	int i, len = 0;
	for(i = 0; i < count; i++)
		encode(ints[i], buffer, len);
	return len;
}

// unpacks 'count' uints
// returns number of input bytes consumed
int unpack_uints(int count, uint64_t *ints, void *buffer){
	int i, len = 0;
	for(i = 0; i < count; i++)
		decode(ints[i], buffer, len);
	return len;
}

// unpacks up to 'count' uints
// returns how many it could have unpacked if buffer was big enough
// (if return is larger than 'count', then call again w/ a bigger buffer)
int unpack_uints2(int count, uint64_t *ints, void *buffer, size_t buflen){
	size_t len;
	int i;
	for(i = 0, len = 0; len < buflen; i++)
		if(i < count)
			decode(ints[i], buffer, len);
		else
			len += enclen(buffer, len);
	return len == buflen ? i : -1;
}

int pack_uint(uint64_t i, char *buffer){
	int len = 0;
	encode(i, buffer, len);
	return len;
}

uint64_t unpack_uint(char *buffer){
	int len = 0;
	uint64_t i;
	decode(i, buffer, len);
	return i;
}

char *graph_strerror(int err){
	return db_strerror(err);
}



typedef struct txn_info_t * txn_info_t;
struct txn_info_t{
	txnID_t id;
	logID_t start;
	logID_t count;
	uint64_t nodes;
	uint64_t edges;
};


static INLINE logID_t _graph_log_nextID(graph_txn_t txn, int consume){
	return _nextID(txn, consume, &txn->next_logID, DB_LOG, 1);
}



static INLINE logID_t _cleanse_beforeID(graph_txn_t txn, logID_t beforeID){
	return (beforeID && _graph_log_nextID(txn, 0) > beforeID) ? beforeID : 0;
}


static INLINE uint8_t *__lookup(graph_txn_t txn, entry_t e, const int db_idx, uint8_t *kbuf, size_t klen, const logID_t beforeID){
	struct cursor_t idx;
	int r = txn_cursor_init(&idx, (txn_t)txn, db_idx);
	assert(DB_SUCCESS == r);

	buffer_t key = { klen, kbuf }, data = { 0, NULL };
	db_cursor_op op = -1;
	uint8_t *logbuf = NULL;

	e->id = 0;

	// use beforeID to seek just past our target
	if(beforeID)
		encode(beforeID, kbuf, key.size);
	else
		kbuf[key.size++] = 0xff;
	r = cursor_get(&idx, &key, &data, DB_SET_RANGE);
	if(DB_SUCCESS == r){ // back up one record
		op = DB_PREV;
	}else if(DB_NOTFOUND == r){ // no records larger than target - try last record
		op = DB_LAST;
	}else{
		assert(DB_SUCCESS == r);
	}

	r = cursor_get(&idx, &key, &data, op);
	if(DB_SUCCESS == r){
		r = cursor_get(&idx, &key, &data, DB_GET_CURRENT);
		assert(DB_SUCCESS == r);
		if(memcmp(key.data, kbuf, klen) == 0){
			uint8_t buf[esizeof(e->id)];

			// harvest id
			decode(e->id, key.data, klen);

			// now pull log entry to fill in .next
			key.size = 0;
			key.data = buf;
			encode(e->id, buf, key.size);
			r = db_get((txn_t)txn, DB_LOG, &key, &data);
			assert(DB_SUCCESS == r);
			assert(e->rectype == *(uint8_t *)data.data);
			klen = 1;
			decode(e->next, data.data, klen);
			if(e->next && (0 == beforeID || e->next < beforeID)){
				e->id = 0;
			}else{
				logbuf = &((uint8_t *)data.data)[klen];
				e->is_new = 0;
			}
		}
	}

	cursor_close(&idx);
	return logbuf;
}

static INLINE logID_t _node_lookup(graph_txn_t txn, node_t e, logID_t beforeID){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->type, kbuf, klen);
	encode(e->val,  kbuf, klen);
	__lookup(txn, (entry_t)e, DB_NODE_IDX, kbuf, klen, beforeID);
	return e->id;
}

static INLINE logID_t _edge_lookup(graph_txn_t txn, edge_t e, logID_t beforeID){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->type, kbuf, klen);
	encode(e->val,  kbuf, klen);
	encode(e->src,  kbuf, klen);
	encode(e->tgt,  kbuf, klen);
	__lookup(txn, (entry_t)e, DB_EDGE_IDX, kbuf, klen, beforeID);
	return e->id;
}

static INLINE logID_t _prop_lookup(graph_txn_t txn, prop_t e, logID_t beforeID){
	uint8_t *logbuf, kbuf[esizeof(e->pid) + esizeof(e->key) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->pid, kbuf, klen);
	encode(e->key, kbuf, klen);
	logbuf = __lookup(txn, (entry_t)e, DB_PROP_IDX, kbuf, klen, beforeID);
	if(logbuf){
		klen = 0;
		klen += enclen(logbuf, klen); // skip pid
		klen += enclen(logbuf, klen); // skip key
		decode(e->val, logbuf, klen); // pull current value
	}
	return e->id;
}

static INLINE graph_iter_t _graph_entry_idx(graph_txn_t txn, int dbi, logID_t id, logID_t beforeID);
graph_iter_t graph_iter_concat(unsigned int count, ...);

static void _delete(graph_txn_t txn, const logID_t newrecID, const logID_t oldrecID, uint8_t *mem){
	uint8_t kbuf[esizeof(newrecID)];
	buffer_t key = { 0, kbuf }, olddata, newdata = { 1, mem };
	int r, tail, tlen;
	graph_iter_t iter;
	entry_t child;

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
	if(GRAPH_NODE == rectype){
		iter = graph_iter_concat(3,
			_graph_entry_idx(txn, DB_PROP_IDX, oldrecID, 0),
			_graph_entry_idx(txn, DB_SRCNODE_IDX, oldrecID, 0),
			_graph_entry_idx(txn, DB_TGTNODE_IDX, oldrecID, 0));
		txn->node_delta--;
	}else{
		if(GRAPH_EDGE == rectype)
			txn->edge_delta--;
		iter = _graph_entry_idx(txn, DB_PROP_IDX, oldrecID, 0);
	}
	while((child = graph_iter_next(iter))){
		_delete(txn, newrecID, child->id, mem);
		free(child);
	}

	graph_iter_close(iter);
}

static INLINE logID_t _log_append(graph_txn_t txn, uint8_t *dbuf, size_t dlen, logID_t delID){
	int r;
	logID_t id;
	uint8_t kbuf[esizeof(id)];
	buffer_t key = { 0, kbuf }, data = { dlen, dbuf };

	id = _graph_log_nextID(txn, 1);

	if(delID){
		uint8_t tmp[MAX_LOGBUF];
		_delete(txn, id, delID, tmp);
	}

	encode(id, kbuf, key.size);

	r = db_put((txn_t)txn, DB_LOG, &key, &data, DB_APPEND);
	if(DB_SUCCESS != r)
		fprintf(stderr, "err: %s\n", db_strerror(r));
	assert(DB_SUCCESS == r);
	return id;
}

static INLINE void _entry_unset(graph_txn_t txn, logID_t id, void *key, size_t klen){
	struct prop_t p = { .pid = id, .rectype = GRAPH_PROP };
	if(_string_resolve(txn, &p.key, key, klen, 1)){
		if(_prop_lookup(txn, &p, 0))
			graph_delete(txn, (entry_t)&p);
	}
}

static INLINE logID_t _entry_delete(graph_txn_t txn, logID_t delID){
	uint8_t dbuf[1 + esizeof(delID)];
	size_t dlen = 0;

	dbuf[dlen++] = GRAPH_DELETION;
	encode(delID, dbuf, dlen);

	return _log_append(txn, dbuf, dlen, delID);
}

static INLINE logID_t _node_append(graph_txn_t txn, node_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->type) + esizeof(e->val)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->type, dbuf, dlen);
	encode(e->val,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}

static INLINE logID_t _edge_append(graph_txn_t txn, edge_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->type, dbuf, dlen);
	encode(e->val,  dbuf, dlen);
	encode(e->src,  dbuf, dlen);
	encode(e->tgt,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}

static INLINE logID_t _prop_append(graph_txn_t txn, prop_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->pid) + esizeof(e->key) + esizeof(e->val)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->pid,  dbuf, dlen);
	encode(e->key,  dbuf, dlen);
	encode(e->val,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}


static INLINE void _node_index(graph_txn_t txn, node_t e){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->id)];
	buffer_t key = { 0, kbuf };
	buffer_t data = { 0, NULL };

	encode(e->type, kbuf, key.size);
	encode(e->val,  kbuf, key.size);
	encode(e->id,   kbuf, key.size);
	int r = db_put((txn_t)txn, DB_NODE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);
}

static INLINE void _edge_index(graph_txn_t txn, edge_t e){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt) + esizeof(e->id)];
	buffer_t key = { 0, kbuf };
	buffer_t data = { 0, NULL };
	int r;

	encode(e->type, kbuf, key.size);
	encode(e->val,  kbuf, key.size);
	encode(e->src,  kbuf, key.size);
	encode(e->tgt,  kbuf, key.size);
	encode(e->id,   kbuf, key.size);
	r = db_put((txn_t)txn, DB_EDGE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);

	key.size = 0;
	encode(e->src, kbuf, key.size);
	encode(e->type, kbuf, key.size);
	encode(e->id,  kbuf, key.size);
	r = db_put((txn_t)txn, DB_SRCNODE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);

	key.size = 0;
	encode(e->tgt, kbuf, key.size);
	encode(e->type, kbuf, key.size);
	encode(e->id,  kbuf, key.size);
	r = db_put((txn_t)txn, DB_TGTNODE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);
}

static INLINE void _prop_index(graph_txn_t txn, prop_t e){
	uint8_t kbuf[esizeof(e->pid) + esizeof(e->key) + esizeof(e->id)];
	buffer_t key = { 0, kbuf };
	buffer_t data = { 0, NULL };

	encode(e->pid, kbuf, key.size);
	encode(e->key, kbuf, key.size);
	encode(e->id,  kbuf, key.size);
	int r = db_put((txn_t)txn, DB_PROP_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);
}

static INLINE logID_t __prop_resolve(graph_txn_t txn, prop_t e, logID_t beforeID, int readonly){
	e->rectype = GRAPH_PROP;
	// stash the old value, in case we cared
	strID_t val = e->val;
	// stomps e->val
	if((_prop_lookup(txn, e, beforeID) && val == e->val) || readonly)
		return e->id;
	assert(0 == beforeID);
	// e->rectype = GRAPH_PROP;
	e->val = val;
	e->next = 0;
	e->is_new = 1;
	_prop_append(txn, e, e->id);
	_prop_index(txn, e);
	return e->id;
}

static INLINE logID_t __node_resolve(graph_txn_t txn, node_t e, logID_t beforeID, int readonly) {
	e->rectype = GRAPH_NODE;
	if(_node_lookup(txn, e, beforeID) || readonly)
		return e->id;
	assert(0 == beforeID);
	// e->rectype = GRAPH_NODE;
	e->next = 0;
	e->is_new = 1;
	txn->node_delta++;
	_node_append(txn, e, e->id);
	_node_index(txn, e);
	return e->id;
}

static INLINE logID_t __edge_resolve(graph_txn_t txn, edge_t e, logID_t beforeID, int readonly){
	e->rectype = GRAPH_EDGE;
	if(_edge_lookup(txn, e, beforeID) || readonly)
		return e->id;
	assert(0 == beforeID);
	// e->rectype = GRAPH_EDGE;
	e->next = 0;
	e->is_new = 1;
	txn->edge_delta++;
	_edge_append(txn, e, e->id);
	_edge_index(txn, e);
	return e->id;
}


static INLINE node_t _node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
	node_t e = malloc(sizeof(*e));
	if(_string_resolve(txn, &e->type, type, tlen, readonly) &&
	   _string_resolve(txn, &e->val, val, vlen, readonly) &&
	   __node_resolve(txn, e, beforeID, readonly) ){
		return e;
	}
	free(e);
	return NULL;
}

static INLINE edge_t _edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
	edge_t e = malloc(sizeof(*e));
	assert(src && tgt);
	e->src = src->id;
	e->tgt = tgt->id;
	if(_string_resolve(txn, &e->type, type, tlen, readonly) &&
	   _string_resolve(txn, &e->val, val, vlen, readonly) &&
	   __edge_resolve(txn, e, beforeID, readonly) ){
		return e;
	}
	free(e);
	return NULL;
}

static INLINE prop_t _prop_resolve(graph_txn_t txn, entry_t parent, void *key, size_t klen, void *val, size_t vlen, logID_t beforeID, int readonly){
	prop_t e = malloc(sizeof(*e));
	e->pid = parent->id;
	if(_string_resolve(txn, &e->key, key, klen, readonly) &&
	   _string_resolve(txn, &e->val, val, vlen, readonly) &&
	   __prop_resolve(txn, e, beforeID, readonly)){
		return e;
	}
	free(e);
	return NULL;
}

entry_t graph_entry(graph_txn_t txn, const logID_t id){
	static const int recsizes[] = {
		[GRAPH_DELETION] = sizeof(struct entry_t),
		[GRAPH_NODE]     = sizeof(struct node_t),
		[GRAPH_EDGE]     = sizeof(struct edge_t),
		[GRAPH_PROP]     = sizeof(struct prop_t),
	};
	uint8_t buf[esizeof(id)];
	buffer_t key = { 0, buf }, data;
	entry_t e = NULL;
	int r;
	encode(id, buf, key.size);
	r = db_get((txn_t)txn, DB_LOG, &key, &data);
	if(DB_SUCCESS == r){
		const uint8_t rectype = *(uint8_t *)data.data;
		assert(rectype < sizeof(recsizes) / sizeof(*recsizes));
		int klen = 1;
		e = malloc(recsizes[rectype]);
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


/*static int graph_string_resolve(graph_txn_t txn, strID_t *id, void const *data, const size_t len){
	return _string_resolve(txn, id, data, len, 0);
}*/


static INLINE logID_t _iter_idx_nextID(graph_iter_t iter);

logID_t graph_entry_updateID(graph_txn_t txn, entry_t e, logID_t beforeID){
	logID_t id, maxID;
	graph_iter_t iter = _graph_entry_idx(txn, DB_PROP_IDX, e->id, beforeID);
	if(beforeID){
		maxID = (e->next && e->next < beforeID) ? e->next : e->id;
		while((id = _iter_idx_nextID(iter))){
			if(id >= beforeID)
				continue;
			entry_t e = graph_entry(txn, id);
			if(e->next){
				if(e->next < beforeID && e->next > maxID)
					maxID = e->next;
			}else if(e->id > maxID){
				maxID = e->id;
			}
			free(e);
		}
	}else{
		maxID = e->next ? e->next : e->id;
		while((id = _iter_idx_nextID(iter))){
			entry_t e = graph_entry(txn, id);
			if(e->next){
				if(e->next > maxID)
					maxID = e->next;
			}else if(e->id > maxID){
				maxID = e->id;
			}
			free(e);
		}
	}
	graph_iter_close(iter);
	return maxID;
}

logID_t graph_updateID(graph_txn_t txn, logID_t beforeID){
	static struct entry_t top = { .id = 0, .next = 0 };
	return graph_entry_updateID(txn, &top, beforeID);
}

logID_t graph_node_updateID(graph_txn_t txn, node_t n, logID_t beforeID){
	return graph_entry_updateID(txn, (entry_t)n, beforeID);
}

logID_t graph_edge_updateID(graph_txn_t txn, edge_t e, logID_t beforeID){
	return graph_entry_updateID(txn, (entry_t)e, beforeID);
}

logID_t graph_prop_updateID(graph_txn_t txn, prop_t p, logID_t beforeID){
	return graph_entry_updateID(txn, (entry_t)p, beforeID);
}

logID_t graph_log_nextID(graph_txn_t txn){
	return _graph_log_nextID(txn, 0);
}

logID_t graph_delete(graph_txn_t txn, entry_t e){
	return _entry_delete(txn, e->id);
}

prop_t graph_prop(graph_txn_t txn, const logID_t id){
	prop_t e = (prop_t) graph_entry(txn, id);
	if(e && GRAPH_PROP != e->rectype){
		free(e);
		e = NULL;
	}
	return e;
}

prop_t graph_prop_get(graph_txn_t txn, prop_t prop, void *key, size_t klen, logID_t beforeID){
	return _prop_resolve(txn, (entry_t)prop, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_prop_set(graph_txn_t txn, prop_t prop, void *key, size_t klen, void *val, size_t vlen){
	return _prop_resolve(txn, (entry_t)prop, key, klen, val, vlen, 0, 0);
}

void graph_prop_unset(graph_txn_t txn, prop_t e, void *key, size_t klen){
	_entry_unset(txn, e->id, key, klen);
}

prop_t graph_get(graph_txn_t txn, void *key, size_t klen, logID_t beforeID){
	static struct entry_t parent = { .id = 0 };
	return _prop_resolve(txn, &parent, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_set(graph_txn_t txn, void *key, size_t klen, void *val, size_t vlen){
	static struct entry_t parent = { .id = 0 };
	return _prop_resolve(txn, &parent, key, klen, val, vlen, 0, 0);
}

void graph_unset(graph_txn_t txn, void *key, size_t klen){
	_entry_unset(txn, 0, key, klen);
}

// logID_t graph_ID_set(graph_txn_t txn, logID_t parent_id, prop_t e){
logID_t graph_ID_set(graph_txn_t txn, logID_t parent_id, strID_t key, strID_t val){
	struct prop_t e = {
		.rectype = GRAPH_PROP,
		.pid = parent_id,
		.key = key,
		.val = val
	};
	return __prop_resolve(txn, &e, 0, 0);
}


node_t graph_node(graph_txn_t txn, const logID_t id){
	node_t e = (node_t) graph_entry(txn, id);
	if(e && GRAPH_NODE != e->rectype){
		free(e);
		e = NULL;
	}
	return e;
}

node_t graph_node_lookup(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID){
	return _node_resolve(txn, type, tlen, val, vlen, beforeID, 1);
}

node_t graph_node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen){
	return _node_resolve(txn, type, tlen, val, vlen, 0, 0);
}

logID_t node_resolve(graph_txn_t txn, node_t e, strID_t type, strID_t val) {
	e->type = type;
	e->val = val;
	return __node_resolve(txn, e, 0, 0);
}

logID_t graph_nodeID_resolve(graph_txn_t txn, strID_t type, strID_t val){
	struct node_t e = {
		.rectype = GRAPH_NODE,
		.type = type,
		.val = val
	};
	return __node_resolve(txn, &e, 0, 0);
}


prop_t graph_node_get(graph_txn_t txn, node_t node, void *key, size_t klen, logID_t beforeID){
	return _prop_resolve(txn, (entry_t)node, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_node_set(graph_txn_t txn, node_t node, void *key, size_t klen, void *val, size_t vlen){
	return _prop_resolve(txn, (entry_t)node, key, klen, val, vlen, 0, 0);
}

void graph_node_unset(graph_txn_t txn, node_t e, void *key, size_t klen){
	_entry_unset(txn, e->id, key, klen);
}


edge_t graph_edge(graph_txn_t txn, const logID_t id){
	edge_t e = (edge_t) graph_entry(txn, id);
	if(e && GRAPH_EDGE != e->rectype){
		free(e);
		e = NULL;
	}
	return e;
}

edge_t graph_edge_lookup(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID){
	return _edge_resolve(txn, src, tgt, type, tlen, val, vlen, beforeID, 1);
}

edge_t graph_edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen){
	return _edge_resolve(txn, src, tgt, type, tlen, val, vlen, 0, 0);
}

logID_t edge_resolve(graph_txn_t txn, edge_t e, logID_t src, logID_t tgt, strID_t type, strID_t val) {
	e->type = type;
	e->val = val;
	e->src = src;
	e->tgt = tgt;
	return __edge_resolve(txn, e, 0, 0);
}

logID_t graph_edgeID_resolve(graph_txn_t txn, logID_t src, logID_t tgt, strID_t type, strID_t val){
	assert(src && tgt);
	struct edge_t e = {
		.rectype = GRAPH_EDGE,
		.src = src,
		.tgt = tgt,
		.type = type,
		.val = val
	};
	return __edge_resolve(txn, &e, 0, 0);
}

prop_t graph_edge_get(graph_txn_t txn, edge_t edge, void *key, size_t klen, logID_t beforeID)
{
	return _prop_resolve(txn, (entry_t)edge, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_edge_set(graph_txn_t txn, edge_t edge, void *key, size_t klen, void *val, size_t vlen)
{
	return _prop_resolve(txn, (entry_t)edge, key, klen, val, vlen, 0, 0);
}

void graph_edge_unset(graph_txn_t txn, edge_t e, void *key, size_t klen)
{
	_entry_unset(txn, e->id, key, klen);
}

kv_t graph_kv(graph_txn_t txn, const void *domain, const size_t dlen, const int flags)
{
	// kv_t kv = NULL;
	// strID_t domainID;
	// if (!_string_resolve(txn, &domainID, domain, dlen, (TXN_RO(txn) || (flags & LG_KV_RO))))
	// 	goto FAIL;
	// kv = malloc(sizeof(*kv));
	// if (!kv)
	// 	goto FAIL;
	// kv->txn = txn;
	// kv->flags = flags;
	// kv->refs = 1;
	// kv->klen = 0;
	// encode(domainID, kv->kbuf, kv->klen);
	gkv_t* kv = NULL;
	d_t d = d_buf(domain, dlen);
	if ( !d_resolve(txn, &d, (TXN_RO(txn) || (flags & LG_KV_RO))) )
        goto FAIL;
	kv = malloc(sizeof(*kv));
	if (!kv)
		goto FAIL;
	gkv_init(txn, kv, d, flags);
	return kv;
FAIL:
	if(kv)
		free(kv);
	return NULL;
}



static INLINE int _kv_setup_key(kv_t kv, void *key, size_t klen, int query)
{
	strID_t id;
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	if (kv->flags & LG_KV_MAP_KEYS){
		if (!_string_resolve(kv->txn, &id, key, klen, query))
			return 0;
		encode(id, kv->kbuf, kv->key.size);
	} else {
		assert(klen <= sizeof(kv->kbuf) - kv->klen);
		memcpy(&kv->kbuf[kv->klen], key, klen);
		kv->key.size += klen;
	}
	return 1;
}


void *kv_get(kv_t kv, void *key, size_t klen, size_t *dlen)
{
	d_t val;
	gkv_get(kv, d_buf(key, klen), &val);
	return val.data;

// 	void *data = NULL;
// 	if (!_kv_setup_key(kv, key, klen, 1))
// 		goto done;
// 	if (db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val) != DB_SUCCESS)
// 		goto done;
// 	if (kv->flags & LG_KV_MAP_DATA){
// 		data = graph_string_enc(kv->txn, kv->val.data, dlen);
// 	} else {
// 		data = kv->val.data;
// 		*dlen = kv->val.size;
// 	}
// done:
//  return data;
}

int kv_del(kv_t kv, void *key, size_t klen){
	int ret = 0;
	if(_kv_setup_key(kv, key, klen, 1))
		ret = (db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL) == DB_SUCCESS);
	return ret;
}

int kv_put(kv_t kv, void *key, size_t klen, void *data, size_t dlen){
	int ret = 0;
	uint8_t dbuf[esizeof(strID_t)];
	if(!_kv_setup_key(kv, key, klen, 0))
		goto done;
	if(kv->flags & LG_KV_MAP_DATA){
		strID_t id;
		if(!_string_resolve(kv->txn, &id, data, dlen, 0))
			goto done;
		kv->val.data = dbuf;
		kv->val.size = 0;
		encode(id, dbuf, kv->val.size);
	}else{
		kv->val.data = data;
		kv->val.size = dlen;
	}
	ret = (db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->val, 0) == DB_SUCCESS);
done:
	return ret;
}

static INLINE void *_kv_key(kv_t kv, buffer_t *key, size_t  *len, const int unmap){
	if(unmap)
		return graph_string_enc(kv->txn, key->data + kv->klen, len);
	*len = key->size - kv->klen;
	return key->data + kv->klen;
}

void *kv_first_key(kv_t kv, size_t *klen){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	buffer_t key;
	void *ret = NULL;
	r = cursor_first_key(&cursor, &key, kv->kbuf, kv->klen);
	if(DB_SUCCESS == r)
		ret = _kv_key(kv, &key, klen, kv->flags & LG_KV_MAP_KEYS);
	cursor_close(&cursor);
	return ret;
}

void *kv_last_key(kv_t kv, size_t *klen){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	buffer_t key;
	void *ret = NULL;
	r = cursor_last_key(&cursor, &key, kv->kbuf, kv->klen);
	if(DB_SUCCESS == r)
		ret = _kv_key(kv, &key, klen, kv->flags & LG_KV_MAP_KEYS);
	cursor_close(&cursor);
	return ret;
}

void kv_deref(kv_t kv){
	if(!kv || !kv->refs)
		return;
	if(!--kv->refs)
		free(kv);
}

int kv_clear_pfx(kv_t kv, uint8_t *pfx, unsigned int len){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	assert(kv->klen + len <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->klen, pfx, len);
	len += kv->klen;
	buffer_t k;
	r = cursor_first_key(&cursor, &k, kv->kbuf, len);
	while(DB_SUCCESS == r){
		cursor_del(&cursor, 0);
		r = cursor_first_key(&cursor, &k, kv->kbuf, len);
	}
	cursor_close(&cursor);
	return 1;
}

int kv_clear(kv_t kv){
	return kv_clear_pfx(kv, NULL, 0);
}

int kv_fifo_push_n(kv_t kv, void **datas, size_t *lens, const int count){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_last_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	if(DB_NOTFOUND == r){
		r = DB_SUCCESS;
		kv->key.size = kv->klen + ctr_init(kv->kbuf + kv->klen);
	}else if(DB_SUCCESS == r){
		memcpy(kv->kbuf, kv->key.data, kv->key.size);
		kv->key.size = kv->klen + ctr_inc(kv->kbuf + kv->klen);
	}
	kv->key.data = kv->kbuf;

	const int resolve = kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA);
	int i;
	strID_t id;
	uint8_t edata[esizeof(id)];
	for(i = 0; DB_SUCCESS == r && i < count; i++){
		if(resolve){
			if(!_string_resolve(kv->txn, &id, datas[i], lens[i], 0)){
				r = DB_NOTFOUND;
				goto done;
			}
			kv->val.size = 0;
			kv->val.data = edata;
			encode(id, edata, kv->val.size);
		}else{
			kv->val.data = datas[i];
			kv->val.size = lens[i];
		}
		if(i)
			kv->key.size = kv->klen + ctr_inc(kv->kbuf + kv->klen);
		r = cursor_put(&cursor, &kv->key, &kv->val, 0);
	}
	if(DB_SUCCESS == r)
		r = i;
done:
	cursor_close(&cursor);
	return r;
}

int kv_fifo_push(kv_t kv, void *data, size_t len){
	return kv_fifo_push_n(kv, &data, &len, 1);
}

int kv_fifo_peek_n(kv_t kv, void **datas, size_t *lens, const int count){
	struct cursor_t cursor;
	int i, r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	const int resolve = kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA);
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	for(i = 0; DB_SUCCESS == r && i < count; i++){
		r = cursor_get(&cursor, &kv->key, &kv->val, DB_SET_KEY);
		assert(DB_SUCCESS == r);
		if(resolve){
			datas[i] = graph_string_enc(kv->txn, kv->val.data, &lens[i]);
		}else{
			datas[i] = kv->val.data;
			lens[i] = kv->val.size;
		}
		r = cursor_get(&cursor, &kv->key, NULL, DB_NEXT);
		if(DB_SUCCESS != r || kv->key.size < kv->klen || memcmp(kv->key.data, kv->kbuf, kv->klen))
			r = DB_NOTFOUND;
	}
	if(DB_SUCCESS == r || DB_NOTFOUND == r)
		r = i;
	cursor_close(&cursor);
	return r;
}

int kv_fifo_peek(kv_t kv, void **data, size_t *size){
	return kv_fifo_peek_n(kv, data, size, 1);
}

int kv_fifo_delete(kv_t kv, const int count){
	struct cursor_t cursor;
	int i, r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	for(i = 0; DB_SUCCESS == r && i < count; i++){
		r = cursor_del(&cursor, 0);
		assert(DB_SUCCESS == r);
		r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	}
	if(DB_SUCCESS == r || DB_NOTFOUND == r)
		r = i;
	cursor_close(&cursor);
	return r;
}

int kv_fifo_len(kv_t kv, uint64_t *len){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	buffer_t key2;
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	if(DB_SUCCESS == r){
		int r2 = cursor_last_key(&cursor, &key2, kv->kbuf, kv->klen);
		assert(DB_SUCCESS == r2);
		*len = 1 + ctr_delta(key2.data + kv->klen, kv->key.data + kv->klen);
	}else if(DB_NOTFOUND == r){
		r = DB_SUCCESS;
		*len = 0;
	}
	cursor_close(&cursor);
	return r;
}

// priority queues on top of kv
// we store two different structures under a domain:
//   first:  enc(domID), 0, priority, counter => key
//   second: enc(domID), 1, key => priority, counter
// priority as well as the 0/1 are literal bytes
// counter is up-to 256 bytes - can increment/decrement from [0 .. ((1<<2040)-1)]

// fetch priority[0..255] for key, on error return < 0
int kv_pq_get(kv_t kv, void *key, size_t klen){
	int r;
	strID_t id;
	uint8_t ekey[esizeof(id)];

	// optionally swap out key w/ encoded string ID
	r = DB_NOTFOUND;
	if(kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)){
		if(!_string_resolve(kv->txn, &id, key, klen, 0))
			goto done;
		klen = 0;
		key = ekey;
		encode(id, ekey, klen);
	}

	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking secondary index
	kv->kbuf[kv->key.size++] = 1;
	// append key
	assert(kv->key.size + klen <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->key.size, key, klen);
	kv->key.size += klen;

	// see if it's already somewhere in the queue
	r = db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val);
	if(DB_SUCCESS != r)
		goto done;

	// grab priority byte
	r = *(uint8_t *)kv->val.data;
done:
	return r;
}

// if get (dom, 1, key) => (old_pri, counter)
//   del (dom, 0, old_pri, counter)
// if find_last_counter (dom, 0, new_pri)
//   ctr_inc(counter)
// else
//   ctr_init(counter)
// put (dom, 0, new_pri, counter) => key
// put (dom, 1, key) => (new_pri, counter)

// on success return 0, on error return < 0
int kv_pq_del(kv_t kv, void *key, size_t klen){
	int r;
	strID_t id;
	uint8_t ekey[esizeof(id)];

	// optionally swap out key w/ encoded string ID
	r = DB_NOTFOUND;
	if(kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)){
		if(!_string_resolve(kv->txn, &id, key, klen, 0))
			goto done;
		klen = 0;
		key = ekey;
		encode(id, ekey, klen);
	}

	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking secondary index
	kv->kbuf[kv->key.size++] = 1;
	// append key
	assert(kv->key.size + klen <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->key.size, key, klen);
	kv->key.size += klen;

	// see if it's already somewhere in the queue
	r = db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val);


	// if found, use returned priority, counter to delete from primary index
	if(DB_SUCCESS == r){
		// start with encoded domID
		kv->key.data = kv->kbuf;
		kv->key.size = kv->klen;
		// we are checking primary index
		kv->kbuf[kv->key.size++] = 0;

		// append old_pri, counter
		assert(kv->key.size + kv->val.size <= sizeof(kv->kbuf));
		memcpy(kv->kbuf + kv->key.size, kv->val.data, kv->val.size);
		kv->key.size += kv->val.size;

		// delete from primary index
		r = db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL);
		assert(DB_SUCCESS == r);

		// rebuild secondary key
		kv->kbuf[kv->klen] = 1;
		memcpy(kv->kbuf + kv->klen + 1, key, klen);
		kv->key.data = kv->kbuf;
		kv->key.size = kv->klen + 1 + klen;

		// delete from secondary index
		r = db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL);
		assert(DB_SUCCESS == r);
	}
done:
	return r;
}

// on success return 0, on error return < 0
int kv_pq_add(kv_t kv, void *key, size_t klen, uint8_t priority){
	strID_t id;
	int r, pc_len;
	struct cursor_t cursor;
	uint8_t ekey[esizeof(id)];
	uint8_t pri_counter[257];

	r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	if(DB_SUCCESS != r)
		goto done0;

	// optionally swap out key w/ encoded string ID
	r = DB_NOTFOUND;
	if(kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)){
		if(!_string_resolve(kv->txn, &id, key, klen, 0))
			goto done;
		klen = 0;
		key = ekey;
		encode(id, ekey, klen);
	}

	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking secondary index
	kv->kbuf[kv->key.size++] = 1;
	// append key
	assert(kv->key.size + klen <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->key.size, key, klen);
	kv->key.size += klen;

	// see if it's already somewhere in the queue
	r = db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val);

	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking primary index
	kv->kbuf[kv->key.size++] = 0;

	// if found, use returned priority, counter to delete from primary index
	if(DB_SUCCESS == r){
		// append old_pri, counter
		assert(kv->key.size + kv->val.size <= sizeof(kv->kbuf));
		memcpy(kv->kbuf + kv->key.size, kv->val.data, kv->val.size);
		kv->key.size += kv->val.size;
		// delete from primary index
		r = db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL);
		assert(DB_SUCCESS == r);
	}

	// now insert new priority byte
	kv->kbuf[kv->klen + 1] = priority;
	// and find last key w/ that priority in primary index
	r = cursor_last_key(&cursor, &kv->key, kv->kbuf, kv->klen+2);

	const int tail = kv->klen + 2;
	if(DB_SUCCESS == r){
		memcpy(kv->kbuf + tail, kv->key.data + tail, kv->key.size - tail);
		// increment counter, grab length of pri & counter bytes
		pc_len = 1 + ctr_inc(kv->kbuf + tail);
	}else{
		pc_len = 1 + ctr_init(kv->kbuf + tail);
	}
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen + 1 + pc_len;
	// snag copy
	memcpy(pri_counter, kv->kbuf + kv->klen + 1, pc_len);

	// add in new record in primary index
	kv->val.data = key;
	kv->val.size = klen;
	r = db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->val, 0);
	assert(DB_SUCCESS == r);

	// add reverse record in secondary index
	kv->kbuf[kv->klen] = 1;
	memcpy(kv->kbuf + kv->klen + 1, key, klen);
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen + 1 + klen;
	kv->val.data = pri_counter;
	kv->val.size = pc_len;
	r = db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->val, 0);
	assert(DB_SUCCESS == r);

done:
	cursor_close(&cursor);
done0:
	return r;
}

uint8_t *kv_pq_cursor(kv_t kv, uint8_t priority){
	uint8_t *cursor = malloc(512);
	int len = kv->klen + 1;
	if(cursor){
		// holds [decode flag][pfx][magic][priority][counter]
		// where:
		//	pfx is (encoded domain ID, 0)
		//	priority is 1 byte

		// append decode flags
		cursor[0] = kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA);

		// append encoded domain
		memcpy(cursor + 1, kv->kbuf, kv->klen);

		// append magic byte to select primary index
		cursor[len++] = 0;

		// append requested priority
		cursor[len++] = priority;

		// and initialize counter
		ctr_init(cursor + len);
	}
	return cursor;
}

// on success, advance cursor, fill in key/klen, and return priority [0-255]
// on error, return < 0
int kv_pq_cursor_next(graph_txn_t txn, uint8_t *cursor, void **key, size_t *klen){
	int r;
	buffer_t k, v;
	struct cursor_t c;

	const unsigned int domlen = enclen(cursor, 1);

	// flags + domlen + magic + priority
	const unsigned int ctroff = domlen + 3;

	// domlen + magic
	const unsigned int pfxlen = domlen + 1;

	// skip flags byte
	k.data = cursor + 1;

	// encoded domain + magic + priority + counter
	k.size = pfxlen + 1 + ctr_len(cursor + ctroff);

	r = txn_cursor_init(&c, (txn_t)txn, DB_KV);
	assert(DB_SUCCESS == r);

	r = cursor_get(&c, &k, NULL, DB_SET_RANGE);
	if(DB_SUCCESS != r)
		goto done;

	if(k.size < pfxlen || memcmp(k.data, cursor + 1, pfxlen)){
		r = DB_NOTFOUND;
		goto done;
	}

	r = cursor_get(&c, &k, &v, DB_GET_CURRENT);
	if(DB_SUCCESS != r)
		goto done;

	// copy what we found
	memcpy(cursor + 1, k.data, k.size);

	// increment its counter
	ctr_inc(cursor + ctroff);

	// harvest priority
	r = cursor[ctroff-1];

	// possibly lookup result
	if(cursor[0]){
		*key = graph_string_enc(txn, v.data, klen);
	}else{
		*key = v.data;
		*klen = v.size;
	}

done:
	cursor_close(&c);
	return r;
}

void kv_pq_cursor_close(uint8_t *cursor){
	free(cursor);
}

kv_iter_t kv_pq_iter(kv_t kv){
	uint8_t pfx = 0;
	return kv_iter_pfx(kv, &pfx, 1);
}

int kv_pq_iter_next(kv_iter_t iter, void **data, size_t *dlen){
	const iter_t it = (iter_t)iter;
	int r = iter_next(it);
	const int ret = (DB_SUCCESS == r);
	if(ret){
		const kv_t kv = iter->kv;
		if(kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)){
			*data = graph_string_enc(kv->txn, it->data.data, dlen);
		}else{
			*data = it->data.data;
			*dlen = it->data.size;
		}
	}
	return ret;
}

kv_iter_t kv_iter_pfx(kv_t kv, uint8_t *pfx, unsigned int len){
	kv_iter_t iter;
	iter = malloc(sizeof(*iter));
	if(iter){
		int r;
		if(pfx){
			assert((kv->flags & LG_KV_MAP_KEYS) == 0);
			uint8_t buf[kv->klen + len];
			memcpy(buf, kv->kbuf, kv->klen);
			memcpy(buf + kv->klen, pfx, len);
			r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, buf, kv->klen + len);
		}else{
			r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, kv->kbuf, kv->klen);
		}
		if(DB_SUCCESS == r){
			iter->kv = kv;
			kv->refs++;
		}else{
			free(iter);
			iter = NULL;
			errno = r;
		}
	}
	return iter;
}

int kv_next_reset(kv_t kv){
	buffer_t bmk = { .size = kv->klen, .data = kv->kbuf };
	int r = db_del((txn_t)kv->txn, DB_KVBM, &bmk, NULL);
	return (DB_SUCCESS == r || DB_NOTFOUND == r);
}

int kv_next(kv_t kv, void **key, size_t *klen, void **data, size_t *dlen){
	int r, ret = 0;
	buffer_t bmk = { .size = kv->klen, .data = kv->kbuf };
	buffer_t pos, val;
	struct cursor_t c;
	txn_t txn = (txn_t)kv->txn;

	r = txn_cursor_init(&c, txn, DB_KV);
	assert(DB_SUCCESS == r);

	// try to fetch the bookmark from where we left off
	r = db_get(txn, DB_KVBM, &bmk, &pos);
	if(DB_SUCCESS == r){
		void *found = pos.data;
		size_t flen = pos.size;
		r = cursor_get(&c, &pos, NULL, DB_SET_RANGE);

		// step forward if we found it exactly
		if(DB_SUCCESS == r && flen == pos.size && memcmp(found, pos.data, flen) == 0)
			r = cursor_get(&c, &pos, NULL, DB_NEXT);

		// if we've run off the end, set error status
		if(DB_SUCCESS == r && (pos.size < kv->klen || memcmp(pos.data, bmk.data, kv->klen)))
			r = DB_NOTFOUND;
	}

	// was there no bookmark? or did set_range fail? or did we run off the end?
	if(DB_SUCCESS != r){
		// fall back to start of kv range
		memcpy(&pos, &bmk, sizeof(bmk));
		r = cursor_get(&c, &pos, NULL, DB_SET_RANGE);
	}

	// nothing to do?
	if(DB_SUCCESS != r || pos.size < kv->klen || memcmp(pos.data, bmk.data, kv->klen))
		goto bail;

	{
		// stash a copy of the key
		// fixme - do we need to make a copy?
		uint8_t kbuf[pos.size];
		memcpy(kbuf, pos.data, pos.size);

		// update the bookmark
		r = db_put(txn, DB_KVBM, &bmk, &pos, 0);
		if(DB_SUCCESS != r)
			goto bail;

		// now go and fetch actual key & data
		pos.data = kbuf;
		assert(pos.size >= kv->klen);
		val.data = NULL;
		r = cursor_get(&c, &pos, &val, DB_SET_KEY);
		assert(val.data);
		if(DB_SUCCESS == r){
			if(kv->flags & LG_KV_MAP_KEYS){
				*key = graph_string_enc(kv->txn, pos.data + kv->klen, klen);
			}else{
				*key = pos.data + kv->klen;
				*klen = pos.size - kv->klen;
			}
			if(kv->flags & LG_KV_MAP_DATA){
				*data = graph_string_enc(kv->txn, val.data, dlen);
			}else{
				*data = val.data;
				*dlen = val.size;
			}
			ret = 1;
		}
	}
done:
	cursor_close(&c);
	return ret;

bail:
	ret = 0;
	goto done;
}

kv_iter_t kv_iter(kv_t kv){
	return kv_iter_pfx(kv, NULL, 0);
}

int kv_iter_next(kv_iter_t iter, void **key, size_t *klen, void **data, size_t *dlen){
	const iter_t it = (iter_t)iter;
	int r = iter_next(it);
	const int ret = (DB_SUCCESS == r);
	if(ret){
		const kv_t kv = iter->kv;
		if(kv->flags & LG_KV_MAP_KEYS){
			*key = graph_string_enc(kv->txn, it->key.data + kv->klen, klen);
		}else{
			*key = it->key.data + kv->klen;
			*klen = it->key.size - kv->klen;
		}
		if(kv->flags & LG_KV_MAP_DATA){
			*data = graph_string_enc(kv->txn, it->data.data, dlen);
		}else{
			*data = it->data.data;
			*dlen = it->data.size;
		}
	}
	return ret;
}

int kv_iter_seek(kv_iter_t iter, void *key, size_t klen){
	// call this anyway to setup key buffer
	int ret = _kv_setup_key(iter->kv, key, klen, 1);
	assert(ret);

	const kv_t kv = iter->kv;
	// don't care if this fails - subsequent iter_next() will fail too
	iter_seek((iter_t)iter, kv->key.data, kv->key.size);

	return ret;
}

void kv_iter_close(kv_iter_t iter){
	kv_deref(iter->kv);
	iter_close((iter_t)iter);
}



graph_iter_t graph_iter_new(graph_txn_t txn, int dbi, void *pfx, size_t pfxlen, logID_t beforeID){
	graph_iter_t gi = malloc(sizeof(*gi));
	if(gi){
		int r = txn_iter_init((iter_t)gi, (txn_t)txn, dbi, pfx, pfxlen);
		if(DB_SUCCESS == r){
			gi->beforeID = _cleanse_beforeID(txn, beforeID);
			gi->txn = txn;
			gi->next = NULL;
			gi->head_active = 1;
		}else{
			free(gi);
			gi = NULL;
			errno = r;
		}
	}
	return gi;
}

graph_iter_t graph_iter_concat(unsigned int count, ...){
	graph_iter_t head = NULL, tail = NULL;
	va_list ap;
	va_start(ap, count);
	while(count--){
		graph_iter_t current = va_arg(ap, graph_iter_t);
		if(!current)
			continue;
		if(tail)
			tail->next = current;
		else
			head = tail = current;
		while(tail->next)
			tail = tail->next;
	}
	va_end(ap);
	return head;
}

static INLINE logID_t _parse_idx_logID(uint8_t *buf, size_t buflen){
	size_t i = 0, len = 0;
	logID_t id;

	do{
		i += len;
		len = enclen(buf, i);
	}while(i + len < buflen);
	assert(i + len == buflen);
	decode(id, buf, i);
	return id;
}

static INLINE logID_t _blarf(graph_iter_t iter){
	logID_t ret = 0;
	while(iter_next_key((iter_t)iter) == DB_SUCCESS){
		logID_t id = _parse_idx_logID(((iter_t)iter)->key.data, ((iter_t)iter)->key.size);
		if(0 == iter->beforeID || id < iter->beforeID){
			ret = id;
			goto done;
		}
	}
done:
	return ret;
}

// scans index and returns logIDs < beforeID (if beforeID applies)
// caller is responsible for filtering out overwritten IDs
static INLINE logID_t _iter_idx_nextID(graph_iter_t gi){
	logID_t id = 0;
	if(gi->head_active){
		// head is still active - try it
		if((id = _blarf(gi)))
			goto done;

		// exhaused - deactivate head
		gi->head_active = 0;
		gi->txn = gi->next ? gi->next->txn : NULL;
	}
	while(gi->next){
		if((id = _blarf(gi->next)))
			goto done;

		// exhausted - remove chained iterator
		graph_iter_t tmp = gi->next;
		gi->next = tmp->next;
		iter_close((iter_t)tmp);
		gi->txn = gi->next ? gi->next->txn : NULL;
	}

done:
	return id;
}

entry_t graph_iter_next(graph_iter_t gi){
	if(gi){
		logID_t id;
		while((id = _iter_idx_nextID(gi))){
			entry_t e = graph_entry(gi->txn, id);
			if(e->next == 0 || (gi->beforeID && e->next >= gi->beforeID))
				return e;
			free(e);
		}
	}
	return NULL;
}

void graph_iter_close(graph_iter_t gi){
	while(gi){
		graph_iter_t next = gi->next;
		iter_close((iter_t)gi);
		gi = next;
	}
}

static INLINE graph_iter_t _graph_entry_idx(graph_txn_t txn, int dbi, logID_t id, logID_t beforeID){
	uint8_t buf[esizeof(id)];
	size_t buflen = 0;
	encode(id, buf, buflen);
	return graph_iter_new(txn, dbi, buf, buflen, beforeID);
}

graph_iter_t graph_nodes(graph_txn_t txn, logID_t beforeID){
	return graph_iter_new(txn, DB_NODE_IDX, "", 0, beforeID);
}

graph_iter_t graph_edges(graph_txn_t txn, logID_t beforeID){
	return graph_iter_new(txn, DB_EDGE_IDX, "", 0, beforeID);
}

static INLINE graph_iter_t _graph_nodes_edges_type(graph_txn_t txn, int dbi, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	uint8_t kbuf[esizeof(typeID)];
	size_t klen = 0;
	graph_iter_t iter = NULL;
	if(graph_string_lookup(txn, &typeID, type, tlen)){
		encode(typeID, kbuf, klen);
		iter = graph_iter_new(txn, dbi, kbuf, klen, beforeID);
	}
	return iter;
}

graph_iter_t graph_nodes_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID){
	return _graph_nodes_edges_type(txn, DB_NODE_IDX, type, tlen, beforeID);
}

graph_iter_t graph_edges_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID){
	return _graph_nodes_edges_type(txn, DB_EDGE_IDX, type, tlen, beforeID);
}

graph_iter_t graph_edges_type_value(graph_txn_t txn, void *type, size_t tlen, void *value, size_t vlen, logID_t beforeID){
	strID_t typeID, valID;
	uint8_t kbuf[esizeof(typeID) + esizeof(valID)];
	size_t klen = 0;
	graph_iter_t iter = NULL;
	if(graph_string_lookup(txn, &typeID, type, tlen) && graph_string_lookup(txn, &valID, value, vlen)){
		encode(typeID, kbuf, klen);
		encode(valID, kbuf, klen);
		iter = graph_iter_new(txn, DB_EDGE_IDX, kbuf, klen, beforeID);
	}
	return iter;
}

graph_iter_t graph_node_edges_in(graph_txn_t txn, node_t node, logID_t beforeID){
	return _graph_entry_idx(txn, DB_TGTNODE_IDX, node->id, beforeID);
}

graph_iter_t graph_node_edges_out(graph_txn_t txn, node_t node, logID_t beforeID){
	return _graph_entry_idx(txn, DB_SRCNODE_IDX, node->id, beforeID);
}

graph_iter_t graph_node_edges(graph_txn_t txn, node_t node, logID_t beforeID){
	return graph_iter_concat(2,
		graph_node_edges_in(txn, node, beforeID),
		graph_node_edges_out(txn, node, beforeID));
}

graph_iter_t graph_node_edges_dir(graph_txn_t txn, node_t node, unsigned int direction, logID_t beforeID){
	graph_iter_t it;
	switch(direction){
		case GRAPH_DIR_IN:
			it = graph_node_edges_in(txn, node, beforeID);
			break;
		case GRAPH_DIR_OUT:
			it = graph_node_edges_out(txn, node, beforeID);
			break;
		default:
			it = graph_node_edges(txn, node, beforeID);
	}
	return it;
}

// lookup edges within a node by type
static INLINE graph_iter_t _graph_node_edges_type(graph_txn_t txn, int dbi, logID_t id, strID_t typeID, logID_t beforeID){
	uint8_t kbuf[esizeof(id) + esizeof(typeID)];
	size_t klen = 0;
	encode(id, kbuf, klen);
	encode(typeID, kbuf, klen);
	return graph_iter_new(txn, dbi, kbuf, klen, beforeID);
}

graph_iter_t graph_node_edges_type_in(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	if(graph_string_lookup(txn, &typeID, type, tlen))
		return _graph_node_edges_type(txn, DB_TGTNODE_IDX, node->id, typeID, beforeID);
	return NULL;
}

graph_iter_t graph_node_edges_type_out(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	if(graph_string_lookup(txn, &typeID, type, tlen))
		return _graph_node_edges_type(txn, DB_SRCNODE_IDX, node->id, typeID, beforeID);
	return NULL;
}

graph_iter_t graph_node_edges_type(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	if(graph_string_lookup(txn, &typeID, type, tlen))
		return graph_iter_concat(2,
			_graph_node_edges_type(txn, DB_TGTNODE_IDX, node->id, typeID, beforeID),
			_graph_node_edges_type(txn, DB_SRCNODE_IDX, node->id, typeID, beforeID));
	return NULL;
}

graph_iter_t graph_node_edges_dir_type(graph_txn_t txn, node_t node, unsigned int direction, void *type, size_t tlen, logID_t beforeID){
	graph_iter_t it;
	switch(direction){
		case GRAPH_DIR_IN:
			it = graph_node_edges_type_in(txn, node, type, tlen, beforeID);
			break;
		case GRAPH_DIR_OUT:
			it = graph_node_edges_type_out(txn, node, type, tlen, beforeID);
			break;
		default:
			it = graph_node_edges_type(txn, node, type, tlen, beforeID);
	}
	return it;
}

graph_iter_t graph_props(graph_txn_t txn, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, 0, beforeID);
}

graph_iter_t graph_entry_props(graph_txn_t txn, entry_t entry, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, entry->id, beforeID);
}

graph_iter_t graph_node_props(graph_txn_t txn, node_t node, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, node->id, beforeID);
}

graph_iter_t graph_edge_props(graph_txn_t txn, edge_t edge, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, edge->id, beforeID);
}

graph_iter_t graph_prop_props(graph_txn_t txn, prop_t prop, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, prop->id, beforeID);
}

graph_t graph_open(const char * const path, const int flags, const int mode, const int db_flags){
	int r;
	graph_t g = malloc(sizeof(*g));
	if(g){
		// fixme? padsize hardcoded to 1gb
		// explicitly disable DB_WRITEMAP - graph_txn_reset current depends on nested write txns
		r = db_init((db_t)g, path, flags, mode, db_flags & ~DB_WRITEMAP, DBS, DB_INFO, 1<<30);
		if(r){
			free(g);
			g = NULL;
			errno = r;
		}
	}
	return g;
}

graph_txn_t graph_txn_begin(graph_t g, graph_txn_t parent, unsigned int flags){
	graph_txn_t txn = malloc(sizeof(*txn));
	int r = errno;
	if(txn){
		r = db_txn_init((txn_t)txn, (db_t)g, (txn_t)parent, flags);
		if(DB_SUCCESS == r){
			if(parent){
				// for child write txns, take snapshot of parent data
				memcpy(sizeof(txn->txn) + (unsigned char *)txn,
				       sizeof(txn->txn) + (unsigned char *)parent, sizeof(*txn) - sizeof(txn->txn));
			}else{
				// for parent write txns, we need to harvest the nextID
				txn->next_strID = txn->next_logID = txn->node_delta = txn->edge_delta = 0;
				txn->begin_nextID = TXN_RW(txn) ? _graph_log_nextID(txn, 0) : 0;

				// other prev_* fields are only valid if prev_start is non-zero
				txn->prev_start = 0;
			}
		}else{
			// TODO: log||warn||err?
			free(txn);
			errno = r;
			txn = NULL;
		}
	}
	return txn;
}

static INLINE int _fetch_info(graph_txn_t txn){
	if(!txn->prev_start){
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

int graph_txn_commit(graph_txn_t txn){
	int r;
	graph_txn_t parent;
	txnID_t txnID = 0;
	if(!txn->txn.updated){
		// nothing happened
		graph_txn_abort(txn);
		r = DB_SUCCESS;
	}else if((parent = TXN_PARENT(txn))){
		// nested write txn
		r = txn_commit((txn_t)txn);
		if(DB_SUCCESS == r){
			memcpy(sizeof(txn->txn) + (unsigned char *)parent,
			       sizeof(txn->txn) + (unsigned char *)txn, sizeof(*txn) - sizeof(txn->txn));
		}else if(txn->prev_start != parent->prev_start){
			memcpy(&parent->prev_id, &txn->prev_id, sizeof(*txn) - (intptr_t)&((graph_txn_t)NULL)->prev_id);
		}
		memset(txn, 0, sizeof(*txn));
	}else if(_fetch_info(txn) && txn->next_logID > txn->begin_nextID){
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
		if(DB_SUCCESS == r){
			r = txn_commit((txn_t)txn);
		}else{
			txn_abort((txn_t)txn);
		}
	}else{
		// write txn w/ invalid txnlog table
		r = txn_commit((txn_t)txn);
	}

	free(txn);

	return r;
}

void graph_txn_abort(graph_txn_t txn){
	graph_txn_t parent = TXN_PARENT(txn);
	if(parent)
		memcpy(&parent->prev_id, &txn->prev_id, sizeof(*txn) - (intptr_t)&((graph_txn_t)NULL)->prev_id);
	txn_abort((txn_t)txn);
}

int graph_txn_reset(graph_txn_t txn){
	int r = 1;
	unsigned int i;
	graph_txn_t sub_txn = graph_txn_begin((graph_t)(((txn_t)txn)->db), txn, 0);
	if(sub_txn){
		// truncate all tables
		for(i = 0, r = DB_SUCCESS; i < DBS && DB_SUCCESS == r; i++)
			r = db_drop((txn_t) sub_txn, i, 0);
		if(DB_SUCCESS == r){
			r = graph_txn_commit(sub_txn);
			if(DB_SUCCESS == r){
				txn->begin_nextID = 1;
				txn->next_strID = txn->next_logID = txn->node_delta = txn->edge_delta = txn->prev_start = 0;
			}
		}else{
			graph_txn_abort(sub_txn);
		}
	}
	return r;
}

int graph_txn_updated(graph_txn_t txn){
	return txn_updated((txn_t)txn);
}

int graph_sync(graph_t g, int force){
	int r = db_sync((db_t)g, force);
	if(DB_SUCCESS != r){
		fprintf(stderr, "%d: mdb_env_sync(): %s (%d)\n", (int)getpid(), db_strerror(r), r);
		assert(DB_SUCCESS == r);
	}
	return r;
}

int graph_updated(graph_t g){
	return db_updated((db_t)g);
}

size_t graph_size(graph_t g){
	size_t size;
	int r = db_size((db_t)g, &size);
	return r ? 0 : size;
}

void graph_remap(graph_t g){
	db_remap((db_t)g);
}

void graph_close(graph_t g){
	if(g)
		db_close((db_t)g);
}

static INLINE int _find_txn(graph_txn_t txn, txn_info_t info, logID_t beforeID){
	assert(beforeID && beforeID <= txn->next_logID);
	const logID_t stopID = beforeID - 1;
	int ret = 0;

	uint8_t kbuf[esizeof(txnID_t) + esizeof(logID_t) + esizeof(logID_t)];
	buffer_t data, key = { 0, kbuf };

	// encode magic to query by logID
	encode(0, kbuf, key.size);
	encode(1, kbuf, key.size);
	encode(stopID, kbuf, key.size);

	struct cursor_t c;
	int r =  txn_cursor_init(&c, (txn_t)txn, DB_TXNLOG);
	assert(DB_SUCCESS == r);
	r = cursor_get(&c, &key, &data, DB_SET_KEY);
	size_t i;

	if(DB_SUCCESS == r){
again:
		i = 0;
		decode(info->id, key.data, i);
		decode(info->start, key.data, i);
		decode(info->count, key.data, i);
		assert(key.size == i);

		if(info->start + info->count <= beforeID){
			i = 0;
			decode(info->nodes, data.data, i);
			decode(info->edges, data.data, i);
			assert(data.size == i);
			info->start = info->start + info->count;
		}else if(info->id > 1){
			r = cursor_get(&c, &key, &data, DB_PREV);
			assert(DB_SUCCESS == r);
			goto again;
		}else{
			info->start = 1;
			info->count = 0;
			info->nodes = 0;
			info->edges = 0;
		}
		ret = 1;
	}else if(_fetch_info(txn)){
//		info->id = txn->prev_id;
		info->start = txn->prev_start + txn->prev_count;
//		info->count = txn->next_logID - info->start;
		info->nodes = txn->prev_nodes;
		info->edges = txn->prev_edges;
		ret = 1;
	}

	cursor_close(&c);

	return ret;
}

static INLINE void _nodes_edges_delta(graph_txn_t txn, txn_info_t info, logID_t beforeID){
	uint64_t nodes = info->nodes, edges = info->edges;
	logID_t id = info->start;

	if(id == beforeID)
		return;

	struct cursor_t c;
	int r = txn_cursor_init(&c, (txn_t)txn, DB_LOG);
	assert(DB_SUCCESS == r);

	uint8_t kbuf[esizeof(id)];
	buffer_t data, key = { 0, &kbuf };
	encode(id, kbuf, key.size);

	r = cursor_get(&c, &key, &data, DB_SET_KEY);
	assert(DB_SUCCESS == r);
	while(1){
		uint8_t rectype = *(uint8_t *)data.data;
		int i = 0;

		decode(id, key.data, i);

		if(GRAPH_NODE == rectype){
			nodes++;
		}else if(GRAPH_EDGE == rectype){
			edges++;
		}else if(GRAPH_DELETION == rectype){
			buffer_t d2, k2 = { enclen((uint8_t *)data.data, 1), 1 + (uint8_t *)data.data };
			r = db_get((txn_t)txn, DB_LOG, &k2, &d2);
			assert(DB_SUCCESS == r);
			rectype = *(uint8_t *)d2.data;
			if(GRAPH_NODE == rectype){
				graph_iter_t it = graph_edges(txn, id);
				entry_t e;
				while((e = graph_iter_next(it))){
					free(e);
					edges--;
				}
				nodes--;
			}else if(GRAPH_EDGE == rectype){
				edges--;
			}
		}

		if(++id == beforeID)
			break;

		r = cursor_get(&c, &key, &data, DB_NEXT);
		assert(DB_SUCCESS == r);
	}
	cursor_close(&c);
	info->nodes = nodes;
	info->edges = edges;
}

size_t graph_nodes_count(graph_txn_t txn, logID_t beforeID){
	size_t count = 0;

	const logID_t nextID = _graph_log_nextID(txn, 0);
	if(!beforeID || beforeID > nextID)
		beforeID = nextID;

	if(1 == beforeID)
		goto done;

	struct txn_info_t info;
	if(_find_txn(txn, &info, beforeID)){
		_nodes_edges_delta(txn, &info, beforeID);
		count = info.nodes;
		goto done;
	}

	// fall back to scanning the nodes index
	graph_iter_t iter = graph_nodes(txn, beforeID);
	entry_t e;
	while((e = graph_iter_next(iter))){
		free(e);
		count++;
	}

done:
	return count;
}

size_t graph_edges_count(graph_txn_t txn, logID_t beforeID){
	size_t count = 0;

	const logID_t nextID = _graph_log_nextID(txn, 0);
	if(!beforeID || beforeID > nextID)
		beforeID = nextID;

	if(1 == beforeID)
		goto done;

	struct txn_info_t info;
	if(_find_txn(txn, &info, beforeID)){
		_nodes_edges_delta(txn, &info, beforeID);
		count = info.edges;
		goto done;
	}

	// fall back to scanning the edges index for old graphs
	graph_iter_t iter = graph_edges(txn, beforeID);
	entry_t e;
	while((e = graph_iter_next(iter))){
		free(e);
		count++;
	}

done:
	return count;
}

// given logID, find txn that it was a part of
// return beforeID that would include the entire txn
logID_t graph_snap_id(graph_txn_t txn, logID_t id){
	logID_t beforeID = txn->next_logID;
	uint8_t kbuf[esizeof(txnID_t) + esizeof(logID_t) + esizeof(logID_t)];
	buffer_t data, key = { 0, kbuf };
	struct txn_info_t info;

	// encode magic to query by logID
	encode(0, kbuf, key.size);
	encode(1, kbuf, key.size);
	encode(id, kbuf, key.size);

	struct cursor_t c;
	int r = txn_cursor_init(&c, (txn_t)txn, DB_TXNLOG);
	assert(DB_SUCCESS == r);

	r = cursor_get(&c, &key, &data, DB_SET_KEY);
	if(DB_SUCCESS == r){
		int i = 0;
		decode(info.id, key.data, i);
		decode(info.start, key.data, i);
		decode(info.count, key.data, i);
		beforeID = info.start + info.count;
	}
	cursor_close(&c);
	return beforeID;
}


int graph_fd(graph_t g){
	return g->db.fd;
}




db_snapshot_t graph_snapshot_new(graph_t g, int compact){
    return db_snapshot_new((db_t)g, compact);
}

int graph_set_mapsize(graph_t g, size_t mapsize){
    return db_set_mapsize((db_t)g, mapsize);
}

size_t graph_get_mapsize(graph_t g){
	size_t size;
	int r = db_get_mapsize((db_t)g, &size);
	return r ? 0 : size;
}

size_t graph_get_disksize(graph_t g){
	size_t size;
    int r = db_get_disksize((db_t)g, &size);
    return r ? 0 : size;
}



void _graph_prop_print(graph_txn_t txn,  prop_t prop) {
    size_t klen;
    size_t vlen;
    char * key = graph_string(txn, prop->key, &klen);
    char * val = graph_string(txn, prop->val, &vlen);
    printf(", \"%s\":\"%s\"", key, vlen ? val : ""); // TODO:
}

void _graph_props_print(graph_txn_t txn, entry_t entry, logID_t beforeID) {
    graph_iter_t props = graph_entry_props(txn, entry, beforeID);
    prop_t prop;
    while ((prop = (prop_t) graph_iter_next(props))) {
        _graph_prop_print(txn, prop);
        free(prop);
    }
    graph_iter_close(props);
}

void graph_node_print(graph_txn_t txn, node_t node, logID_t beforeID) {
    size_t tlen;
    size_t vlen;
    char * type = graph_string(txn, node->type, &tlen);
    char * val = graph_string(txn, node->val, &vlen);
    printf("{\"ID\":%i, \"TYPE\":\"%s\", \"VAL\":\"%s\"",
        node->id, type, val);
    _graph_props_print(txn, (entry_t)node, beforeID);
    puts("}");
}

void graph_edge_print(graph_txn_t txn, edge_t edge, logID_t beforeID) {
    size_t tlen;
    size_t vlen;
    char * type = graph_string(txn, edge->type, &tlen);
    char * val = graph_string(txn, edge->val, &vlen);
    printf("{\"ID\":%i, \"SRC\":%i, \"TGT\":%i, \"TYPE\":\"%s\", \"VAL\":\"%s\" ",
        edge->id, edge->src, edge->tgt, type, val);
    _graph_props_print(txn, (entry_t)edge, beforeID);
    puts("}");
}

void graph_nodes_print(graph_iter_t nodes) {
    node_t node;
    while ((node = (node_t) graph_iter_next(nodes))) {
        graph_node_print(nodes->txn, node, nodes->beforeID);
        free(node);
    }
    graph_iter_close(nodes);
    free(nodes);
}

void graph_edges_print(graph_iter_t edges) {
    edge_t edge;
    while ((edge = (edge_t) graph_iter_next(edges))) {
        graph_edge_print(edges->txn, edge, edges->beforeID);
        free(edge);
    }
    graph_iter_close(edges);
    free(edges);
}