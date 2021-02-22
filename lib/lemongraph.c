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
#include"lemongraph.h"

STATIC_ASSERT(sizeof(LG_id) == sizeof(txnID_t), "");
STATIC_ASSERT(sizeof(LG_id) == sizeof(logID_t), "");
STATIC_ASSERT(sizeof(LG_id) == sizeof(strID_t), "");


char *graph_strerror(int err){
	return db_strerror(err);
}

static INLINE uint8_t *__lookup(graph_txn_t txn, entry_t e, const int db_idx, uint8_t *kbuf, size_t klen, const logID_t beforeID){
	struct cursor_t idx;
	int r = txn_cursor_init(&idx, (txn_t)txn, db_idx);
	assert(DB_SUCCESS == r);

	buffer_t key = { klen, kbuf };
	buffer_t data = { 0, NULL };
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
		uint8_t tmp[LOG_MAX_BUF_SIZE];
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
	ggprop_t p = { .pid = id, .rectype = GRAPH_PROP };
	if(ggblob_resolve(txn, &p.key, key, klen, 1)){
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
	node_t e = smalloc(sizeof(*e));
	if(ggblob_resolve(txn, &e->type, type, tlen, readonly) &&
	   ggblob_resolve(txn, &e->val, val, vlen, readonly) &&
	   __node_resolve(txn, e, beforeID, readonly) ){
		return e;
	}
	free(e);
	return NULL;
}

static INLINE edge_t _edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
	edge_t e = smalloc(sizeof(*e));
	assert(src && tgt);
	e->src = src->id;
	e->tgt = tgt->id;
	if(ggblob_resolve(txn, &e->type, type, tlen, readonly) &&
	   ggblob_resolve(txn, &e->val, val, vlen, readonly) &&
	   __edge_resolve(txn, e, beforeID, readonly) ){
		return e;
	}
	free(e);
	return NULL;
}

static INLINE prop_t _prop_resolve(graph_txn_t txn, entry_t parent, void *key, size_t klen, void *val, size_t vlen, logID_t beforeID, int readonly){
	prop_t e = smalloc(sizeof(*e));
	e->pid = parent->id;
	if(ggblob_resolve(txn, &e->key, key, klen, readonly) &&
	   ggblob_resolve(txn, &e->val, val, vlen, readonly) &&
	   __prop_resolve(txn, e, beforeID, readonly)){
		return e;
	}
	free(e);
	return NULL;
}

entry_t graph_entry(graph_txn_t txn, const logID_t id){
	static const int recsizes[] = {
		[GRAPH_DELETION] = sizeof(ggentry_t),
		[GRAPH_NODE]     = sizeof(ggnode_t),
		[GRAPH_EDGE]     = sizeof(ggedge_t),
		[GRAPH_PROP]     = sizeof(ggprop_t),
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


int graph_string_lookup(ggtxn_t* txn, strID_t *id, void const *data, const size_t len){
    return ggblob_resolve(txn, id, data, len, 1);
}

int graph_string_resolve(ggtxn_t* txn, strID_t *id, void const *data, const size_t len){
	return ggblob_resolve(txn, id, data, len, 0);
}


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
	static ggentry_t top = { .id = 0, .next = 0 };
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
	static ggentry_t parent = { .id = 0 };
	return _prop_resolve(txn, &parent, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_set(graph_txn_t txn, void *key, size_t klen, void *val, size_t vlen){
	static ggentry_t parent = { .id = 0 };
	return _prop_resolve(txn, &parent, key, klen, val, vlen, 0, 0);
}

void graph_unset(graph_txn_t txn, void *key, size_t klen){
	_entry_unset(txn, 0, key, klen);
}

// logID_t graph_ID_set(graph_txn_t txn, logID_t parent_id, prop_t e){
logID_t graph_ID_set(graph_txn_t txn, logID_t parent_id, strID_t key, strID_t val){
	ggprop_t e = {
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
	ggnode_t e = {
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
	ggedge_t e = {
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



graph_iter_t graph_iter_new(graph_txn_t txn, int dbi, void *pfx, size_t pfxlen, logID_t beforeID){
	graph_iter_t gi = smalloc(sizeof(*gi));
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
	uint8_t pfx[esizeof(typeID)];
	size_t pfxlen = 0;
	graph_iter_t iter = NULL;
	if(graph_string_lookup(txn, &typeID, type, tlen)){
		encode(typeID, pfx, pfxlen);
		iter = graph_iter_new(txn, dbi, pfx, pfxlen, beforeID);
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
	graph_t g = smalloc(sizeof(*g));
	if(g){
		// fixme? padsize hardcoded to 1gb
		// TODO: explicitly disable DB_WRITEMAP - graph_txn_reset current depends on nested write txns
		r = db_init((db_t)g, path, flags, mode,
			db_flags,// & ~DB_WRITEMAP,
			DBS, DB_INFO, 1<<30);
		if(r){
			free(g);
			g = NULL;
			errno = r;
		}
	}
	return g;
}

graph_txn_t graph_txn_begin(graph_t g, graph_txn_t parent, unsigned int flags){
	graph_txn_t txn = smalloc(sizeof(*txn));
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

int graph_txn_commit(graph_txn_t txn) {
	int r;
	ggtxn_commit(txn);
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
	}else if(_ggtxn_update_info(txn)){
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