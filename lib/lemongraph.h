#ifndef _LEMONGRAPH_H
#define _LEMONGRAPH_H

#include "lg.h"

typedef ggraph_t* graph_t;
typedef ggtxn_t* graph_txn_t;
typedef ggiter_t* graph_iter_t;
typedef ggentry_t* entry_t;
typedef ggentry_t* deletion_t;
typedef ggnode_t* node_t;
typedef ggedge_t* edge_t;
typedef ggprop_t* prop_t;

char *graph_strerror(int err);

graph_t graph_open(const char * const path, const int flags, const int mode, int db_flags);
int graph_sync(graph_t g, int force);
int graph_updated(graph_t g);
size_t graph_size(graph_t g);
void graph_remap(graph_t g);
int graph_fd(graph_t g);
void graph_close(graph_t g);

graph_txn_t graph_txn_begin(graph_t g, graph_txn_t parent, unsigned int flags);
#define 	graph_txn_begin_ro(graph) graph_txn_begin(graph, NULL, DB_RDONLY)
#define 	graph_txn_begin_rw(graph) graph_txn_begin(graph, NULL, 0)
int graph_txn_updated(graph_txn_t txn);
int graph_txn_reset(graph_txn_t txn);
int graph_txn_commit(graph_txn_t txn);
void graph_txn_abort(graph_txn_t txn);

db_snapshot_t graph_snapshot_new(graph_t g, int compact);
int graph_set_mapsize(graph_t g, size_t mapsize);
size_t graph_get_mapsize(graph_t g);
size_t graph_get_disksize(graph_t g);

// fetch entities by logID
entry_t graph_entry(graph_txn_t txn, const logID_t id);
prop_t graph_prop(graph_txn_t txn, const logID_t id);
node_t graph_node(graph_txn_t txn, const logID_t id);
edge_t graph_edge(graph_txn_t txn, const logID_t id);

// returns highest logID affecting an entry - self, deletion, or latest property (or prop deletion)
logID_t graph_updateID(graph_txn_t txn, logID_t beforeID);
logID_t graph_entry_updateID(graph_txn_t txn, entry_t e, logID_t beforeID);
logID_t graph_node_updateID(graph_txn_t txn, node_t n, logID_t beforeID);
logID_t graph_edge_updateID(graph_txn_t txn, edge_t e, logID_t beforeID);
logID_t graph_prop_updateID(graph_txn_t txn, prop_t p, logID_t beforeID);

// returns beforeID for entire transaction that given id was written in
logID_t graph_snap_id(graph_txn_t txn, logID_t id);

// get properties
prop_t graph_get(graph_txn_t txn, void *key, size_t klen, logID_t beforeID);
prop_t graph_node_get(graph_txn_t txn, node_t node, void *key, size_t klen, logID_t beforeID);
prop_t graph_edge_get(graph_txn_t txn, edge_t edge, void *key, size_t klen, logID_t beforeID);
prop_t graph_prop_get(graph_txn_t txn, prop_t prop, void *key, size_t klen, logID_t beforeID);

// set properties
prop_t graph_set(graph_txn_t txn, void *key, size_t klen, void *val, size_t vlen);
prop_t graph_node_set(graph_txn_t txn, node_t node, void *key, size_t klen, void *val, size_t vlen);
prop_t graph_edge_set(graph_txn_t txn, edge_t edge, void *key, size_t klen, void *val, size_t vlen);
prop_t graph_prop_set(graph_txn_t txn, prop_t prop, void *key, size_t klen, void *val, size_t vlen);

// unset properties
void graph_unset(graph_txn_t txn, void *key, size_t klen);
void graph_node_unset(graph_txn_t txn, node_t e, void *key, size_t klen);
void graph_edge_unset(graph_txn_t txn, edge_t e, void *key, size_t klen);
void graph_prop_unset(graph_txn_t txn, prop_t e, void *key, size_t klen);

// query node/edge
node_t graph_node_lookup(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID);
edge_t graph_edge_lookup(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID);

// resolve node/edge
node_t graph_node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen);
edge_t graph_edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen);

logID_t node_resolve(graph_txn_t txn, node_t e, strID_t type, strID_t val);
logID_t edge_resolve(graph_txn_t txn, edge_t e, logID_t src, logID_t tgt, strID_t type, strID_t val);

// resolve ids
logID_t graph_nodeID_resolve(graph_txn_t txn, strID_t type, strID_t val);
logID_t graph_edgeID_resolve(graph_txn_t txn, logID_t src, logID_t tgt, strID_t type, strID_t val);
logID_t graph_ID_set(graph_txn_t txn, logID_t parent_id, strID_t key, strID_t val);

// logID_t graph_node_id_resolve(graph_txn_t txn, node_t e);
// logID_t graph_edge_id_resolve(graph_txn_t txn, edge_t e);
// logID_t graph_ID_set(graph_txn_t txn, logID_t parent_id, prop_t e);

// count nodes/edges
size_t graph_nodes_count(graph_txn_t txn, logID_t beforeID);
size_t graph_edges_count(graph_txn_t txn, logID_t beforeID);

// delete any type of graph entity
logID_t graph_delete(graph_txn_t txn, entry_t e);

// iterator foo - be sure to close them before aborting or commiting a txn
graph_iter_t graph_nodes(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_edges(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_nodes_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_edges_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_edges_type_value(graph_txn_t txn, void *type, size_t tlen, void *value, size_t vlen, logID_t beforeID);
graph_iter_t graph_node_edges_in(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_node_edges_out(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_node_edges(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_node_edges_dir(graph_txn_t txn, node_t node, unsigned int direction, logID_t beforeID);
graph_iter_t graph_node_edges_type_in(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_type_out(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_type(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_dir_type(graph_txn_t txn, node_t node, unsigned int direction, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_props(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_entry_props(graph_txn_t txn, entry_t entry, logID_t beforeID);
graph_iter_t graph_node_props(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_edge_props(graph_txn_t txn, edge_t edge, logID_t beforeID);
graph_iter_t graph_prop_props(graph_txn_t txn, prop_t prop, logID_t beforeID);
entry_t graph_iter_next(graph_iter_t iter);
void graph_iter_close(graph_iter_t iter);

char *graph_string(graph_txn_t txn, strID_t id, size_t *len);
int graph_string_lookup(graph_txn_t txn, strID_t *id, void const *data, const size_t len);
int graph_string_resolve(graph_txn_t txn, strID_t *id, void const *data, const size_t len);
logID_t graph_log_nextID(graph_txn_t txn);

void graph_node_print(graph_txn_t txn, node_t node, logID_t beforeID);
void graph_edge_print(graph_txn_t txn, edge_t edge, logID_t beforeID);
void graph_nodes_print(graph_iter_t nodes);
void graph_edges_print(graph_iter_t edges);

// helpers for serializing/unserializing tuples of non-negative integers
int pack_uints(int count, uint64_t *ints, void *buffer);
int unpack_uints(int count, uint64_t *ints, void *buffer);
int unpack_uints2(int count, uint64_t *ints, void *buffer, size_t buflen);
int pack_uint(uint64_t i, char *buffer);
uint64_t unpack_uint(char *buffer);


#endif
