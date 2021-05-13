#ifndef _LG_KV_HEADER
#define _LG_KV_HEADER

// kv storage api - domains get mapped to stringIDs via the string storage layer
// so do keys and values if LG_KV_MAP_KEYS or LG_KV_MAP_DATA are set
// non-mapped keys must be fairly short (less than 500 bytes is safe)
// flags are not stored internally - client must know per domain
// note - related kv & kv_iter objects share buffers - do not use concurrently from multiple threads

#include "counter.h"
#include "serdes.h"

// kv flags
#define LG_KV_RO       0x1
#define LG_KV_MAP_KEYS 0x2
#define LG_KV_MAP_DATA 0x4

#define LG_KV_KEY_BUF_SIZE 511



typedef struct
LG_bufs {
	uint32_t len;
	LG_buf*	 buf;
} LG_bufs;

#define LG_fifo LG_kv
#define LG_pq LG_kv
#define LG_ob LG_kv
#define LG_kv_iter ggkv_iter_t
#define LG_pq_iter ggkv_iter_t

typedef struct
LG_kv {
	LG_txn* txn;
	LG_buf key;
	LG_buf val;
	int flags;
	uint32_t refs;
	uint32_t klen;
	uint8_t kbuf[LG_KV_KEY_BUF_SIZE];
	// uint8_t vbuf[511];
} LG_kv;

typedef struct
ggkv_iter_t {
	union {
		struct iter_t iter;
		struct { // NOTE: struct iter_t members replicated for iter.key/.val
			struct cursor_t cursor;
			LG_buf key;
			union {
				LG_buf val;
				LG_buf data;
			};
			// void *pfx;
			// unsigned int pfxlen;
			// db_cursor_op op;
			// int r;
			// int release : 1;
		};
	};
	LG_kv* kv;
} ggkv_iter_t;

typedef struct
LG_pq_cursor {
	#define LG_PQ_CURSOR_SIZE 512
	size_t size;
	union {
		uint8_t u8[LG_PQ_CURSOR_SIZE];
		void* data;
	};
	// ^ buf compat end
	LG_buf val;

} LG_pq_cursor;

// kv
void    lg_kv_init(LG_kv* kv, LG_txn* txn, LG_id domain_id, const int flags);
void 	lg_kv_key(LG_kv* kv, LG_buf* key);
int 	lg_kv_put(LG_kv* kv, LG_buf* val);
int 	lg_kv_get(LG_kv* kv, LG_buf* val);
int 	lg_kv_del(LG_kv* kv);
int 	lg_kv_clear(LG_kv* kv); // => clear count
int 	lg_kv_clear_pfx(LG_kv* kv, LG_buf* pfx); // => clear count
int 	lg_kv_next(LG_kv* kv, LG_buf* key, LG_buf* val);
int 	lg_kv_next_reset(LG_kv* kv);
int 	lg_kv_first_key(LG_kv* kv, LG_buf* key);
int 	lg_kv_last_key(LG_kv* kv, LG_buf* key);
int 	lg_kv_empty(LG_kv* kv);

#define lg_kv_key_reset(kv) \
{ \
	kv->key.data = kv->kbuf; \
    kv->key.size = kv->klen; \
}

//TODO:
int     ggkv_get(LG_kv* kv, pod_t* key, pod_t* val);

// kv iterators
int		lg_kv_iter(LG_kv* kv, LG_kv_iter* iter);
int		lg_kv_iter_pfx(LG_kv* kv, LG_kv_iter* iter, LG_buf* pfx);
bool	lg_kv_iter_next(LG_kv_iter* iter);
// int 	lg_kv_iter_seek(LG_kv_iter* iter, LG_buf* key);
void	lg_kv_iter_close(LG_kv_iter* iter);

#define LG_KV_EACH(kv, iter, CODE) \
	lg_kv_iter(kv, iter); \
	while (lg_kv_iter_next(iter)) { \
		CODE \
	} \
	lg_kv_iter_close(iter);

#define LG_KV_EACH_PFX(kv, iter, pfx, CODE) \
	lg_kv_iter_pfx(kv, iter, pfx); \
	while (lg_kv_iter_next(iter)) { \
		CODE \
	} \
	lg_kv_iter_close(iter);

// // kv fifos
void    lg_fifo_init(LG_kv* kv, LG_txn* txn, LG_id domain_id, const int flags);
int 	lg_fifo_push(LG_kv* kv, LG_buf* val);
int 	lg_fifo_push_n(LG_kv* kv, LG_buf* vals, const int n);
int 	lg_fifo_push_uint(LG_kv* kv, LG_uint val);
int 	lg_fifo_push_uint_n(LG_kv* kv, LG_uint* vals, const int n);
int		lg_fifo_peek(LG_kv* kv, LG_buf* val);
int		lg_fifo_peek_n(LG_kv* kv, LG_buf* vals, const int count);
int		lg_fifo_peek_uint(LG_kv* kv, LG_uint* x);
int		lg_fifo_peek_uint_n(LG_kv* kv, LG_uint* vals, const int count);
// int 	lg_fifo_pop(LG_kv* kv, LG_buf* val);
// int	lg_fifo_pop_n(LG_kv* kv, LG_buf* vals, const int count);
int		lg_fifo_pop_uint(LG_kv* kv, LG_uint* val);
int		lg_fifo_pop_uint_n(LG_kv* kv, LG_uint* vals, const int count);
#define lg_fifo_del(kv)		lg_fifo_del_n(kv, 1)
int 	lg_fifo_del_n(LG_kv* kv, const int count);
int 	lg_fifo_len(LG_kv* kv, LG_uint* len);
#define lg_fifo_clear(kv)	lg_kv_clear(kv)
#define lg_fifo_empty(kv)	lg_kv_empty(kv)

// kv priority queues
// we store two different structures under a domain:
//   first:  enc(domID), 0, priority, counter => key
//   second: enc(domID), 1, key => priority, counter
// priority as well as the 0/1 are literal bytes
// counter is up-to 256 bytes - can increment/decrement from [0 .. ((1<<2040)-1)]
void    lg_pq_init(LG_kv* kv, LG_txn* txn, LG_id domain_id, const int flags);
int 	lg_pq_add(LG_kv* kv, LG_buf* val, uint8_t priority); // TODO: ret??
int 	lg_pq_get(LG_kv* kv, LG_buf* val, uint8_t *priority);
int 	lg_pq_del(LG_kv* kv, LG_buf* val);
#define lg_pq_clear(kv)		lg_kv_clear(kv)
#define lg_pq_empty(kv)		lg_kv_empty(kv)
// pq iterator
int		lg_pq_iter(LG_kv* kv, LG_pq_iter* iter);
bool	lg_pq_iter_next(LG_pq_iter* iter);
#define lg_pq_iter_close(it) lg_kv_iter_close(it)
#define LG_PQ_EACH(kv, iter, CODE) \
	lg_pq_iter(kv, iter); \
	while (lg_pq_iter_next(iter)) { \
		CODE \
	} \
	lg_pq_iter_close(iter);
// pq (persitable) cursor
void 	lg_pq_cursor_init(LG_kv* kv, LG_pq_cursor* pq_cursor, uint8_t priority);
bool 	lg_pq_cursor_next(LG_txn* txn, LG_pq_cursor* pq_cursor, LG_buf* val, uint8_t* priority);
void 	lg_pq_cursor_close(LG_pq_cursor* pq_cursor);

// void	lg_v_reset(LG_kv* kv);
// void	lg_k_write_str(LG_kv* kv, const char* str);
// void	lg_k_write_buf(LG_kv* kv, const char* buf, size_t len);
// void	lg_k_write_uint(LG_kv* kv, const char* str);
// void	lg_v_write_str(LG_kv* kv, const char* str);
// void	lg_v_write_buf(LG_kv* kv, const char* buf, size_t len);
// void	lg_v_write_uint(LG_kv* kv, uint64_t x);
// void	lg_v_read_str(LG_kv* kv, const char* str);
// void	lg_v_read_buf(LG_kv* kv, const char* buf, size_t* len);
// void	lg_v_read_uint(LG_kv* kv, uint64_t* x);

// (compat)
typedef LG_kv* kv_t;
typedef ggkv_iter_t* kv_iter_t;
// kv
kv_t graph_kv(LG_txn* txn, const void *domain, const size_t dlen, const int flags);
void *kv_get(kv_t kv, void *key, size_t klen, size_t *dlen);
void *kv_first_key(kv_t kv, size_t *klen);
void *kv_last_key(kv_t kv, size_t *len);
int kv_del(kv_t kv, void *key, size_t klen);
int kv_put(kv_t kv, void *key, size_t klen, void *data, size_t dlen);
int kv_next(kv_t kv, void **key, size_t *klen, void **data, size_t *dlen);
int kv_next_reset(kv_t kv);
int kv_clear_pfx(kv_t kv, uint8_t *pfx, unsigned int len);
int kv_clear(kv_t kv);
void kv_deref(kv_t kv);
// kv iterators
kv_iter_t kv_iter(kv_t kv);
kv_iter_t kv_iter_pfx(kv_t kv, uint8_t *pfx, unsigned int len);
int kv_iter_next(kv_iter_t iter, void **key, size_t *klen, void **data, size_t *dlen);
int kv_iter_seek(kv_iter_t iter, void *key, size_t klen);
void kv_iter_close(kv_iter_t iter);
// kv fifos
int kv_fifo_push_n(kv_t kv, void **datas, size_t *lens, const int count);
int kv_fifo_push(kv_t kv, void *data, size_t len);
int kv_fifo_peek_n(kv_t kv, void **datas, size_t *lens, const int count);
int kv_fifo_peek(kv_t kv, void **data, size_t *size);
int kv_fifo_delete(kv_t kv, const int count);
int kv_fifo_len(kv_t kv, uint64_t *len);
// kv priority queues
int kv_pq_add(kv_t kv, void *key, size_t klen, uint8_t priority);
int kv_pq_get(kv_t kv, void *key, size_t klen);
int kv_pq_del(kv_t kv, void *key, size_t klen);
kv_iter_t kv_pq_iter(kv_t kv);
int kv_pq_iter_next(kv_iter_t iter, void **data, size_t *dlen);
uint8_t *kv_pq_cursor(kv_t kv, uint8_t priority);
int kv_pq_cursor_next(LG_txn* txn, uint8_t *cursor, void **key, size_t *klen);
void kv_pq_cursor_close(uint8_t *cursor);



#ifdef LG_IMPLEMENTATION



void lg_kv_init(LG_kv* kv, LG_txn* txn, LG_id domain_id, const int flags)
{
    kv->txn = txn;
    kv->flags = flags;
    kv->refs = 1;
    kv->klen = 0;
    // TODO: docs on ggkv as perf-oriented/unsafe
    // if ( !pod_resolve(txn, domain, (flags & LG_KV_RO)) )
    //     return 0;
    encode(domain_id, kv->kbuf, kv->klen);
    // return 1;
}

kv_t graph_kv(LG_txn* txn, const void *domain, const size_t dlen, const int flags)
{
	// kv_t kv = NULL;
	// strID_t domainID;
	// if (!ggblob_resolve(txn, &domainID, domain, dlen, (TXN_RO(txn) || (flags & LG_KV_RO))))
	// 	goto FAIL;
	// kv = smalloc(sizeof(*kv));
	// if (!kv)
	// 	goto FAIL;
	// kv->txn = txn;
	// kv->flags = flags;
	// kv->refs = 1;
	// kv->klen = 0;
	// encode(domainID, kv->kbuf, kv->klen);
	LG_kv* kv = NULL;
	pod_t d = pod_buf(domain, dlen);
	if ( !pod_resolve(txn, &d, (flags & LG_KV_RO)) )
        goto FAIL;
	kv = smalloc(sizeof(*kv));
	if (!kv)
		goto FAIL;
	lg_kv_init(kv, txn, d.id, flags);
	return kv;
FAIL:
	if(kv)
		free(kv);
	return NULL;
}

void
lg_kv_key(LG_kv* kv, LG_buf* key)
{
	lg_kv_key_reset(kv);
	assert(key && LG_KV_KEY_BUF_SIZE >= kv->key.size + key->size);
	kv->key.size += key->size;
	memcpy(&kv->kbuf[kv->klen], key->data, key->size);
}

int
lg_kv_put(LG_kv* kv, LG_buf* val)
{
	// kv->val.size = val->size;
	// kv->val.data = val->data;
	// r = db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->val, 0);
	int r = db_put((txn_t)kv->txn, DB_KV, &kv->key, val, 0);
	return r;
}

int
lg_kv_get(LG_kv* kv, LG_buf* val)
{
    int r = db_get((txn_t)kv->txn, DB_KV, &kv->key, val);
	// if UNLIKELY(r != DB_SUCCESS) {
	// 	val->size = 0;
    // 	val->data = NULL;
	// }
    return r;
}

int
lg_kv_del(LG_kv* kv)
{
	return db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL);
}

int // => clear count
lg_kv_clear(LG_kv* kv)
{
	int count = 0;
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	LG_buf k;
	r = cursor_first_key(&cursor, &k, NULL, 0);
	while (DB_SUCCESS == r) {
		cursor_del(&cursor, 0);
		r = cursor_first_key(&cursor, &k, NULL, 0);
		++count;
	}
	cursor_close(&cursor);
	return count;
}

int // => clear count
lg_kv_clear_pfx(LG_kv* kv, LG_buf* pfx)
{
	assert(pfx);
	int count = 0;
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	uint32_t len = kv->klen + pfx->size;
	uint8_t buf[len];
	memcpy(buf, kv->kbuf, kv->klen);
	memcpy(buf + kv->klen, pfx->data, pfx->size);
	LG_buf k;
	r = cursor_first_key(&cursor, &k, buf, len);
	while (DB_SUCCESS == r) {
		cursor_del(&cursor, 0);
		r = cursor_first_key(&cursor, &k, buf, len);
		++count;
	}
	cursor_close(&cursor);
	return count;
}

int
lg_kv_next(LG_kv* kv, LG_buf* key, LG_buf* val)
// void **key, size_t *klen, void **data, size_t *dlen
{
	int r;
	int ret = 0;
	LG_buf bmk = { .size = kv->klen, .data = kv->kbuf };
	LG_buf pos;
	struct cursor_t c;
	txn_t txn = (txn_t)kv->txn;
	r = txn_cursor_init(&c, txn, DB_KV);
	assert(DB_SUCCESS == r);
	// try to fetch the bookmark from where we left off
	r = db_get(txn, DB_KVBM, &bmk, &pos);
	if (DB_SUCCESS == r) {
		void *found = pos.data;
		size_t flen = pos.size;
		r = cursor_get(&c, &pos, NULL, DB_SET_RANGE);
		// step forward if we found it exactly
		if (DB_SUCCESS == r
			&& flen == pos.size
			&& memcmp(found, pos.data, flen) == 0 )
		{
			r = cursor_get(&c, &pos, NULL, DB_NEXT);
		}
		// if we've run off the end, set error status
		if (DB_SUCCESS == r
			&& (pos.size < kv->klen
				|| memcmp(pos.data, bmk.data, kv->klen) ) )
		{
			r = DB_NOTFOUND;
		}
	}
	// was there no bookmark?
	// or did set_range fail?
	// or did we run off the end?
	if (DB_SUCCESS != r) {
		// fall back to start of kv range
		memcpy(&pos, &bmk, sizeof(bmk));
		r = cursor_get(&c, &pos, NULL, DB_SET_RANGE);
	}
	// nothing to do?
	if (DB_SUCCESS != r
		|| pos.size < kv->klen
		|| memcmp(pos.data, bmk.data, kv->klen) )
	{
		goto bail;
	}
	{
		// stash a copy of the key
		// FIXME: do we need to make a copy?
		uint8_t kbuf[pos.size];
		memcpy(kbuf, pos.data, pos.size);
		// update the bookmark
		r = db_put(txn, DB_KVBM, &bmk, &pos, 0);
		if (DB_SUCCESS != r)
			goto bail;
		// now go and fetch actual key & data
		pos.data = kbuf;
		assert(pos.size >= kv->klen);
		val->data = NULL;
		r = cursor_get(&c, &pos, val, DB_SET_KEY);
		assert(val->data);
		if (DB_SUCCESS == r) {
			// if (kv->flags & LG_KV_MAP_KEYS) {
			// 	*key = graph_string_enc(kv->txn, pos.data + kv->klen, klen);
			// } else {
				key->size = pos.size - kv->klen;
				key->data = pos.data + kv->klen;
			// }
			// if (kv->flags & LG_KV_MAP_DATA) {
			// 	*data = graph_string_enc(kv->txn, val.data, dlen);
			// } else {
				// *data = val.data;
				// *dlen = val.size;
			// }
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

int
lg_kv_next_reset(LG_kv* kv)
{
	LG_buf bmk = { .size = kv->klen, .data = kv->kbuf };
	int r = db_del((txn_t)kv->txn, DB_KVBM, &bmk, NULL);
	r = (DB_SUCCESS == r || DB_NOTFOUND == r)
		? DB_SUCCESS
		: r;
	return r;
}

int
lg_kv_first_key(LG_kv* kv, LG_buf* key)
{
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_first_key(&cursor, key, kv->kbuf, kv->klen);
	if LIKELY(DB_SUCCESS == r) {
		key->size -= kv->klen;
		key->data += kv->klen;
	} // else { key->size = 0; key->data = NULL; }
	cursor_close(&cursor);
	return r;
}

int
lg_kv_last_key(LG_kv* kv, LG_buf* key)
{
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_last_key(&cursor, key, kv->kbuf, kv->klen);
	if LIKELY(DB_SUCCESS == r) {
		key->size -= kv->klen;
		key->data += kv->klen;
	} // else { key->size = 0; key->data = NULL; }
	cursor_close(&cursor);
	return r;
}

int
lg_kv_empty(LG_kv* kv)
{
	LG_buf key;
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_first_key(&cursor, &key, kv->kbuf, kv->klen);
	int empty = (DB_SUCCESS != r);
	cursor_close(&cursor);
	return empty;
}

int
lg_kv_iter(kv_t kv, LG_kv_iter* iter)
{
	int r;
	r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, kv->kbuf, kv->klen);
	if LIKELY(DB_SUCCESS == r) {
		iter->kv = kv;
		kv->refs++;
	} else
		errno = r;
	return r;
}

int
lg_kv_iter_pfx(kv_t kv, LG_kv_iter* iter, LG_buf* pfx)
{
	int r;
	if LIKELY(pfx) {
		assert((kv->flags & LG_KV_MAP_KEYS) == 0);//TODO:
		uint8_t buf[kv->klen + pfx->size];
		memcpy(buf, kv->kbuf, kv->klen);
		memcpy(buf + kv->klen, pfx->data, pfx->size);
		r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, buf, kv->klen + pfx->size);
	} else {
		r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, kv->kbuf, kv->klen);
	}
	if LIKELY(DB_SUCCESS == r) {
		iter->kv = kv;
		kv->refs++;
	} else
		errno = r;
	return r;
}

bool
lg_kv_iter_next(LG_kv_iter* iter)
{
	const bool ret = (DB_SUCCESS == iter_next((iter_t)iter));
	if (ret) {
		// clean kv domain from key
		const uint32_t klen = iter->kv->klen;
		iter->key.data += klen;
		iter->key.size -= klen;
	}
	return ret;
}

void
lg_kv_iter_close(LG_kv_iter* iter)
{
	--iter->kv->refs;
	iter_close((iter_t)iter);
}


static INLINE int
_ggkv_setup_key(LG_kv* kv, void *key, size_t klen, int readonly)
{
    strID_t id;
    kv->key.data = kv->kbuf;
    kv->key.size = kv->klen;
    if (kv->flags & LG_KV_MAP_KEYS) {
        if (!ggblob_resolve(kv->txn, &id, key, klen, readonly))
            return 0;
        encode(id, kv->kbuf, kv->key.size);
    } else {
        assert(klen <= sizeof(kv->kbuf) - kv->klen);
        memcpy(&kv->kbuf[kv->klen], key, klen);
        kv->key.size += klen;
    }
    return 1;
}


int ggkv_get(LG_kv* kv, pod_t* key, pod_t* val)
{
    val->id = 0;
    if (!_ggkv_setup_key(kv, key->data, key->size, 1))
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

void *kv_get(kv_t kv, void *key_data, size_t key_size, size_t *val_size)
{
	pod_t val;
	pod_t key = pod_buf(key_data, key_size);
	ggkv_get(kv, &key, &val);
	*val_size = val.size;
	return val.data;

// 	void *data = NULL;
// 	if (!_ggkv_setup_key(kv, key, klen, 1))
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



void
lg_fifo_init(LG_kv* kv, LG_txn* txn, LG_id domain_id, const int flags)
{
	// TODO: flags |= LG_KV_FIFO
	lg_kv_key_reset(kv);
	lg_kv_init(kv, txn, domain_id, flags);
}

int
lg_fifo_push(LG_kv* kv, LG_buf* val)
{
	return lg_fifo_push_n(kv, val, 1);
}

int // => count pushed / new length?
lg_fifo_push_n(LG_kv* kv, LG_buf* vals, const int count)
{
	// lg_kv_key_reset(kv);
	struct cursor_t cursor;
	// int len = 0;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_last_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	if (DB_NOTFOUND == r) {
		r = DB_SUCCESS;
		kv->key.size = kv->klen + ctr_init(kv->kbuf + kv->klen);
	} else if(DB_SUCCESS == r) {
		memcpy(kv->kbuf, kv->key.data, kv->key.size);
		kv->key.size = kv->klen + ctr_inc(kv->kbuf + kv->klen);
	}
	int i = 0;
	kv->key.data = kv->kbuf;
	while (DB_SUCCESS == r && i < count) {
		if (i)
			kv->key.size = kv->klen + ctr_inc(kv->kbuf + kv->klen);
		r = cursor_put(&cursor, &kv->key, &vals[i], 0);
		++i;
	}
	cursor_close(&cursor);
	return i;
}

int
lg_fifo_peek(LG_kv* kv, LG_buf* val)
{
	return lg_fifo_peek_n(kv, val, 1);
}

int
lg_fifo_peek_n(LG_kv* kv, LG_buf* vals, const int count)
{
	// lg_kv_key_reset(kv);
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	// const int resolve = kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA);
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	int i;
	for (i = 0; DB_SUCCESS == r && i < count; i++) {
		r = cursor_get(&cursor, &kv->key, &vals[i], DB_SET_KEY);
		assert(DB_SUCCESS == r);
		r = cursor_get(&cursor, &kv->key, NULL, DB_NEXT);
		if (DB_SUCCESS != r
			|| kv->key.size < kv->klen
			|| memcmp(kv->key.data, kv->kbuf, kv->klen) )
		{
			r = DB_NOTFOUND;
		}
	}
	if UNLIKELY(!( DB_SUCCESS == r || DB_NOTFOUND == r ))
		errno = r;
	cursor_close(&cursor);
	return i;
}

// int
// lg_fifo_pop(LG_kv* kv, LG_buf* val)
// {
// 	return lg_fifo_pop_n(kv, val, 1);
// }

// int
// lg_fifo_pop_n(LG_kv* kv, LG_buf* vals, const int count)
// {
// 	int n = lg_fifo_peek_n(kv, vals, count);
// 	return kv_fifo_delete(kv, n);
// }


int
lg_fifo_push_uint(LG_kv* f, LG_uint x)
{
	return lg_fifo_push_uint_n(f, &x, 1);
}

int
lg_fifo_push_uint_n(LG_kv* kv, LG_uint* x, const int count)
{
	// TODO: maybe chunk up large values of count !stackoverflow, for embedded?
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_last_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	if (DB_NOTFOUND == r) {
		r = DB_SUCCESS;
		kv->key.size = kv->klen + ctr_init(kv->kbuf + kv->klen);
	} else if(DB_SUCCESS == r) {
		memcpy(kv->kbuf, kv->key.data, kv->key.size);
		kv->key.size = kv->klen + ctr_inc(kv->kbuf + kv->klen);
	}
	int i = 0;
	kv->key.data = kv->kbuf;
	while (DB_SUCCESS == r && i < count) {
		LG_buf buf = LG_UINT_BUF;
        LG_ser val;
		lg_ser_init(&val, &buf);
		lg_ser_uint(&val, x[i]);
		if (i)
			kv->key.size = kv->klen + ctr_inc(kv->kbuf + kv->klen);
		r = cursor_put(&cursor, &kv->key, &val, 0);
		++i;
	}
	cursor_close(&cursor);
	return i;
}

int
lg_fifo_peek_uint(LG_kv* kv, LG_uint* x)
{
    return lg_fifo_peek_uint_n(kv, x, 1);
}

int
lg_fifo_peek_uint_n(LG_kv* kv, LG_uint* vals, const int count)
{
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	int i;
	for (i = 0; DB_SUCCESS == r && i < count; i++) {
		LG_buf buf;
		LG_des des;
		r = cursor_get(&cursor, &kv->key, &buf, DB_SET_KEY);
		assert(DB_SUCCESS == r);
		lg_des_init(&des, &buf);
		vals[i] = lg_des_uint(&des);
		r = cursor_get(&cursor, &kv->key, NULL, DB_NEXT);
		if (DB_SUCCESS != r
			|| kv->key.size < kv->klen
			|| memcmp(kv->key.data, kv->kbuf, kv->klen) )
		{
			r = DB_NOTFOUND;
		}
	}
	if UNLIKELY(!( DB_SUCCESS == r || DB_NOTFOUND == r ))
		errno = r;
	cursor_close(&cursor);
	return i;
}

int
lg_fifo_pop_uint(LG_kv* kv, LG_uint* x)
{
    return lg_fifo_pop_uint_n(kv, x, 1);
}

int
lg_fifo_pop_uint_n(LG_kv* kv, LG_uint* vals, const int count)
{
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	int i = 0;
	while (i < count) {
		LG_buf buf;
		r = cursor_first(&cursor, &kv->key, &buf, kv->kbuf, kv->klen);
		// r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
		if (DB_SUCCESS == r) {
			LG_des des;
			// r = cursor_get(&cursor, &kv->key, &buf, DB_SET_KEY); // assert(DB_SUCCESS == r);
			lg_des_init(&des, &buf);
			vals[i] = lg_des_uint(&des);
			r = cursor_del(&cursor, 0);
			assert(DB_SUCCESS == r);
			++i;
		}
		else {
			if UNLIKELY(DB_NOTFOUND != r)
				errno = r;
			break;
		}
	}
	cursor_close(&cursor);
	return i;
}

int
lg_fifo_del_n(LG_kv* kv, const int count)
{
	struct cursor_t cursor;
	int i = 0;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	while (DB_SUCCESS == r) {
		r = cursor_del(&cursor, 0);
		assert(DB_SUCCESS == r);
		if (++i == count)
			break;
		r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	}
	if UNLIKELY(!( DB_SUCCESS == r || DB_NOTFOUND == r ))
		errno = r;
	cursor_close(&cursor);
	return i;
}

int
lg_fifo_len(LG_kv* kv, LG_uint* len)
{
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	r = cursor_first_key(&cursor, &kv->key, kv->kbuf, kv->klen);
	if (DB_SUCCESS == r) {
		LG_buf key2;
		int r2 = cursor_last_key(&cursor, &key2, kv->kbuf, kv->klen);
		assert(DB_SUCCESS == r2);
		*len = 1 + ctr_delta(key2.data + kv->klen, kv->key.data + kv->klen);
	} else {
		*len = 0;
		if (DB_NOTFOUND == r)
			r = DB_SUCCESS;
		// else errno = r;
	}
	cursor_close(&cursor);
	return r;
}





void
lg_pq_init(LG_kv* kv, LG_txn* txn, LG_id domain_id, const int flags)
{
	// TODO: flags |= LG_KV_PQ
	lg_kv_key_reset(kv);
	lg_kv_init(kv, txn, domain_id, flags);
}

// on success return 0, on error return < 0
int
lg_pq_add(LG_kv* kv, LG_buf* val, uint8_t priority)
{
	priority = 255 - priority; // # invert priority, internally high=0 low=255
	int r;
	int pc_len;
	struct cursor_t cursor;
	uint8_t pri_counter[257];
	r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	if UNLIKELY(DB_SUCCESS != r)
		return r;
	// optionally swap out key w/ encoded string ID
	r = DB_NOTFOUND;
	//
	// if(kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)){
	//  strID_t id;
	//  uint8_t ekey[esizeof(id)];
	// 	if(!ggblob_resolve(kv->txn, &id, key, klen, 0))
	// 		goto done;
	// 	klen = 0;
	// 	key = ekey;
	// 	encode(id, ekey, klen);
	// }
	//
	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking secondary index
	kv->kbuf[kv->key.size++] = 1;
	// append key
	assert(kv->key.size + val->size <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->key.size, val->data, val->size);
	kv->key.size += val->size;
	// see if it's already somewhere in the queue
	r = db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val);
	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking primary index
	kv->kbuf[kv->key.size++] = 0;
	// if found, use returned priority, counter to delete from primary index
	if (DB_SUCCESS == r) {
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
	if (DB_SUCCESS == r) {
		memcpy(kv->kbuf + tail, kv->key.data + tail, kv->key.size - tail);
		// increment counter, grab length of pri & counter bytes
		pc_len = 1 + ctr_inc(kv->kbuf + tail);
	} else {
		pc_len = 1 + ctr_init(kv->kbuf + tail);
	}
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen + 1 + pc_len;
	// snag copy
	memcpy(pri_counter, kv->kbuf + kv->klen + 1, pc_len);
	// add in new record in primary index
	kv->val.data = val->data;
	kv->val.size = val->size;
	r = db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->val, 0);
	assert(DB_SUCCESS == r);
	// add reverse record in secondary index
	kv->kbuf[kv->klen] = 1;
	memcpy(kv->kbuf + kv->klen + 1, val->data, val->size);
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen + 1 + val->size;
	kv->val.data = pri_counter;
	kv->val.size = pc_len;
	r = db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->val, 0);
	assert(DB_SUCCESS == r);
	cursor_close(&cursor);
	return r;
}

// fetch priority[0..255] for key, on error return < 0
int
lg_pq_get(LG_kv* kv, LG_buf* val, uint8_t *priority)
{
	int r = DB_NOTFOUND;
	// ZZZ: optionally swap out key w/ encoded string ID
	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking secondary index
	kv->kbuf[kv->key.size++] = 1;
	// append key
	assert(kv->key.size + val->size <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->key.size, val->data, val->size);
	kv->key.size += val->size;
	// see if it's already somewhere in the queue
	r = db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val);
	if (DB_SUCCESS == r)
		*priority = 255 - *(uint8_t *)kv->val.data;
	return r;
}

// on success return 0, on error return < 0
int
lg_pq_del(LG_kv* kv, LG_buf* val)
{
	int r;
	// optionally swap out key w/ encoded string ID
	r = DB_NOTFOUND;
	//
	// if (kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)) {
	// 	strID_t id;
	// 	uint8_t ekey[esizeof(id)];
	// 	if(!ggblob_resolve(kv->txn, &id, key, klen, 0))
	// 		goto done;
	// 	klen = 0;
	// 	key = ekey;
	// 	encode(id, ekey, klen);
	// }
	//
	// start with encoded domID
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	// we are checking secondary index
	kv->kbuf[kv->key.size++] = 1;
	// append key
	assert(kv->key.size + val->size <= sizeof(kv->kbuf));
	memcpy(kv->kbuf + kv->key.size, val->data, val->size);
	kv->key.size += val->size;
	// see if it's already somewhere in the queue
	r = db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->val);
	// if found, use returned priority, counter to delete from primary index
	if (DB_SUCCESS == r) {
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
		memcpy(kv->kbuf + kv->klen + 1, val->data, val->size);
		kv->key.data = kv->kbuf;
		kv->key.size = kv->klen + 1 + val->size;
		// delete from secondary index
		r = db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL);
		assert(DB_SUCCESS == r);
	}
	return r;
}

int
lg_pq_iter(LG_kv* kv, LG_pq_iter* iter)
{
	uint8_t pfx_data = 0;
	LG_buf pfx = {.size = 1, .data = &pfx_data};
	return lg_kv_iter_pfx(kv, iter, &pfx);
}

bool
lg_pq_iter_next(LG_kv_iter* iter)
{
	// NOTE: no need to offset/use iter.key
	return (DB_SUCCESS == iter_next((iter_t)iter));
}


void
lg_pq_cursor_init(LG_kv* kv, LG_pq_cursor* pq_cursor, uint8_t priority)
// cursor holds [decode flag][pfx][magic][priority][counter]
// where:
//	pfx is (encoded domain ID, 0)
//	priority is 1 byte
{
	// uint8_t *cursor = smalloc(LG_PQ_CURSOR_SIZE);
	pq_cursor->size = LG_PQ_CURSOR_SIZE;
	uint8_t* cursor = pq_cursor->u8;
	int len = kv->klen + 1;
	// TODO: ? append decode flags
	// cursor[0] = kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA);
	// append encoded domain
	memcpy(cursor + 1, kv->kbuf, kv->klen);
	// append magic byte to select primary index
	cursor[len++] = 0;
	// append requested priority
	cursor[len++] = 255 - priority;
	// and initialize counter
	ctr_init(cursor + len);
}

bool
lg_pq_cursor_next(LG_txn* txn, LG_pq_cursor* pq_cursor, LG_buf* val, uint8_t* priority)
{
	// on success, advance cursor
	uint8_t* cursor = pq_cursor->u8;
	int r;
	bool success;
	LG_buf k;
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
	if (DB_SUCCESS != r)
		goto done;
	if (k.size < pfxlen || memcmp(k.data, cursor + 1, pfxlen)) {
		r = DB_NOTFOUND;
		goto done;
	}
	r = cursor_get(&c, &k, val, DB_GET_CURRENT);
	if (DB_SUCCESS != r)
		goto done;
	// copy what we found
	memcpy(cursor + 1, k.data, k.size);
	// increment its counter
	ctr_inc(cursor + ctroff);
	// harvest priority
	*priority = 255 - cursor[ctroff-1];
	// TODO: ? possibly lookup result
	// if (cursor[0])
	// 	val->data = graph_string_enc(txn, v.data, (&val->size));
done:
	cursor_close(&c);
	success = DB_SUCCESS == r;
	if UNLIKELY(!success && DB_NOTFOUND != r)
		errno = r;
	return success;
}

void
lg_pq_cursor_close(LG_pq_cursor* pq_cursor)
{
	// free(cursor);
}





int kv_del(kv_t kv, void *key, size_t klen){
	int ret = 0;
	if(_ggkv_setup_key(kv, key, klen, 1))
		ret = (db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL) == DB_SUCCESS);
	return ret;
}

int kv_put(kv_t kv, void *key, size_t klen, void *data, size_t dlen){
	int ret = 0;
	uint8_t dbuf[esizeof(strID_t)];
	if(!_ggkv_setup_key(kv, key, klen, 0))
		goto done;
	if(kv->flags & LG_KV_MAP_DATA){
		strID_t id;
		if(!ggblob_resolve(kv->txn, &id, data, dlen, 0))
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



static INLINE void *_kv_key(kv_t kv, LG_buf *key, size_t  *len, const int unmap){
	if(unmap)
		return graph_string_enc(kv->txn, key->data + kv->klen, len);
	*len = key->size - kv->klen;
	return key->data + kv->klen;
}

void *kv_first_key(kv_t kv, size_t *klen){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	LG_buf key;
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
	LG_buf key;
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
	LG_buf k;
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
			if(!ggblob_resolve(kv->txn, &id, datas[i], lens[i], 0)){
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
	LG_buf key2;
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

// fetch priority[0..255] for key, on error return < 0
int kv_pq_get(kv_t kv, void *key, size_t klen){
	int r;
	strID_t id;
	uint8_t ekey[esizeof(id)];

	// optionally swap out key w/ encoded string ID
	r = DB_NOTFOUND;
	if(kv->flags & (LG_KV_MAP_KEYS|LG_KV_MAP_DATA)){
		if(!ggblob_resolve(kv->txn, &id, key, klen, 0))
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
		if(!ggblob_resolve(kv->txn, &id, key, klen, 0))
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
		if(!ggblob_resolve(kv->txn, &id, key, klen, 0))
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
int kv_pq_cursor_next(LG_txn* txn, uint8_t *cursor, void **key, size_t *klen){
	int r;
	LG_buf k, v;
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
	iter = smalloc(sizeof(*iter));
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
	LG_buf bmk = { .size = kv->klen, .data = kv->kbuf };
	int r = db_del((txn_t)kv->txn, DB_KVBM, &bmk, NULL);
	return (DB_SUCCESS == r || DB_NOTFOUND == r);
}

int kv_next(kv_t kv, void **key, size_t *klen, void **data, size_t *dlen){
	int r, ret = 0;
	LG_buf bmk = { .size = kv->klen, .data = kv->kbuf };
	LG_buf pos, val;
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
	int ret = _ggkv_setup_key(iter->kv, key, klen, 1);
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


#endif
#endif