
// kv storage api - domains get mapped to stringIDs via the string storage layer
// so do keys and values if LG_KV_MAP_KEYS or LG_KV_MAP_DATA are set
// non-mapped keys must be fairly short (less than 500 bytes is safe)
// flags are not stored internally - client must know per domain
// note - related kv & kv_iter objects share buffers - do not use concurrently from multiple threads

#include "counter.h"

// kv flags
#define LG_KV_RO       0x1
#define LG_KV_MAP_KEYS 0x2
#define LG_KV_MAP_DATA 0x4

typedef struct
ggkv_t {
	ggtxn_t* txn;
	buffer_t key, val;
	int flags;
	unsigned int refs, klen;
	uint8_t kbuf[511];
} ggkv_t;

typedef struct
ggkv_iter_t {
	struct iter_t iter;
	ggkv_t* kv;
} ggkv_iter_t;

void    ggkv_init(ggkv_t* kv, ggtxn_t* txn, LG_id domain_id, const int flags);
int     ggkv_get(ggkv_t* kv, pod_t* key, pod_t* val);

// (compat)
typedef ggkv_t* kv_t;
typedef ggkv_iter_t* kv_iter_t;
// kv
kv_t graph_kv(ggtxn_t* txn, const void *domain, const size_t dlen, const int flags);
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
int kv_pq_cursor_next(ggtxn_t* txn, uint8_t *cursor, void **key, size_t *klen);
void kv_pq_cursor_close(uint8_t *cursor);

void ggkv_init(ggkv_t* kv, ggtxn_t* txn, LG_id domain_id, const int flags)
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

kv_t graph_kv(ggtxn_t* txn, const void *domain, const size_t dlen, const int flags)
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
	ggkv_t* kv = NULL;
	pod_t d = pod_buf(domain, dlen);
	if ( !pod_resolve(txn, &d, (flags & LG_KV_RO)) )
        goto FAIL;
	kv = smalloc(sizeof(*kv));
	if (!kv)
		goto FAIL;
	ggkv_init(kv, txn, d.id, flags);
	return kv;
FAIL:
	if(kv)
		free(kv);
	return NULL;
}

static INLINE int _ggkv_setup_key(ggkv_t* kv, void *key, size_t klen, int readonly)
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


int ggkv_get(ggkv_t* kv, pod_t* key, pod_t* val)
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


// int ggkv_del(ggkv_t* kv, pod_t key)
// {

// }

// int ggkv_put(ggkv_t* kv, pod_t key, pod_t* val)
// {

// }




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
int kv_pq_cursor_next(ggtxn_t* txn, uint8_t *cursor, void **key, size_t *klen){
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
