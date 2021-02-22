#ifndef LG_SERDES_H
#define LG_SERDES_H

#define LG_buf buffer_t

#define LG_uint	uint64_t
#define LG_UINT_SIZE 9
#define LG_UINT_DATA (uint8_t[9]){}
#define LG_UINT_BUF (LG_buf){.size=LG_UINT_SIZE, .data=LG_UINT_DATA}

#define LG_UINT_N_SIZE(n) (9*(n))
#define LG_UINT_N_DATA(n) (uint8_t[9*(n)]){}
#define LG_UINT_N_BUF(n) (LG_buf){.size=LG_UINT_N_SIZE(n), .data=LG_UINT_DATA}


// provide type-agnostic clz wrapper, and return a more useful value for clz(0)
#define __clz_wrapper(x) \
    (unsigned int)(( x ) ? \
        (sizeof( x ) == sizeof( long ) ? \
            (unsigned)__builtin_clzl( x ) \
            : (sizeof( x ) == sizeof(long long) ? \
                (unsigned)__builtin_clzll( x ) \
                : (unsigned)__builtin_clz( (int)(x) ) \
            ) \
        ) : sizeof( x ) * 8)

// quickly take unsigned numeric types and count minimum number of bytes needed to represent - for varint encoding
#define _varint_intbytes(x) \
    (sizeof(x) - __clz_wrapper(x) / 8)

// encode unsigned values into buffer, advancing iter
// ensure you have a least 9 bytes per call
#define encode(x, buffer, iter)  _encode((x), (buffer), (iter))
#define _encode(x, buffer, iter) \
do { \
    int _shift; \
    ((uint8_t *)(buffer))[iter] = _varint_intbytes(x); \
    for ( \
		_shift = (((uint8_t *)(buffer))[iter++] - 1) * 8; \
		_shift >= 0; \
		iter++, _shift -= 8 \
	) \
        ((uint8_t *)(buffer))[iter] = ((x) >> _shift) & 0xff; \
} while (0)

// corresponding decode
#define decode(x, buffer, iter)  _decode((x), (buffer), (iter))
#define _decode(x, buffer, iter) \
do { \
    uint8_t _count = ((uint8_t *)(buffer))[iter++]; \
    assert((uint8_t)sizeof(x) >= (uint8_t)_count); \
    x = 0; \
    while(_count--) \
        x = (x<<8) + ((uint8_t *)(buffer))[iter++]; \
} while (0)

#define enclen(buffer, offset) \
    (1 + ((uint8_t *)(buffer))[offset])

#define esizeof(x) \
    (sizeof(x)+1)

int pack_uint(LG_uint i, char *buffer);
LG_uint unpack_uint(char *buffer);
int pack_uints(int count, LG_uint *ints, void *buffer);
int unpack_uints(int count, LG_uint *ints, void *buffer);
int unpack_uints2(int count, LG_uint *ints, void *buffer, size_t buflen);


#define lg_buf_eq(b1, b2) 		_lg_buf_eq((b1), (b2))
#define lg_buf_eq_safe(b1, b2) 	(_lg_buf_is_safe((b1)) && _lg_buf_is_safe((b2)) && _lg_buf_eq((b1), (b2)))
#define lg_buf_is_safe(b) 		_lg_buf_is_safe((b))
#define lg_buf_is_unsafe(b) 	(!_lg_buf_is_safe((b)))
#define _lg_buf_eq(b1, b2) 		(b1->size == b2->size && memcmp(b1->data,b2->data,b1->size) == 0)
#define _lg_buf_is_safe(b) 		(b && b->data)
// bool 	lg_buf_eq(const buffer_t* b1, const buffer_t* b2, size_t n);
// int 	lg_buf_cmp(const buffer_t* b1, const buffer_t* b2, size_t n);
// int 	lg_buf_cmp_safe(const buffer_t* b1, const buffer_t* b2, size_t n);

typedef struct
LG_serdes {
	union {
		LG_buf as_buf;
		struct {
			union {
				size_t len;
				size_t idx;
			};
			union {
				void* data; // address of the data item
				uint8_t* u8;
				char* 	 ch;
			};
		};
	};
	// size_t cap;
} LG_serdes;

#define LG_ser LG_serdes
#define LG_des LG_serdes

// size_t 	lg_ser_mem(buffer_t* buf, void* src, size_t n);
// size_t 	lg_ser_uint(buffer_t* buf, LG_uint x);
// #define lg_ser_str(buf, str) (lg_ser_mem(buf, str, strlen(str)))

int 	lg_serdes_init(LG_ser* ser, LG_buf* buf);
#define lg_ser_init(ser, buf)	lg_serdes_init(ser, buf)
#define lg_des_init_n	lg_serdes_init_n
#define lg_ser_init_n	lg_serdes_init_n
#define lg_des_init(des, buf)	lg_serdes_init(des, buf)
LG_uint lg_des_uint(LG_des* des);

#define lg_ser_reset(ser) \
	(ser)->len = 0;

#define lg_ser_uint(ser, x) \
	encode(x, (ser)->data, (ser)->len);



// ============================================================================
#ifdef LG_IMPLEMENTATION


int
lg_serdes_init(LG_ser* ser, LG_buf* buf)
{
	// TODO: if UNLIKELY(lg_buf_is_unsafe(buf))
	// 	return 1;
	ser->len = 0;
	// ser->cap = buf->size;
	ser->data = buf->data;
	return 0;
}

int
lg_serdes_init_n(LG_ser* ser, LG_buf* buf, const int n)
{
	int i = 0;
	while (i < n) {
		// TODO: if UNLIKELY(lg_buf_is_unsafe(buf))
		// 	return 1;
		ser[i].len = 0;
		// ser->cap = buf->size;
		ser[i].data = buf[i].data;
		++i;
	}
	return 0;
}

LG_uint lg_des_uint(LG_des* des)
{
	LG_uint x;
	decode(x, (des)->data, (des)->len);
	return x;
}

// int
// lg_buf_cmp(const buffer_t* b1, const buffer_t* b2, size_t n)
// /*
// 	@return ((b1->size - b2->size)<<8) | memcmp()
// 		* memcmp returns lowest 8 bits : 0 if identical, otherwise the difference between the first two differing bytes
// */
// {
// 	size_t delta = b1->size - b2->size;
// 	return (delta << 8) | memcmp(b1->data, b2->data, b2->size + size);
// }
// int
// lg_buf_cmp_safe(const buffer_t* b1, const buffer_t* b2, size_t n)
// /*
// 	@return
// 		* memcmp returns lowest 8 bits : 0 if identical, otherwise the difference between the first two differing bytes
// */
// {
// 	int unsafe = lg_buf_is_unsafe(b1) | (lg_buf_is_unsafe(b2) << 1);
// 	return (unsafe ? unsafe : (lg_buf_cmp(b1, b2, n) << 2))
// 	return (delta << 8) | memcmp(b1->data, b2->data, b2->size + size);
// }

// size_t
// lg_ser_mem(buffer_t* buf, void* src, size_t n)
// {
// 	int len = 0;
// 	uint8_t * data = (uint8_t *)(buf->data) + buf->size;
// 	encode(n, data, len);
// 	memcpy(data + len, src, n);
// 	len += n;
// 	buf->size += len;
// 	return len;
// }


int
pack_uint(uint64_t i, char *buffer)
{
	int len = 0;
	encode(i, buffer, len);
	return len;
}

uint64_t
unpack_uint(char *buffer)
{
	int len = 0;
	uint64_t i;
	decode(i, buffer, len);
	return i;
}

int
pack_uints(int count, uint64_t *ints, void *buffer)
{
	int i, len = 0;
	for(i = 0; i < count; i++)
		encode(ints[i], buffer, len);
	return len;
}

// unpacks 'count' uints
// returns number of input bytes consumed
int
unpack_uints(int count, uint64_t *ints, void *buffer)
{
	int i, len = 0;
	for(i = 0; i < count; i++)
		decode(ints[i], buffer, len);
	return len;
}

// unpacks up to 'count' uints
// returns how many it could have unpacked if buffer was big enough
// (if return is larger than 'count', then call again w/ a larger buffer)
int
unpack_uints2(int count, uint64_t *ints, void *buffer, size_t buflen)
{
	size_t len;
	int i;
	for(i = 0, len = 0; len < buflen; i++)
		if(i < count)
			decode(ints[i], buffer, len);
		else
			len += enclen(buffer, len);
	return len == buflen ? i : -1;
}



#endif
#endif