#ifndef LG_SERDES_H
#define LG_SERDES_H


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
#define encode(x, buffer, iter) \
do { \
    int _shift; \
    ((uint8_t *)(buffer))[iter] = _varint_intbytes(x); \
    for (_shift = (((uint8_t *)(buffer))[iter++] - 1) * 8; _shift >= 0; iter++, _shift -= 8) \
        ((uint8_t *)(buffer))[iter] = ((x) >> _shift) & 0xff; \
} while (0)

// corresponding decode
#define decode(x, buffer, iter) \
do { \
    uint8_t _count = ((uint8_t *)(buffer))[iter++]; \
    assert(sizeof(x) >= _count); \
    x = 0; \
    while(_count--) \
        x = (x<<8) + ((uint8_t *)(buffer))[iter++]; \
} while (0)

#define enclen(buffer, offset) \
    (1 + ((uint8_t *)(buffer))[offset])

#define esizeof(x) \
    (sizeof(x)+1)

int pack_uints(int count, uint64_t *ints, void *buffer)
{
	int i, len = 0;
	for(i = 0; i < count; i++)
		encode(ints[i], buffer, len);
	return len;
}

// unpacks 'count' uints
// returns number of input bytes consumed
int unpack_uints(int count, uint64_t *ints, void *buffer)
{
	int i, len = 0;
	for(i = 0; i < count; i++)
		decode(ints[i], buffer, len);
	return len;
}

// unpacks up to 'count' uints
// returns how many it could have unpacked if buffer was big enough
// (if return is larger than 'count', then call again w/ a larger buffer)
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


#endif