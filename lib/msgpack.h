#ifndef LG_MSGPACK_H
#define LG_MSGPACK_H


#include "cwpack/src/cwpack.h"
#include "cwpack/src/utils/cwpack_utils.h"
// #include "cwpack/goodies/basic-contexts/basic_contexts.h"
// #include "cwpack/goodies/numeric-extensions/numeric_extensions.h"

/*******************************   (U N) P A C K Return Codes   *****************************/

// #define LG_PACK_OK                         0  WP_RC_OK
// #define LG_PACK_END_OF_INPUT              -1  WP_RC_END_OF_INPUT
// #define LG_PACK_BUFFER_OVERFLOW           -2  WP_RC_BUFFER_OVERFLOW
// #define LG_PACK_BUFFER_UNDERFLOW          -3  WP_RC_BUFFER_UNDERFLOW
// #define LG_PACK_MALFORMED_INPUT           -4  WP_RC_MALFORMED_INPUT
// #define LG_PACK_WRONG_BYTE_ORDER          -5  WP_RC_WRONG_BYTE_ORDER
// #define LG_PACK_ERROR_IN_HANDLER          -6  WP_RC_ERROR_IN_HANDLER
// #define LG_PACK_ILLEGAL_CALL              -7  WP_RC_ILLEGAL_CALL
// #define LG_PACK_MALLOC_ERROR              -8  WP_RC_MALLOC_ERROR
// #define LG_PACK_STOPPED                   -9  WP_RC_STOPPED
// #define LG_PACK_TYPE_ERROR               -10  WP_RC_TYPE_ERROR
// #define LG_PACK_VALUE_ERROR              -11  WP_RC_VALUE_ERROR
// #define LG_PACK_WRONG_TIMESTAMP_LENGTH   -12  WP_RC_WRONG_TIMESTAMP_LENGTH

/*******************************   P A C K   **********************************/

// typedef struct LG_pack {
// 	union {
// 		cw_pack_context ctx;
// 		struct {
// 			uint8_t*                current;
// 			uint8_t*                start;
// 			uint8_t*                end;
// 			bool                    be_compatible;
// 			int                     return_code;
// 			int                     err_no;          /* handlers can save error here */
// 			pack_overflow_handler   handle_pack_overflow;
// 			pack_flush_handler      handle_flush;
// 		};
// 	};
// } LG_pack;
typedef cw_pack_context 				LG_pack;
typedef pack_overflow_handler			LG_pack_handler;	// int (*pack_overflow_handler)(struct cw_pack_context*, unsigned long);
typedef pack_flush_handler				LG_pack_flush_cb;	// int (*pack_flush_handler)(struct cw_pack_context*);

int 	lg_pack_init(LG_pack* p, LG_buf* buf);
#define lg_pack_set_flush_cb(p, fn)	cw_pack_set_flush_handler(p, fn)
#define lg_pack_set_compat(p, b)	cw_pack_set_compatibility(p, b)
#define lg_pack_get_size(p)	        ((p)->current - (p)->start)
#define lg_pack_get_buf(p, buf)	    (buf)->size=lg_pack_get_size((p)); (buf)->data=(void*)((p)->start)
#define lg_pack_flush(p) 			cw_pack_flush(p)
#define lg_pack_nil(p) 				cw_pack_nil(p)
#define lg_pack_true(p) 			cw_pack_true(p)
#define lg_pack_false(p) 			cw_pack_false(p)
#define lg_pack_bool(p, b) 			cw_pack_boolean(p, b)
#define lg_pack_int(p, i) 			cw_pack_signed(p, i)
#define lg_pack_uint(p, u) 			cw_pack_unsigned(p, u)
#define lg_pack_f32(p, f) 			cw_pack_float(p, f)
#define lg_pack_f64(p, d) 			cw_pack_double(p, d)
#define lg_pack_f32_opt(p, f) 		cw_pack_float_opt(p, f)   /* Pack as signed if precision isn't destroyed */
#define lg_pack_f64_opt(p, d) 		cw_pack_double_opt(p, d)  /* Pack as signed or float if precision isn't destroyed */
#define lg_pack_arr_size(p, n)		cw_pack_array_size(p,( uint32_t)n)
#define lg_pack_map_size(p, n)		cw_pack_map_size(p, (uint32_t)n)
#define lg_pack_str(p, s)			cw_pack_str(p, (const char*)s, (uint32_t)strlen(s))
#define lg_pack_bin(p, v, l)		cw_pack_bin(p, (const void*)v, (uint32_t) l)
#define lg_pack_ext(p, type, v, l)	cw_pack_ext(p, (int8_t)type, (const void*)v, (uint32_t)l)
#define lg_pack_insert(p, v, l) 	cw_pack_insert(p, (const void*)v, (uint32_t)l)
// #define lg_pack_time(p, )			cw_pack_time(p, (int64_t) sec, (uint32_t) nsec)
// void cw_pack_time_interval (cw_pack_context* pack_context, double ti); /* ti is seconds relative epoch */



/*****************************   U N P A C K   ********************************/

// typedef struct cw_unpack_context {
//     cwpack_item                 item;
//     uint8_t*                    start;
//     uint8_t*                    current;
//     uint8_t*                    end;             /* logical end of buffer */
//     int                         return_code;
//     int                         err_no;          /* handlers can save error here */
//     unpack_underflow_handler    handle_unpack_underflow;
// } cw_unpack_context;
typedef cw_unpack_context 				LG_unpack;	// int (*unpack_underflow_handler)(struct cw_unpack_context*, unsigned long);
typedef unpack_underflow_handler		LG_unpack_handler;
typedef	cwpack_item_types 				LG_pack_type;
	#define LG_PACK_MIN_RESERVED_EXT  	/* -128 */ CWP_ITM_MIN_RESERVED_EXT
	#define LG_PACK_TIMESTAMP         	/*   -1 */ CWP_ITM_TIMESTAMP
	#define LG_PACK_MAX_RESERVED_EXT  	/*   -1 */ CWP_ITM_MAX_RESERVED_EXT
	#define LG_PACK_MIN_USER_EXT      	/*    0 */ CWP_ITM_MIN_USER_EXT
	#define LG_PACK_MAX_USER_EXT      	/*  127 */ CWP_ITM_MAX_USER_EXT
	#define LG_PACK_NIL               	/*  300 */ CWP_ITM_NIL
	#define LG_PACK_BOOL           		/*  301 */ CWP_ITM_BOOLEAN
	#define LG_PACK_POSITIVE_INT  		/*  302 */ CWP_ITM_POSITIVE_INTEGER
	#define LG_PACK_NEGATIVE_INT  		/*  303 */ CWP_ITM_NEGATIVE_INTEGER
	#define LG_PACK_F32             	/*  304 */ CWP_ITM_FLOAT
	#define LG_PACK_F64            		/*  305 */ CWP_ITM_DOUBLE
	#define LG_PACK_STR               	/*  306 */ CWP_ITM_STR
	#define LG_PACK_BIN               	/*  307 */ CWP_ITM_BIN
	#define LG_PACK_ARR             	/*  308 */ CWP_ITM_ARRAY
	#define LG_PACK_MAP               	/*  309 */ CWP_ITM_MAP
	#define LG_PACK_EXT               	/*  310 */ CWP_ITM_EXT
	#define LG_PACK_INVALID             /*  999 */ CWP_NOT_AN_ITEM

// typedef struct {
//     const void*     start;
//     uint32_t        length;
// } cwpack_blob;

// typedef struct {
//     uint32_t    size;
// } cwpack_container;

// typedef struct {
//     int64_t     tv_sec;
//     uint32_t    tv_nsec;
// } cwpack_timespec;

// typedef struct {
//     cwpack_item_types   type;
//     union
//     {
//         bool            boolean;
//         uint64_t        u64;
//         int64_t         i64;
//         float           real;
//         double          long_real;
//         cwpack_container array;
//         cwpack_container map;
//         cwpack_blob     str;
//         cwpack_blob     bin;
//         cwpack_blob     ext;
//         cwpack_timespec time;
//     } as;
// } cwpack_item;

int 	lg_unpack_init(LG_unpack* u, LG_buf* buf);
void	lg_unpack_set_handler(LG_unpack* u, LG_unpack_handler h);
#define lg_unpack_next(u)		cw_unpack_next(u)
	// void cw_unpack_next (cw_unpack_context* unpack_context);

#define lg_unpack_bool(u) 		cw_unpack_next_boolean(u)
#define lg_unpack_i64(u)		cw_unpack_next_signed64(u)
#define lg_unpack_i32(u)		cw_unpack_next_signed32(u)
#define lg_unpack_i16(u)		cw_unpack_next_signed16(u)
#define lg_unpack_i8(u)			cw_unpack_next_signed8(u)
#define lg_unpack_u64(u)		cw_unpack_next_unsigned64(u)
#define lg_unpack_u32(u)		cw_unpack_next_unsigned32(u)
#define lg_unpack_u16(u)		cw_unpack_next_unsigned16(u)
#define lg_unpack_u8(u)			cw_unpack_next_unsigned8(u)
#define lg_unpack_f64(u)		cw_unpack_next_double(u)
#define lg_unpack_f32(u)		cw_unpack_next_float(u)
// #define lg_unpack_time(u)		cw_unpack_next_time_interval(u)
	// double cw_unpack_next_time_interval (cw_unpack_context* unpack_context);

size_t  lg_unpack_str(LG_unpack* u, char** str);
#define lg_unpack_str_len(u)	cw_unpack_next_str_lengh(u)
#define lg_unpack_arr_size(u)	cw_unpack_next_array_size(u)
#define lg_unpack_map_size(u) 	cw_unpack_next_map_size(u)
#define lg_unpack_skip(u, n) 	cw_skip_items(u, n)
#define lg_unpack_at_end(u) 	((CWP_RC_END_OF_INPUT == (u)->return_code) || (u)->current + 1 > (u)->end)


//============================================================================
#ifdef LG_IMPLEMENTATION
//============================================================================


#include "cwpack/src/cwpack.c"
#include "cwpack/src/utils/cwpack_utils.c"
// #include "cwpack/goodies/basic-contexts/basic_contexts.c"
// #include "cwpack/goodies/numeric-extensions/numeric_extensions.c"

int
lg_pack_init(LG_pack* p, LG_buf* buf)
{
	return cw_pack_context_init((cw_pack_context*)p, buf->data, (unsigned long)buf->size, 0);
}

int
lg_unpack_init(LG_unpack* u, LG_buf* buf)
{
	return cw_unpack_context_init(u, buf->data, buf->size, 0);
}

void
lg_unpack_set_handler(LG_unpack* u, LG_unpack_handler h)
{
	u->handle_unpack_underflow = (unpack_underflow_handler)h;
}

size_t
lg_unpack_str(LG_unpack* u, char** str)
{
    cw_unpack_next (u);
    if (u->return_code)
        return 0;
    if (u->item.type == CWP_ITEM_STR) {
        *str = u->item.as.str.start;
        return u->item.as.str.length;
    }
    u->return_code = CWP_RC_TYPE_ERROR;
    return 0;
}

#endif


#endif