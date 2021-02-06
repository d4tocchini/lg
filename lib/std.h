#include <string.h>
#include <inttypes.h>
#include <sys/types.h>

#include "mimalloc/include/mimalloc-override.h"
#include "mimalloc/include/mimalloc.h"

#define gg_malloc       malloc
#define gg_free         free

#define smalloc(sz)     mi_malloc_small(sz)
#define zsmalloc(sz)    mi_zalloc_small(sz)

// typed allocation
#define malloc_tp(tp)                mi_malloc_tp(tp)
#define zalloc_tp(tp)                mi_zalloc_tp(tp)
#define calloc_tp(tp,n)              mi_calloc_tp(tp,n)
#define mallocn_tp(tp,n)             mi_mallocn_tp(tp,n)
#define reallocn_tp(p,tp,n)          mi_reallocn_tp(p,tp,n)
#define recalloc_tp(p,tp,n)          mi_recalloc_tp(p,tp,n)
#define smalloc_tp(tp)               ((tp*)mi_malloc_small(sizeof(tp)))



/*
heap
    * heaps are thread local

Maximum size allowed for small allocations in #mi_malloc_small and #mi_zalloc_small
(usually `128*sizeof(void*)` (= 1KB on 64-bit systems))

        #define MI_SMALL_SIZE_MAX   (128*sizeof(void*))


*/

typedef mi_heap_t  heap_t;

// alloc type
#define heap_malloc_tp(hp,tp)        mi_heap_malloc_tp(hp,tp)
#define heap_zalloc_tp(hp,tp)        mi_heap_zalloc_tp(hp,tp)
#define heap_calloc_tp(hp,tp,n)      mi_heap_calloc_tp(hp,tp,n)
#define heap_mallocn_tp(hp,tp,n)     mi_heap_mallocn_tp(hp,tp,n)
#define heap_reallocn_tp(hp,p,tp,n)  mi_heap_reallocn_tp(hp,p,tp,n)
#define heap_recalloc_tp(hp,p,tp,n)  mi_heap_recalloc_tp(hp,p,tp,n)
#define heap_smalloc_tp(hp,tp)       ((tp*)mi_heap_malloc_small(hp,sizeof(tp)))

// => mi_heap_t*
#define heap_new()              mi_heap_new()
#define heap_set_default(heap)  mi_heap_set_default(heap)
#define heap_get_default()      mi_heap_get_default()
#define heap_get_backing()      mi_heap_get_backing()

// => void
#define heap_delete(heap)                mi_heap_delete(heap)
#define heap_destroy(heap)               mi_heap_destroy(heap)
#define heap_collect(heap, bool_force)   mi_heap_collect(heap, bool_force)

// => void*
#define heap_alloc(heap, size)                                        mi_heap_malloc_small(heap, size)
#define heap_malloc(heap, size)                                       mi_heap_malloc(heap, size)
#define heap_zalloc(heap, size)                                       mi_heap_zalloc(heap, size)
#define heap_calloc(heap, count, size)                                mi_heap_calloc(heap, count, size)
#define heap_mallocn(heap, count, size)                               mi_heap_mallocn(heap, count, size)
#define heap_malloc_aligned(heap, size, alignment)                    mi_heap_malloc_aligned(heap, size, alignment)
#define heap_malloc_aligned_at(heap, size, alignment, offset)         mi_heap_malloc_aligned_at(heap, size, alignment, offset)
#define heap_zalloc_aligned(heap, size, alignment)                    mi_heap_zalloc_aligned(heap, size, alignment)
#define heap_zalloc_aligned_at(heap, size, alignment, offset)         mi_heap_zalloc_aligned_at(heap, size, alignment, offset)
#define heap_calloc_aligned(heap, count, size, alignment)             mi_heap_calloc_aligned(heap, count, size, alignment)
#define heap_calloc_aligned_at(heap, count, size, alignment, offset)  mi_heap_calloc_aligned_at(heap, count, size, alignment, offset)
#define heap_realloc(heap, p, newsize)                                mi_heap_realloc(heap, p, newsize)
#define heap_reallocn(heap, p, count, size)                           mi_heap_reallocn(heap, p, count, size)
#define heap_reallocf(heap, p, newsize)                               mi_heap_reallocf(heap, p, newsize)
#define heap_realloc_aligned(heap, p, newsize, alignment)             mi_heap_realloc_aligned(heap, p, newsize, alignment)
#define heap_realloc_aligned_at(heap, p, newsize, alignment, offset)  mi_heap_realloc_aligned_at(heap, p, newsize, alignment, offset)
#define heap_smalloc(heap, size)                                    mi_heap_malloc_small(heap, size)

// => char*
#define heap_strdup(heap, s)                         mi_heap_strdup(heap, s)
#define heap_strndup(heap, s, n)                     mi_heap_strndup(heap, s, n)
#define heap_realpath(heap, fname, resolved_name)    mi_heap_realpath(heap, fname, resolved_name)


#include "static_assert.h"

// Please only use the ALIGNED macro before the type. Using after the variable declaration is not portable!
#if defined(_MSC_VER)
    #define INLINE          __forceinline
    #define LIKELY(x)       ((x))
    #define UNLIKELY(x)     ((x))
    #define ALIGNED(x)      __declspec(align(x))
#else
    #define INLINE          __attribute__((always_inline)) inline
    #define LIKELY(x)       (__builtin_expect(!!(x), 1))
    #define UNLIKELY(x)     (__builtin_expect(!!(x), 0))
    #define ALIGNED(x)      __attribute__((aligned(x)))
#endif