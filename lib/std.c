// TODO:
// #include"lmdb.h"

// MI_STAT MI_DEBUG
// NOTE: MacOS is def faster with simple override header macros
    // #define MI_MALLOC_OVERRIDE 1
    // #define MI_OSX_ZONE 1
    // #define MI_INTERPOSE  1

// Minimal alignment necessary. On most platforms 16 bytes are needed
// due to SSE registers for example. This must be at least `MI_INTPTR_SIZE`
// #define MI_MAX_ALIGN_SIZE  16   // sizeof(max_align_t)

#include "mimalloc/mimalloc.c"

#include "std.h"

#include "lmdb/lmdb.c"