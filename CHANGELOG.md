2/10/2021

* FIX: unclosed edges iterator from `_nodes_edges_delta`
* FIX: memory leak from iterators created with malloc-ing "graph_iter_new" APIs where close() would not free top-level struct iter instances (b/c iter.release == 0)
    * For example the concatted iterators within `_delete`.
    * Solved primarily by internally relying on malloc-less "iter_init" methods.


TODO: WASM?

* possible/worthwhile?
* minimize uneeded use of `goto` (due to WASM's questionable internals)?
* impact of 32 bit?

TODO: embedded?

* gaurd against stack overflow?