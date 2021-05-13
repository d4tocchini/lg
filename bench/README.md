* nodes
    - nodes total time: 1.051608 sec
    - nodes insert rate: 950,924.679158 / sec
    - commit updates: 1000000
* props
    - props total time: 0.987498 sec
    - props insert rate: 1,012,660.278806 / sec
    - commit updates: 1000000
* edges
    - edges total time: 0.852036 sec
    - edges insert rate: 586,829.664474 / sec
    - commit updates: 500000
* nodes iterate
    - nodes iterate total time: 0.278161 sec
    - nodes iterate op rate: 3,595,040.282426 / sec
* kv_put x 1M
    - kv_put total time: 0.601404 sec
    - kv_put op rate: 1,662,775.771362 / sec
* kv_get x 1M
    - kv_get total time: 0.391792 sec
    - kv_get op rate: 2,552,374.729448 / sec
* kv_iterate x 1M
    - kv_iterate total time: 0.022987 sec
    - kv_iterate op rate: 43,502,849.436638 / sec

# graph

https://github.com/linuxerwang/dgraph-bench

https://dgraph.io/blog/post/benchmark-neo4j/

https://nebula-graph.io/posts/benchmarking-mainstraim-graph-databases-dgraph-nebula-graph-janusgraph/

# kv

http://www.lmdb.tech/bench/microbench/benchmark.html


/* JS obj/map bench

chrome:
    obj_set: 366.464111328125 ms
    obj_get: 15.998291015625 ms
    map_set: 704.948974609375 ms
    map_get: 119.78466796875 ms
safari:
    obj_set: 19.012ms
    obj_get: 6.094ms
    map_set: 195.853ms
    map_get: 163.748ms

        (function(){
            const n = 1000000;
            let i ;

            console.time("obj_set");
            i = 0;
            var o = {}
            while (i<n) {
                o[i] = i*(++i);
            }
            console.timeEnd("obj_set");

            console.time("obj_get");
            i = 0;
            while (i<n) {
                if (o[i] !== i*(++i)) throw new Error("obj_get!!!")
            }
            console.timeEnd("obj_get");

            console.time("map_set");
            i = 0;
            var m = new Map()
            while (i<n) {
                m.set(i,i*(++i));
            }
            console.timeEnd("map_set");

            console.time("map_get");
            i = 0;
            while (i<n) {
                if (m.get(i) !== i*(++i)) throw new Error("map_get!!!")
            }
            console.timeEnd("map_get");
        })()

*/