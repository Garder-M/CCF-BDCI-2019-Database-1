# CCF BDCI 2019 Database Contest

Team: default6079621



## A. Step by Step Play-book

Here shows the layout in `/data` directory:

````
/data
├── docs/                               # Some detailed documentation here
├── src/                                # All source files here
├── test/                               # Some self-made test cases for validation
├── index/                              # The created index (at first run)
├── BDCI19                              # The compiled executable
├── CMakeLists.txt
├── compile.sh                          # Use this script to compile
├── run.sh                              # Use this script to run
├── README.md
├── data/                               # Contains the 3 text files
├── customer.txt -> data/customer.txt
├── orders.txt -> data/orders.txt
├── lineitem.txt -> data/lineitem.txt
└── workspace/                          # For development - ignore it please
````



#### How to Compile

````
cd /data
./compile.sh
````

This will produce an executable located at `/data/BDCI19`.



#### How to Run

````
cd /data
./run.sh customer.txt orders.txt lineitem.txt <query_count> ...
````

The created index will be located at `/data/index`.



#### How to Clear Created Index

````
rm -rf /data/index
````



## B. Suggested Test **Steps**

1. Reboot the machine to reset system status.
2. Invoke `run.sh` with queries. It's expected to be slow, because we have to read text files from disk.
3. Delete the created index `data/index`.
4. Invoke `run.sh` with queries for many times (like 10 times). During the first run, `data/index` will be generated. For the following runs, it will be must faster since using index.

All done. The grade should be based on **Step 4**.



**NOTE 1:**

When creating index, we will try to determine whether all 3 texts file are in page cache.

- If true (like in **Step 4**), we will create index as fast as possible (which takes < 13 sec).

- If not (like in **Step 2**), we will try to create index, flush index to disk, drop page cache for index files, and reload all 3 text files to page cache. This should be relatively slow and takes 1~2 minutes. **This is to prepare for the next time that we create index (in Step 4)**

**So please DO NOT skip Step 3. Otherwise you will get poor performance because index files are flushed to disk and their page caches are dropped.**



**NOTE 2:**

If you are tired about running Step 2 + Step 3, you can simply **replace whole Step 2 + Step 3** with:

`cat customer.txt orders.txt lineitem.txt >/dev/null`

This is simply equivalent to Step 2 + Step 3. Then just **go to Step 4**.



## C. The Design

#### Theoretical Design 

Based on the given query form, we used **materialized view** to serve the queries.

The semantics can be expressed by the following SQL clause:

````sql
CREATE MATERIALIZED VIEW mv_items
AS
    SELECT
        c_mktsegment,
        o_orderdate,
        l_orderkey,
        l_shipdate,
        l_extendedprice
    FROM
        customer, orders, lineitem
    WHERE
        customer.c_custkey = orders.o_custkey
        AND lineitem.l_orderkey = orders.o_orderkey	
WITH DATA
````

When creating the materialized view, we use **hash join** (with "equal to" predicate) on:

- orders JOIN customer: Left table: orders; right table: customer; predicate c_custkey = o_custkey
-  lineitem JOIN orders: Left table: lineitem; right table: orders; predicate l_orderkey = o_orderkey



Accordingly, The the query can be expressed by

````sql
SELECT
	l_orderkey,
	o_orderdate,
	SUM(l_extendedprice) AS revenue
FROM
	mv_items
WHERE
	c_mktsegment = ?
	AND o_orderdate < ?
	AND l_shipdate > ?
GROUP BY
	l_orderkey,
	o_orderdate
ORDER BY
	revenue DESC
LIMIT ?
````

No more "JOIN" anymore!



Then, we could create index on the  materialized view mv_items. We create a **composite (multicolumn) index** (c_mktsegment, o_orderdate) named **idx_items** on mv_items, which could be expressed by

````sql
CREATE INDEX idx_items
ON mv_items
(
    c_mktsegment,
    o_orderdate
)
````

This limits each query to scanning a very limited number of order dates.



#### Further pruning

When building `mv_items`, we maintained some more statistical information. Let

- `max_shipdate_orderdate_diff = MAX(l_shipdate::date - o_orderdate::date)`
- `min_shipdate_orderdate_diff = MIN(l_shipdate::date - o_orderdate::date)`

After some simple mathematical analysis, it could be found that items in a large range of `o_orderdate` could be accepted without really checking their `l_shipdate > ?`. So we further build an materialized view (named **mv_pretopn**) based on this finding.

**mv_pretopn** could (roughly) be expressed by:

````sql
CREATE MATERIALIZED VIEW mv_pretopn
AS
    SELECT
        c_mktsegment,
        o_orderdate,
        l_orderkey,
        SUM(l_extendedprice) AS revenue
    FROM
        mv_items
    GROUP BY
        c_mktsegment,
        o_orderdate,
        l_orderkey
    ORDER BY
        c_mktsegment,
    	o_orderdate ASC,
        revenue DESC
WITH DATA

````

Also, we build composite (multicolumn) index (c_mktsegment, o_orderdate) on `mv_pretopn`.

When doing queries, items in about `(max_shipdate_orderdate_diff - min_shipdate_orderdate_diff)` order dates are retrieved from `mv_items`, all other items are retrieved from `mv_pretopn`.

Also, index on  `pretopn` is much smaller and much more efficient than index on `mv_items`.



#### Implementation Details

Currently it is a pure CPU implementation with quite good performance.

We use quite a lot of x86-specific and Linux-specific optimizations in order to get best performance, which requires deep understanding of the operating system and CPU architecture. A few of them are briefly listed here:

- We use **mult-processing** (via `fork`) but not mult-threading. This is to reduce `mmap_sem` contention when the kernel do mmap().
- We use **multiple sparse files** to store our index (which is inspired by postgresql). This is to reduce the contention on inode lock when doing file IO.
- The **index file format** is carefully designed to keep it both small and efficient.
- We carefully use the **page cache** to ensure everything we need in a query doesn't trigger a disk IO. 
- We use **hugetlb** to reduce CPU TLB miss.
- We use **AVX2** instruction set to boost index scanning.
- We reduce our **virtual memory footprint** as much as possible to reduce the time cleaning process's page table when the it exits.




## D. Reference Performance

Here attaches the performance tested on our side for reference.

| Situation                                            | Time       |
| ---------------------------------------------------- | ---------- |
| Create index, without any page cache                 | ~ 2.5 min  |
| Create index, with page cache                        | ~ 13 sec   |
| 100 random queries, each selecting top 9000~10000    | ~ 0.23 sec |
| 100 random queries, each selecting top 45000~50000   | ~ 0.45 sec |
| 100 random queries, each selecting top 90000~100000  | ~ 0.65 sec |
| 500 random queries, each selecting top 9000~10000    | ~ 0.96 sec |
| 500 random queries, each selecting top 45000~50000   | ~ 1.83 sec |
| 500 random queries, each selecting top 90000~100000  | ~ 2.85 sec |
| 1000 random queries, each selecting top 9000~10000   | ~ 1.95 sec |
| 1000 random queries, each selecting top 45000~50000  | ~ 3.60 sec |
| 1000 random queries, each selecting top 90000~100000 | ~ 5.57 sec |


