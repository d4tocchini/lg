#ifndef _DB_H
#define _DB_H

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include <stddef.h>
#include <pthread.h>

// status codes
#define DB_SUCCESS 0
#define DB_NOTFOUND (-30798)

// env/txn flags
#define DB_FIXEDMAP    0x0000001 /** mmap at a fixed address (experimental) */
// *      	use a fixed address for the mmap region. This flag must be specified
// *      	when creating the environment, and is stored persistently in the environment.
// *		If successful, the memory map will always reside at the same virtual address
// *		and pointers used to reference data items in the database will be constant
// *		across multiple invocations. This option may not always work, depending on
// *		how the operating system has allocated memory to shared libraries and other uses.
// *		The feature is highly experimental.
#define DB_NOSYNC      0x0010000 /** don't fsync after commit */
// *		Don't flush system buffers to disk when committing a transaction.
// *		This optimization means a system crash can corrupt the database or
// *		lose the last transactions if buffers are not yet flushed to disk.
// *		The risk is governed by how often the system flushes dirty buffers
// *		to disk and how often #mdb_env_sync() is called.  However, if the
// *		filesystem preserves write order and the #MDB_WRITEMAP flag is not
// *		used, transactions exhibit ACI (atomicity, consistency, isolation)
// *		properties and only lose D (durability).  I.e. database integrity
// *		is maintained, but a system crash may undo the final transactions.
// *		Note that (#MDB_NOSYNC | #MDB_WRITEMAP) leaves the system with no
// *		hint for when to write transactions to disk, unless #mdb_env_sync()
// *		is called. (#MDB_MAPASYNC | #MDB_WRITEMAP) may be preferable.
// *		This flag may be changed at any time using #mdb_env_set_flags().
#define DB_RDONLY      0x0020000
// *		Open the environment in read-only mode. No write operations will be
// *		allowed. LMDB will still modify the lock file - except on read-only
// *		filesystems, where LMDB does not use locks.
#define DB_NOMETASYNC  0x0040000 /** don't fsync metapage after commit */
// *		Flush system buffers to disk only once per transaction, omit the
// *		metadata flush. Defer that until the system flushes files to disk,
// *		or next non-MDB_RDONLY commit or #mdb_env_sync(). This optimization
// *		maintains database integrity, but a system crash may undo the last
// *		committed transaction. I.e. it preserves the ACI (atomicity,
// *		consistency, isolation) but not D (durability) database property.
// *		This flag may be changed at any time using #mdb_env_set_flags().
#define DB_WRITEMAP    0x0080000
// *		Use a writeable memory map unless MDB_RDONLY is set. This uses
// 	*		fewer mallocs but loses protection from application bugs
// 	*		like wild pointer writes and other bad updates into the database.
// 	*		This may be slightly faster for DBs that fit entirely in RAM, but
// 	*		is slower for DBs larger than RAM.
// 	*		Incompatible with nested transactions.
// 	*		Do not mix processes with and without MDB_WRITEMAP on the same
// 	*		environment.  This can defeat durability (#mdb_env_sync etc).
#define DB_MAPASYNC    0x0100000
// *		When using #MDB_WRITEMAP, use asynchronous flushes to disk.
// *		As with #MDB_NOSYNC, a system crash can then corrupt the
// *		database or lose the last transactions. Calling #mdb_env_sync()
// *		ensures on-disk database integrity until next commit.
// *		This flag may be changed at any time using #mdb_env_set_flags().
#define DB_NOTLS       0x0200000
// *		Don't use Thread-Local Storage. Tie reader locktable slots to
// *		#MDB_txn objects instead of to threads. I.e. #mdb_txn_reset() keeps
// *		the slot reseved for the #MDB_txn object. A thread may use parallel
// *		read-only transactions. A read-only transaction may span threads if
// *		the user synchronizes its use. Applications that multiplex many
// *		user threads over individual OS threads need this option. Such an
// *		application must also serialize the write transactions in an OS
// *		thread, since LMDB's write locking is unaware of the user threads.
#define DB_NOLOCK      0x0400000
// *		Don't do any locking. If concurrent access is anticipated, the
// *		caller must manage all concurrency itself. For proper operation
// *		the caller must enforce single-writer semantics, and must ensure
// *		that no readers are using old transactions while a writer is
// *		active. The simplest approach is to use an exclusive lock so that
// *		no readers may be active at all when a writer begins.
#define DB_NORDAHEAD   0x0800000
// * 		Turn off readahead. It's harmful when the DB is larger than RAM.
// *		Turn off readahead. Most operating systems perform readahead on
// *		read requests by default. This option turns it off if the OS
// *		supports it. Turning it off may help random read performance
// *		when the DB is larger than RAM and system RAM is full.
// *		The option is not implemented on Windows.
#define DB_NOMEMINIT   0x1000000
// *		Don't initialize malloc'd memory before writing to unused spaces
// *		in the data file. By default, memory for pages written to the data
// *		file is obtained using malloc. While these pages may be reused in
// *		subsequent transactions, freshly malloc'd pages will be initialized
// *		to zeroes before use. This avoids persisting leftover data from other
// *		code (that used the heap and subsequently freed the memory) into the
// *		data file. Note that many other system libraries may allocate
// *		and free memory from the heap for arbitrary uses. E.g., stdio may
// *		use the heap for file I/O buffers. This initialization step has a
// *		modest performance cost so some applications may want to disable
// *		it using this flag. This option can be a problem for applications
// *		which handle sensitive data like passwords, and it makes memory
// *		checkers like Valgrind noisy. This flag is not needed with #MDB_WRITEMAP,
// *		which writes directly to the mmap instead of using malloc for pages. The
// *		initialization is also skipped if #MDB_RESERVE is used; the
// *		caller is expected to overwrite all of the memory that was
// *		reserved in that case.
// *		This flag may be changed at any time using #mdb_env_set_flags().
#define DB_PREVMETA    0x2000000
// *		Open the environment with the previous snapshot rather than the latest
// *		one. This loses the latest transaction, but may help work around some
// *		types of corruption. If opened with write access, this must be the
// *		only process using the environment. This flag is automatically reset
// *		after a write transaction is successfully committed.

// db flags
#define DB_REVERSEKEY  0x00002
// *		Keys are strings to be compared in reverse order, from the end
// *		of the strings to the beginning. By default, Keys are treated as strings and
// *		compared from beginning to end.
#define DB_DUPSORT     0x00004
// *		Duplicate keys may be used in the database. (Or, from another perspective,
// *		keys may have multiple data items, stored in sorted order.) By default
// *		keys must be unique and may have only a single data item.
#define DB_INTEGERKEY  0x00008
// *		Keys are binary integers in native byte order, either unsigned int
// *		or #mdb_size_t, and will be sorted as such.
// *		(lmdb expects 32-bit int <= size_t <= 32/64-bit mdb_size_t.)
// *		The keys must all be of the same size.
#define DB_DUPFIXED    0x00010
// *		This flag may only be used in combination with #MDB_DUPSORT. This option
// *		tells the library that the data items for this database are all the same
// *		size, which allows further optimizations in storage and retrieval. When
// *		all data items are the same size, the #MDB_GET_MULTIPLE, #MDB_NEXT_MULTIPLE
// *		and #MDB_PREV_MULTIPLE cursor operations may be used to retrieve multiple
// *		items at once.
#define DB_INTEGERDUP  0x00020
// *		This option specifies that duplicate data items are binary integers,
// *		similar to #MDB_INTEGERKEY keys.
#define DB_REVERSEDUP  0x00040
// *		This option specifies that duplicate data items should be compared as
// *		strings in reverse order.
#define DB_CREATE      0x40000
// *		Create the named database if it doesn't exist. This option is not
// *		allowed in a read-only transaction or a read-only environment.

// write flags
#define DB_NOOVERWRITE 0x00010
// * 		For put: Don't write if the key already exists.
#define DB_NODUPDATA   0x00020
// 		Only for #DB_DUPSORT:
// * 			For put: don't write if the key and data pair already exist.
// * 			For mdb_cursor_del: remove all duplicate data items.
#define DB_CURRENT     0x00040
// * 		For mdb_cursor_put: overwrite the current key/data pair
#define DB_RESERVE     0x10000
#define DB_APPEND      0x20000
#define DB_APPENDDUP   0x40000
#define DB_MULTIPLE    0x80000

typedef enum db_cursor_op {
    DB_FIRST,
    DB_FIRST_DUP,
    DB_GET_BOTH,
    DB_GET_BOTH_RANGE,
    DB_GET_CURRENT,
    DB_GET_MULTIPLE,
    DB_LAST,
    DB_LAST_DUP,
    DB_NEXT,
    DB_NEXT_DUP,
    DB_NEXT_MULTIPLE,
    DB_NEXT_NODUP,
    DB_PREV,
    DB_PREV_DUP,
    DB_PREV_NODUP,
    DB_SET,
    DB_SET_KEY,
    DB_SET_RANGE,
    DB_PREV_MULTIPLE,
} db_cursor_op;

// txn_end flags
#define DB_TXN_ABORT  1

typedef struct db_t * db_t;
typedef struct txn_t * txn_t;
typedef struct cursor_t * cursor_t;
typedef struct iter_t * iter_t;
typedef struct db_snapshot_t * db_snapshot_t;
typedef struct buffer_t buffer_t;
typedef int (*db_cmp_func)(const buffer_t *a, const buffer_t *b);
typedef unsigned int db_dbi;

typedef const struct {
	const char *const name;
	const unsigned int flags;
	db_cmp_func cmp;
} dbi_t;

typedef struct buffer_t {
	size_t size;   	// size of the data item
	union {
		void    * data; // address of the data item
		char    * ch;
		uint8_t * u8;
		uint16_t* u16;
		uint32_t* u32;
		uint64_t* u64;
	};
} buffer_t;

// base database object
struct db_t {
	void *env;
	db_dbi *handles;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	size_t padsize;
	int fd, txns;
	int updated : 1;
	int release : 1;
};

// base txn object
struct txn_t {
	cursor_t head; // must be first
	db_t db;
	txn_t parent;
	void *txn;
	int ro : 1;
	int rw : 1;
	int updated : 1;
	int release : 1;
};

// base cursor object
struct cursor_t {
	cursor_t next;  // must be first
	cursor_t prev;
	void *cursor;
	txn_t txn;
	int release : 1;
};

// base iterator object
struct iter_t {
	struct cursor_t cursor;
	buffer_t key;
	union {
		buffer_t val;
		buffer_t data;
	};
	void *pfx;
	unsigned int pfxlen;
	db_cursor_op op;
	int r;
	int release : 1;
};

char *db_strerror(int err);

int db_new(db_t *db, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbi, size_t padsize);
int db_init(db_t db, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbi, size_t padsize);
int db_get(txn_t txn, int dbi, buffer_t *key, buffer_t *data);
int db_put(txn_t txn, int dbi, buffer_t *key, buffer_t *data, unsigned int flags);
int db_del(txn_t txn, int dbi, buffer_t *key, buffer_t *data);
int db_drop(txn_t txn, int dbi, int del);
int db_sync(db_t db, int force);
int db_updated(db_t db);
void db_close(db_t db);
int db_size(db_t db, size_t *size);
int db_remap(db_t db);

	/** @brief Return the path that was used in #mdb_env_open().
	 *
	 * @param[in] db An environment handle returned by #mdb_env_create()
	 * @param[out] path Address of a string pointer to contain the path. This
	 * is the actual string in the environment, not a copy. It should not be
	 * altered in any way.
	 * @return A non-zero error value on failure and 0 on success. Some possible
	 * errors are:
	 * <ul>
	 *	<li>EINVAL - an invalid parameter was specified.
	 * </ul>
	 */
int db_get_path(db_t db, const char **path);
// #define db_get_path(db, path_pp) mdb_env_get_path((MDB_env *)(( (db_t)db )->env), path)


// will fail if this process has active txns/snapshots
// supplied mapsize must be a multiple of the OS pagesize
int db_set_mapsize(db_t db, size_t mapsize);

// these are always safe
int db_get_mapsize(db_t db, size_t *size);
int db_get_disksize(db_t db, size_t *size);

// snapshot foo
int db_snapshot_to_fd(db_t db, int fd, int compact);
db_snapshot_t db_snapshot_new(db_t db, int compact);
ssize_t db_snapshot_read(db_snapshot_t snap, void *buffer, size_t len);
int db_snapshot_close(db_snapshot_t snap);
int db_snapshot_fd(db_snapshot_t snap);

int db_txn_new(txn_t *txn, db_t db, txn_t parent, int flags);
int db_txn_init(txn_t txn, db_t db, txn_t parent, int flags);
int txn_updated(txn_t txn);
void txn_abort(txn_t txn);
int txn_commit(txn_t txn);
int txn_end(txn_t txn, int flags);

int txn_cursor_new(cursor_t *cursor, txn_t txn, int dbi);
int txn_cursor_init(cursor_t cursor, txn_t txn, int dbi);

int cursor_get(cursor_t cursor, buffer_t *key, buffer_t *data, db_cursor_op op);
int cursor_put(cursor_t cursor, buffer_t *key, buffer_t *data, unsigned int flags);
int cursor_del(cursor_t cursor, unsigned int flags);
int cursor_count(cursor_t cursor, size_t *count);
int cursor_first(cursor_t cursor, buffer_t *key, buffer_t *val, uint8_t *pfx, const unsigned int pfxlen);
int cursor_first_key(cursor_t cursor, buffer_t *key, uint8_t *pfx, const unsigned int pfxlen);
int cursor_last(cursor_t cursor, buffer_t *key, buffer_t *val, uint8_t *pfx, const unsigned int pfxlen);
int cursor_last_key(cursor_t cursor, buffer_t *key, uint8_t *pfx, const unsigned int pfxlen);
void cursor_close(cursor_t cursor);

int txn_iter_new(iter_t *iter, txn_t txn, int dbi, void *pfx, const unsigned int len);
int txn_iter_init(iter_t iter, txn_t txn, int dbi, void *pfx, const unsigned int len);
int iter_seek(iter_t iter, void *pfx, const unsigned int len);
int iter_next(iter_t iter);
int iter_next_key(iter_t iter);
void iter_close(iter_t iter);

#endif
