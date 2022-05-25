package simpledb;

import java.io.*;
import java.util.*;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */

public class BufferPool {

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int MAX_PAGES;
    private Cache cache;
    private HashMap<TransactionId, Set<PageId>> transactions;

    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.MAX_PAGES = numPages;
        this.cache = new Cache(numPages);
        this.transactions = new HashMap<TransactionId, Set<PageId>> ();
        this.lockManager = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, InterruptedException {
        try {
            this.lockManager.acquireLock(tid, pid, perm);
        } catch (DeadlockException e) {
            throw new TransactionAbortedException();
        }

        if (cache.containsKey(pid)) {
            Page page = this.cache.get(pid);
            this.insertTransactions(tid, pid);
            return page;
        } else {
            HeapFile hf = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
            Page page = (Page) hf.readPage(pid);
            this.cache.put(pid, page);
            this.insertTransactions(tid, pid);
            return page;
        }
    }

    private void insertTransactions(TransactionId tid, PageId pid) {
        if(!this.transactions.containsKey(tid)) {
            this.transactions.put(tid, new HashSet<PageId> ());
        }
        this.transactions.get(tid).add(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return this.lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        if(commit) {
            this.flushPages(tid);
        } else {
            // "you should throw away any changes to pages that it made"
            this.rollBack(tid);
        }

        this.transactions.remove(tid);
        this.lockManager.releaseAllLocks(tid);
    }

    public void rollBack(TransactionId tid) {
        Set<PageId> pids = this.transactions.get(tid);
        // go through all the cache and remove all those pids from the cache
        this.cache.removePids(pids);
    }
    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> pages = hf.insertTuple(tid, t);

        if(!this.transactions.containsKey(tid)) {
            this.transactions.put(tid, new HashSet<PageId> ());
        }
        Page page;
        for(int i = 0; i < pages.size(); i++) {
            page = pages.get(i);
            page.markDirty(true, tid);
            this.cache.put(page.getId(), page);
            this.transactions.get(tid).add(page.getId());
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableId);
        Page pg = hf.deleteTuple(tid, t);
        pg.markDirty(true, tid);
        this.cache.put(pg.getId(), pg);
        this.insertTransactions(tid, pg.getId());
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(TransactionId tid: this.transactions.keySet()) {
            this.flushPages(tid);
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.
     */
    public synchronized void discardPage(PageId pid) {
        if(this.cache.containsKey(pid)) {
            this.cache.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = this.cache.get(pid);
        assert page != null;
        // I need to get the HeapFile
        DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        try {
            Set<PageId> pids = this.transactions.get(tid);
            if(pids == null) return;

            Iterator<PageId> it = pids.iterator();
            PageId pid;
            while (it.hasNext()) {
                pid = it.next();
                if (this.cache.containsKey(pid) && this.cache.get(pid).isDirty() != null) {
                    flushPage(pid);
                }
            }
        } catch (Exception e) {
            System.out.println("exception caught");
            e.printStackTrace();
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        PageId pid = this.cache.getEvictingKey();
        if(pid != null) {
            Page page = this.cache.get(pid);
            assert page.isDirty() == null;
            this.cache.evict();
        }
    }

}
