package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManager;
import simpledb.transaction.LockType;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;


import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /** Max number of pages in this buffer pool. */
    private int numPages;

    /** All pages stored in this buffer pool. */
    private Map<PageId, Page> pages; 

    /** all 2PL in the buffer pool */
    private LockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<PageId, Page>();
        this.lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        lockManager.getLock(tid, pid, getLockType(perm));
        Page requestedPage = null;

        if (this.pages.containsKey(pid)){
            requestedPage = this.pages.get(pid);
        }
        else{
            if (this.pages.size() >= this.numPages){
                evictPage();
            }
            // read page from disk
            requestedPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            this.pages.put(pid, requestedPage);
        } 

        return requestedPage;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, pid) != LockType.FREE;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2

        for(PageId pid : pages.keySet()){
            synchronized(pid) {
                if((lockManager.holdsLock(tid, pid) == LockType.EXCLUSIVE)){
                    // commit success, flush the dirty page
                    if(commit) {
                        try {
                            this.flushPage(pid);
                            // use current page contents as the before-image
                            // for the next transaction that modifies this page.
                            pages.get(pid).setBeforeImage();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    // discard current page state
                    // if other transaction access the page in the future
                    // it need to read it from disk
                    else {
                        this.discardPage(pid);
                    }
                }
            }
        }
        lockManager.releaseAllLock(tid);
        //lockManager.removeAllDependency(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // exclusive lock is acquired here
        List<Page> dirties = file.insertTuple(tid, t);

        // update pages in buffer
        for(Page p : dirties){
            updatePage(p, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // exclusive lock is acquired here
        List<Page> dirties = file.deleteTuple(tid, t);

        // update pages in buffer
        for(Page p : dirties){
            updatePage(p, tid);
        }
    }

    /**
     * Replace the existing version of page in buffer, 
     * or add the page to buffer if it is not in buffer
     * 
     * @param page the page to be updated
     * @param tid the transaction updating the page
     */
    private void updatePage(Page page, TransactionId tid){
        PageId pid = page.getId();
        page.markDirty(true, tid);

        if(!pages.containsKey(pid) && (pages.size() >= this.numPages)){
            try {
                evictPage();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        pages.put(pid, page);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId pid : pages.keySet()){
            flushPage(pid);
        }
    }

    /** <p>Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        <p>Remove a page from the buffer pool 
        without flushing it to disk
        
        <p>Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // append an update record to the log, with 
        // a before-image and after-image.
        Page p = pages.get(pid);
        TransactionId dirtier = p.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }

        DbFile table = Database.getCatalog().getDatabaseFile(pid.getTableId());
        table.writePage(pages.get(pid));
    }

    /** 
     * Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for(PageId pid : pages.keySet()){
            if((lockManager.holdsLock(tid, pid) == LockType.EXCLUSIVE) && (pages.get(pid).isDirty() != null)){
                this.flushPage(pid);
            }
        }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * <p> Implement NO STEAL stragegy
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        // randomly choose a not dirty page to evict
        // if all pages are dirty, throw exception
        PageId evictedPid = null;
        boolean isDirty = true;

        for(PageId pid : this.pages.keySet()){
            if(pages.get(pid).isDirty() == null){
                evictedPid = pid;
                isDirty = false;
                break;
            }
        }

        if(isDirty){
            throw new DbException("All pages in buffer are dirty");
        }
        else discardPage(evictedPid);
    }

    private static LockType getLockType(Permissions perm){
        if(perm == Permissions.READ_ONLY) return LockType.SHARED;
        else return LockType.EXCLUSIVE;
    }

}
