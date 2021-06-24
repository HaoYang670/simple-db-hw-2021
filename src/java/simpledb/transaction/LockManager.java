package simpledb.transaction;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.storage.PageId;

/**
 * LockManager manages all page locks in this buffer pool
 */
public class LockManager {
    private ConcurrentHashMap<PageId, Set<TwoPhaseLock>> allPageLocks;

    public LockManager(){
        allPageLocks = new ConcurrentHashMap<PageId, Set<TwoPhaseLock>>();
    }

    /**
     * A transaction acquires a lock on a page.
     * @param tid the transaction id
     * @param pid the page id
     * @param type the lock type
     */
    public void getLock(TransactionId tid, PageId pid, LockType type){
        synchronized(pid){
            TwoPhaseLock lock = new TwoPhaseLock(tid, type);
            if(holdsExpectedLock(tid, pid, type)){
                // the transaction has acquired the lock
                return;
            }
            else if(type == LockType.EXCLUSIVE){ // this is a writer
                // release reader lock if has acquires
                releaseLock(pid, tid);
                // wait for readers or writer to release the page
                while(getLockType(pid) != LockType.FREE){
                    try {
                        pid.wait(); // blocking
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else { // this is a reader
                // wait for writer to release the page
                while(getLockType(pid) == LockType.EXCLUSIVE){
                    try {
                        pid.wait(); // blocking
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            upgradeLock(pid, lock);
        }
    }

    /**
     * @param pid the page id
     * @param tid the transaction id
     */
    public void releaseLock(PageId pid, TransactionId tid){
        
        synchronized(pid){
            if(!allPageLocks.containsKey(pid)) return;

            allPageLocks.get(pid).removeIf(lock -> lock.tid == tid);
            if(allPageLocks.get(pid).isEmpty()){
                // wake up all sleep threads, because this page is free now
                pid.notifyAll();
            }
        }
    }

    /**
     * @param tid the transaction id
     * @param pid the page id
     * @return If a transaction holds any lock on the page
     */
    public LockType holdsLock(TransactionId tid, PageId pid){
        synchronized(pid){
            if(!allPageLocks.containsKey(pid)) return LockType.FREE;
            
            for(TwoPhaseLock lock : allPageLocks.get(pid)){
                if(lock.tid == tid) return lock.type;
            }
            return LockType.FREE;
        }
    }

    /**
     * @param tid the transaction id
     * @param pid the page id
     * @param expectedType the desired lock type
     * @return if the transaction holds expected type (or higher) lock on the page
     */
    private boolean holdsExpectedLock(TransactionId tid, PageId pid, LockType expectedType){
        synchronized(pid){
            if(!allPageLocks.containsKey(pid)) return false;
    
            LockType currentType = LockType.FREE;
            for(TwoPhaseLock lock : allPageLocks.get(pid)){
                if(lock.tid == tid && lock.type.higherThanOrEqual(currentType)){
                    currentType = lock.type;
                }
            }
            return currentType.higherThanOrEqual(expectedType);
        }
    }

    /**
     * add a lock to a page. 
     * @param pid the page id
     * @param lock the 2 phase lock
     */
    private void upgradeLock(PageId pid, TwoPhaseLock lock){
        synchronized(pid){
            if(holdsExpectedLock(lock.tid, pid, lock.type)){
                return;
            }
            
            if(!allPageLocks.containsKey(pid)){
                allPageLocks.put(pid, new HashSet<TwoPhaseLock>());
            }
            
            if (lock.type == LockType.EXCLUSIVE){
                // release reader lock if acquired
                allPageLocks.get(pid).removeIf(x -> x.tid == lock.tid);
            }
            
            allPageLocks.get(pid).add(lock);
        }
    }

    /**
     * @param pid the id of the page
     */
    private LockType getLockType(PageId pid){
        synchronized(pid){
            if(!allPageLocks.containsKey(pid) || allPageLocks.get(pid).isEmpty())
            return LockType.FREE;
            
            for(TwoPhaseLock lock : allPageLocks.get(pid)){
                if(lock.type == LockType.EXCLUSIVE)
                return LockType.EXCLUSIVE;
            }
            return LockType.SHARED;
        }
    }
}
