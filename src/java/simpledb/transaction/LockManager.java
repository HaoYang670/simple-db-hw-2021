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
    private ConcurrentHashMap<TransactionId, Set<TwoPhaseLock>> waitForGraph;

    public LockManager(){
        allPageLocks = new ConcurrentHashMap<PageId, Set<TwoPhaseLock>>();
        waitForGraph = new ConcurrentHashMap<TransactionId, Set<TwoPhaseLock>>();
    }

    /**
     * A transaction acquires a lock on a page.
     * @param tid the transaction id
     * @param pid the page id
     * @param type the lock type
     */
    public void getLock(TransactionId tid, PageId pid, LockType type) throws TransactionAbortedException{
        TwoPhaseLock lock = new TwoPhaseLock(tid, type, pid);

        synchronized(pid){
            if(holdsExpectedLock(tid, pid, type)){
                // the transaction has acquired the lock
                return;
            }

            if(type == LockType.EXCLUSIVE){ // this is a writer
                // release reader lock if has acquires
                releaseLock(pid, tid);
                // wait for readers or writer to release the page
                // add wait-for dependency if cannot acquire the lock directly
                addDependency(tid, pid);
                
                while(getLockType(pid) != LockType.FREE){
                    if(detectDependency(tid, tid, new HashSet<>())){
                        throw new TransactionAbortedException();
                    }
                    try {
                        pid.wait(); // blocking
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            else { // this is a reader
                // wait for writer to release the page
                // add wait-for dependency if cannot acquire the lock directly
                addDependency(tid, pid);

                while(getLockType(pid) == LockType.EXCLUSIVE){
                    if(detectDependency(tid, tid, new HashSet<>())){
                        throw new TransactionAbortedException();
                    }
                    try {
                        pid.wait(); // blocking
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            upgradeLock(pid, lock);
            removeDependency(tid, pid);
        }
    }

    /**
     * Release all locks that the transaction holds on the page
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

    /**
     * Add wait-for dependency from tid to all lock holders of pid
     * @param tid the transaction id
     * @param pid the page id
     */
    private void addDependency(TransactionId tid, PageId pid){
        if(allPageLocks.containsKey(pid)){
            if(!waitForGraph.containsKey(tid)){
                waitForGraph.put(tid, new HashSet<TwoPhaseLock>());
            }
            waitForGraph.get(tid).addAll(allPageLocks.get(pid));
        }
    }

    /**
     * If there is a direct wait-for dependency from start to end, remove it
     * @param start The id of start transaction 
     * @param end The id of end transaction
     */
    private void removeDependency(TransactionId tid,PageId pid){
        synchronized(tid){
            if(waitForGraph.containsKey(tid)){
                waitForGraph.get(tid).removeIf(x -> x.pid == pid);
            }
        }
    }

    public void removeAllDependency(TransactionId tid){
        synchronized(tid){
            waitForGraph.remove(tid);
            for(TransactionId start : waitForGraph.keySet()){
                waitForGraph.get(start).removeIf(x -> x.tid == tid);
            }
        }
    }



    /**
     * Detected whether there is a wait-for path between two transactions
     * @param start the id of start transaction
     * @param end the id of end transaction
     * @param detected all start transactions that have been detected
     * @return
     */
    private boolean detectDependency(TransactionId start, TransactionId end, Set<TransactionId> detected){
        synchronized(waitForGraph){
            if(!waitForGraph.containsKey(start)) return false;
            if(detected.contains(start)) return false;
            
            detected.add(start);
            for(TwoPhaseLock lock : waitForGraph.get(start)){
                if(lock.tid == end) return true;
                if(detectDependency(lock.tid, end, detected)) return true;
            }
            return false;
        }
    }
}
