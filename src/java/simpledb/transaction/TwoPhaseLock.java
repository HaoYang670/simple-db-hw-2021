package simpledb.transaction;

import simpledb.storage.PageId;

/**
 * A transaction should holds a lock when getting a page.
 */
public class TwoPhaseLock {
    public final TransactionId tid;
    public final LockType type;
    public final PageId pid;
    public TwoPhaseLock(TransactionId tid, LockType type, PageId pid){
        this.tid = tid;
        this.type = type;
        this.pid = pid;
    }

    @Override
    public boolean equals(Object obj){
        if(obj == null || getClass() != obj.getClass()) return false;

        TwoPhaseLock that = (TwoPhaseLock) obj; 
        return (tid == that.tid) && (type == that.type) && (pid == that.pid);
    }

    @Override
    public int hashCode(){
        return (tid.hashCode() + "+" + type.hashCode() + "+" + pid.hashCode()).hashCode();
    }
}
