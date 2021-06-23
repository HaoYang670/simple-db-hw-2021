package simpledb.transaction;

/**
 * A transaction should holds a lock when getting a page.
 */
public class TwoPhaseLock {
    public final TransactionId tid;
    public final LockType type;
    public TwoPhaseLock(TransactionId tid, LockType type){
        this.tid = tid;
        this.type = type;
    }

    @Override
    public boolean equals(Object obj){
        if(obj == null || getClass() != obj.getClass()) return false;

        TwoPhaseLock that = (TwoPhaseLock) obj; 
        return (tid == that.tid) && (type == that.type);
    }

    @Override
    public int hashCode(){
        return (tid.hashCode() + "+" + type.hashCode()).hashCode();
    }
}
