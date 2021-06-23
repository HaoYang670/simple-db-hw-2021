package simpledb.transaction;

/** Lock types in 2PL 
 * <p>FREE: neither readers nor writer acquire the lock
 * <p>SHARED: one or more readers acquire the lock, no writer 
 * <p>EXCLUSIVE: just one writer acquire the lock
*/
public enum LockType {
    FREE, SHARED, EXCLUSIVE;

    /**
     * @param that another lock type
     * @return if this type is higher than or equal to that
     */
    public boolean higherThanOrEqual(LockType that){
        if(that == FREE) return true;
        if(that == SHARED) return this != FREE;
        else return this == EXCLUSIVE;
    }

    @Override
    public String toString(){
        switch (this) {
            case FREE: return "FREE";
            case SHARED: return "SHARED";
            case EXCLUSIVE: return "EXCLUSIVE";
            default: return "???";
        }
    }
}
