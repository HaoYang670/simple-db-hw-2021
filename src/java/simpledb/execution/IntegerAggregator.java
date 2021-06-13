package simpledb.execution;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /* this value is used as a key when declared NO_GROUPING */
    private static final Field defaultGroupVal = new IntField(-1);

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    
    /**
     * The form is <grouping-value, [value, count]>
     * 
     * For SUM, COUNT, MIN, MAX, count = 1.
     * For COUNT the form is special: [count, 1]
     * For AVG, count is the total num of values
     * 
     * We use this form because we can get the aggregation value 
     * using one function: <value> / <count>
     */
    private final Map<Field, int[]> groups;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new HashMap<Field, int[]>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor.
     * 
     * Using hashing approach
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        final Field groupingVal = this.gbfield == NO_GROUPING ? defaultGroupVal : tup.getField(this.gbfield);
        final int aggregateVal = ((IntField) tup.getField(afield)).getValue();

        
        if(!groups.containsKey(groupingVal)){
            if(what == Op.COUNT) groups.put(groupingVal, new int[] {1, 1});
            else groups.put(groupingVal, new int[] {aggregateVal, 1});
        }
        else{
            int[] runningInfo = groups.get(groupingVal); // this is mutable, change in place
            switch (what) {
                case SUM: runningInfo[0] += aggregateVal; break;
                case MIN: runningInfo[0] = Math.min(runningInfo[0], aggregateVal); break;
                case MAX: runningInfo[0] = Math.max(runningInfo[0], aggregateVal); break;
                case COUNT: runningInfo[0] += 1; break;
                case AVG: runningInfo[0] += aggregateVal;
                          runningInfo[1] += 1;
                          break;
                default: break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator(){
            private final Field[] groupingVals = groups.keySet().toArray(new Field[groups.size()]);
            private int idx = -1; // iterator not open yet
            private final boolean noGrouping = (gbfield == NO_GROUPING);

            @Override
            public void open() throws DbException, TransactionAbortedException {
                idx = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(idx < 0) throw new IllegalStateException("not open yet");

                return idx < groupingVals.length;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(idx < 0) throw new IllegalStateException("not open yet");
                if(!this.hasNext()) throw new NoSuchElementException();

                int[] runningVal = groups.get(this.groupingVals[idx]);
                Field aggregateVal = new IntField(runningVal[0] / runningVal[1]);

                Tuple t = new Tuple(this.getTupleDesc());
                
                if(noGrouping){
                    t.setField(0, aggregateVal);
                }
                else{
                    t.setField(0, groupingVals[idx]);
                    t.setField(1, aggregateVal);
                }
                
                idx ++;
                return t;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if(idx < 0) throw new IllegalStateException("not open yet");

                idx = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (this.noGrouping) {
                    return new TupleDesc(new Type[] {Type.INT_TYPE});
                }
                else{
                    return new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
                }
            }

            @Override
            public void close() {
                idx = -1;                
            }            
        };
    }
}
