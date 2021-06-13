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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    /* default grouping value when there is no grouing */
    private static final Field defaultGroupingVal = new IntField(-1);
    private static final Op what = Op.COUNT; 

    private final int gbfield;
    private final Type gbfieldType;
    private final int afield;
    private final boolean noGrouping;
    private final Map<Field, Integer> groups;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != StringAggregator.what) throw new IllegalArgumentException("Only support COUNT");
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldType = gbfieldtype;
        this.noGrouping = (gbfield == NO_GROUPING);
        this.groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupingVal = noGrouping ? defaultGroupingVal : tup.getField(gbfield);
        if(!groups.containsKey(groupingVal)) {
            groups.put(groupingVal, 1);
        }
        else{
            groups.put(groupingVal, groups.get(groupingVal)+1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator(){
            private final Field[] groupingVals = groups.keySet().toArray(new Field[groups.size()]);
            private int idx = -1; // not open yet
            private TupleDesc td = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                idx = 0;                
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(idx < 0) throw new IllegalStateException("Not open yet");

                return idx < groupingVals.length;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(idx < 0) throw new IllegalStateException("Not open yet");
                if(!hasNext()) throw new NoSuchElementException();

                Tuple t = new Tuple(getTupleDesc());
                Field aggregateVal = new IntField(groups.get(groupingVals[idx]));

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
                if(idx < 0) throw new IllegalStateException("Not open yet");

                idx = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                if(this.td == null){
                    if(noGrouping){
                        this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
                    }
                    else {
                        this.td = new TupleDesc(new Type[] {gbfieldType, Type.INT_TYPE});
                    }
                }
                return this.td;
            }

            @Override
            public void close() {
                idx = -1;                
            }
        };
    }
}
