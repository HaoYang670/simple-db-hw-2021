package simpledb.storage;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private TupleDesc schema;
    private Field[] contents;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.schema = td;
        this.contents = new Field[td.numFields()];
        this.rid = null;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        this.contents[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return this.contents[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String expression = "";

        for(int i=0; i<contents.length; i++){
            expression += contents[i].toString();

            if(i != contents.length-1){
                expression += "\t";
            }
        }
        return expression;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields(){

        // some code goes here
        return new Iterator<Field>(){

            int idx = 0;

            @Override
            public boolean hasNext() {
                return idx < contents.length;
            }

            @Override
            public Field next() {
                Field f = contents[idx];
                idx++;
                return f;
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td){
        // some code goes here
        this.schema = td;
    }

    public static Tuple merge(Tuple t1, Tuple t2){
        TupleDesc mergedTd = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple merged = new Tuple(mergedTd);

        int fieldId = 0;
        Iterator<Field> fields1 = t1.fields();
        Iterator<Field> fields2 = t2.fields();

        while(fields1.hasNext()){
            merged.setField(fieldId, fields1.next());
            fieldId ++;
        }
        while(fields2.hasNext()){
            merged.setField(fieldId, fields2.next());
            fieldId ++;
        }
        return merged;
    }
}
