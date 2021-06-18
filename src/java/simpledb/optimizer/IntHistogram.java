package simpledb.optimizer;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int min;
    private final int max;
    private final float width;
    private final int[] buckets;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param numBuckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int numBuckets, int min, int max) {
    	// some code goes here
        this.max = max;
        this.min = min;
        this.width = (max - min) / (float) numBuckets;
        this.buckets = new int[numBuckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        final int idx = getIdx(v);
        buckets[idx] ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double equal = 0.0;
        double greaterThan = 0.0;

        if(v < min) greaterThan = 1.0;
        else if(v <= max){
            final int idx = getIdx(v);
            // inclusive
            final int higherBound = (int) (width * (idx+1));
            final double numVals = (double) getNumVals();
            equal = (buckets[idx] / width) / numVals;
    
            greaterThan = equal * (higherBound - v);
            for(int i=idx+1; i<buckets.length; i++){
                greaterThan += buckets[i];
            }
            greaterThan /= numVals;
        }

        switch (op) {
            case EQUALS: return equal;
            case NOT_EQUALS: return 1 - equal;
            case GREATER_THAN: return greaterThan;
            case GREATER_THAN_OR_EQ: return equal + greaterThan;
            case LESS_THAN: return 1 - equal - greaterThan;
            case LESS_THAN_OR_EQ: return 1 - greaterThan;
            default: return 1.0;
        }
    }

    /**
     * @param v Value
     * @return the index of bucket that v belongs to.
     */
    private int getIdx(int v){
        return Math.min((int) ((v - min) / width), buckets.length-1);
    }

    /**
     * @return the number of values added to this histogram.
     */
    private int getNumVals(){
        int numVals = 0;
        for(int n : buckets){
            numVals += n;
        }
        return numVals;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity(){
        // some code goes here
        return  getNumVals() / (double) buckets.length;
    }
    

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String str = "";

        for(int i=0; i<buckets.length; i++){
            str += "bucket: " + i + "number of values: " + buckets[i] + "\n";
        }
        return str;
    }
}
