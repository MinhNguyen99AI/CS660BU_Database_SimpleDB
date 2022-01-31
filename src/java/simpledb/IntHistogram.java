package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

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
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	private int buckets;
	private int min;
	private int max;
	private int[] histogram;
	
	private int sizeCurrent;
	private int numVal;
	
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.min = min;
    	this.max = max;
    	this.buckets = buckets;
    	this.histogram = new int[buckets];
    	this.sizeCurrent = (int) Math.ceil((double) (max - min + 1)/buckets);
    	
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int index = (int) Math.ceil((v - this.min)/this.sizeCurrent);
    	this.histogram[index] += 1;
    	this.numVal += 1;
    	
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
	
		switch (op) {
    		case EQUALS:
    			return selectivityEqual(v);
    		case LESS_THAN_OR_EQ:
    			double equal = selectivityEqual(v);
    			double notEqual = selectivityNotEqual(Predicate.Op.LESS_THAN, v);
    			return equal + notEqual;
    		case LESS_THAN:
    			return  selectivityNotEqual(Predicate.Op.LESS_THAN, v);
    		case NOT_EQUALS:
    			return 1-selectivityEqual(v);
    		case GREATER_THAN_OR_EQ:
    			double equal2 = selectivityEqual(v);
    			double notEqual2 = selectivityNotEqual(Predicate.Op.GREATER_THAN, v);
    			return equal2 + notEqual2;
    		case GREATER_THAN:
    			return selectivityNotEqual(Predicate.Op.GREATER_THAN, v);
    		
		}
        return -1.0;
    }
    
    private int getBucketIndex(int v) {
    	int index = (int) Math.ceil((v - this.min)/this.sizeCurrent);
    	if (index < 0)
    		return -1;
    	if (index >= this.buckets){
    		return this.buckets;
    	
    	}
		return index;
    }
    
    private double selectivityEqual( int v) {
    	int index = getBucketIndex(v);
    	if (index == -1) return 0;
    	if (index == this.buckets) return 0;
    	return (double) ((double) (this.histogram[index])/this.numVal);
    	
    }
    private double selectivityNotEqual(Predicate.Op op, int v) {
    	int index = getBucketIndex(v);
    	int bucketFull, bucketLeft, bucketRight, bucketHeight;
    	if (index == this.buckets) {
    		bucketRight = this.buckets;
    		bucketLeft = this.buckets-1;
    		bucketFull = 0;	
    		bucketHeight = 0;
    	}
    	else if (index == -1) {
    		bucketRight = 0;
    		bucketLeft = -1;
    		bucketFull = 0;
    		bucketHeight = 0;
    	}
    	else {
    		bucketRight = index + 1;
    		bucketLeft = index -1;
    		bucketFull = -1;
    		bucketHeight = this.histogram[index];
    		}
    	double select = 0;
    	switch (op) {
			case GREATER_THAN:
				if (bucketFull == -1) bucketFull = (bucketRight*this.sizeCurrent+this.min-v)/this.sizeCurrent;
				select = (bucketHeight*bucketFull)/this.numVal;
				if (bucketRight >= this.buckets) return select/this.numVal;
				
				for (int i = bucketRight; i<buckets; i++) {
					select += this.histogram[i];
				}
				return select/this.numVal;
			case LESS_THAN:
				if (bucketFull == -1) bucketFull = (v-(bucketLeft*this.sizeCurrent)+this.min)/this.sizeCurrent;
				select = (bucketHeight*bucketFull)/this.numVal;
				if (bucketLeft < 0) return select/this.numVal;
				
				for (int i = bucketLeft; i>=0; i--) {
					select += this.histogram[i];
				}
				return select/this.numVal;
		default:
			return -1;
    	}
    	
    	
    }
    
    
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
    	StringBuilder toString = new StringBuilder();
    	toString.append("buckets = ");
    	toString.append(buckets);
    	toString.append( "min = ");
	   	toString.append(min);
	   	toString.append(" max = ");
	   	toString.append(max);
	   	return toString.toString();
        
    }
}
