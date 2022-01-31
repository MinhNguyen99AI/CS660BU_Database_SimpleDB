package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;
    private HashMap<Field, Integer> count = new HashMap<>();
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	if (gbfield != NO_GROUPING)
    		this.tupleDesc = new TupleDesc(new Type[] {gbfieldtype,Type.INT_TYPE});
    	else
    		this.tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE});
    }
    

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field group;
    	if(gbfield != NO_GROUPING) {
    		group = tup.getField(gbfield);
    	}
    	else {
    		group = null;
    	}
    	count.put(group, count.getOrDefault(group, 0)+1);
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	ArrayList<Tuple> listOfTuple = new ArrayList<>();
    	for (Entry<Field, Integer> iterateEntry : count.entrySet()) {
    		Field key = iterateEntry.getKey();
    		Integer index = iterateEntry.getValue();
    		Tuple tup = new Tuple(tupleDesc);
    		if (gbfield != NO_GROUPING) {
    			tup.setField(0, key);
    			tup.setField(1, new IntField(index));
    			
    		}
    		else {
    			tup.setField(0, new IntField(index));
			}
    		listOfTuple.add(tup);
    	}
    	return new TupleIterator(tupleDesc, listOfTuple);
    	
    }

}
