package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> value;
    private ConcurrentHashMap<Field, Integer> count;
    private static final Field NO_GROUPING = new IntField(0);
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what =  what;
    	value = new ConcurrentHashMap<Field, Integer>(); //op !=COUNT
    	count = new ConcurrentHashMap<Field, Integer>(); //op = COUNT
    }

    private int merge_op_checker(int t1, int t2) {
    	if (what == Aggregator.Op.MIN) {
    		return Math.min(t1, t2);
    	}
    	if (what == Aggregator.Op.MAX) {
    		return Math.max(t1, t2);
    	}
    	if ((what == Aggregator.Op.SUM) || (what == Aggregator.Op.AVG) || (what == Aggregator.Op.COUNT)) {
    		return t1+t2;
    	}
    	
    	return 0;
    }
    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field checkerKey = null;
    	if (gbfield == Aggregator.NO_GROUPING) {
    		checkerKey = NO_GROUPING;
    		
    	}
    	else {
    		checkerKey = tup.getField(gbfield);
    	}
    	if (value.containsKey(checkerKey)) {
    		value.put(checkerKey, merge_op_checker(value.get(checkerKey), ((IntField) tup.getField(afield)).getValue()));
    		count.put(checkerKey, count.get(checkerKey) + 1);
    		
    	}
    	else
    	{
    		value.put(checkerKey, ((IntField) tup.getField(afield)).getValue());
    		count.put(checkerKey, 1);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	List<Tuple> listOfTuple = new ArrayList<Tuple>();
        if(gbfield == Aggregator.NO_GROUPING) {
        	TupleDesc tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
        	Tuple tuple = new Tuple(tupleDesc);
        	int index = value.get(NO_GROUPING);
        	if (what == Aggregator.Op.COUNT) {
        		index = count.get(NO_GROUPING);
        	}
        	else if(what == Aggregator.Op.AVG) {
        		index = index/(count.get(NO_GROUPING));
        	}
        	tuple.setField(0, new IntField(index));
        	listOfTuple.add(tuple);
        	return new TupleIterator(tupleDesc, listOfTuple);
        }
        else {
        	TupleDesc tupleDesc = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
        	for (Field key : value.keySet()) {
        		Tuple tuple = new Tuple(tupleDesc);
        		int index = value.get(key);
        		if (what == Aggregator.Op.COUNT) {
            		index = count.get(key);
            	}
            	else if(what == Aggregator.Op.AVG) {
            		index = index/(count.get(key));
            	}
        		tuple.setField(0, key);
        		tuple.setField(1, new IntField(index));
        		listOfTuple.add(tuple);
        		
        	}
        	return new TupleIterator(tupleDesc, listOfTuple);
        }
       
    }
}
