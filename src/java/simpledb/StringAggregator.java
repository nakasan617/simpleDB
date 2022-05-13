package simpledb;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbField;
    Type gbFieldType;
    int aggregateField;
    Op operator;
    TupleDesc td;

    HashMap<Field, Integer> cntAggregator;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        if(gbfield != NO_GROUPING) {
            this.cntAggregator = new HashMap<Field, Integer> ();
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        } else {
            this.cntAggregator = new HashMap<Field, Integer> ();
            this.td = new TupleDesc(new Type[]{gbfieldtype});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tmpField = tup.getField(this.gbField);
        if(this.gbField == NO_GROUPING) {
            this.cntAggregator.put(tmpField, 0);
        } else {
            if(this.operator == Aggregator.Op.COUNT) {
                if(this.cntAggregator.containsKey(tmpField)) {
                    this.cntAggregator.put(tmpField, this.cntAggregator.get(tmpField) + 1);
                } else {
                    this.cntAggregator.put(tmpField, 1);
                }
            }
        }
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
        ArrayList<Tuple> tuples = new ArrayList<Tuple> ();
        Tuple currTuple = null;
        for(Map.Entry element: this.cntAggregator.entrySet()) {
            Field key = (Field)element.getKey();
            Integer value = (Integer)element.getValue();
            currTuple = new Tuple(this.td);
            currTuple.setField(0, key);
            if(this.gbField != NO_GROUPING) {
                currTuple.setField(1, new IntField(value.intValue()));
            }
            tuples.add(currTuple);
        }
        return new TupleIterator(td, tuples);
    }

}
