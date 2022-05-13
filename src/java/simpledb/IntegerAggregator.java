package simpledb;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbField;
    Type gbFieldType;
    int aggregateField;
    Op operator;
    TupleDesc td;

    HashMap<Field, Integer> opAggregator;
    HashMap<Field, Integer> sumAggregator;
    HashMap<Field, Integer> cntAggregator;
    Integer sum;
    Integer cnt;
    Integer num;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.operator = what;
        if(gbfield != NO_GROUPING) {
            this.opAggregator = new HashMap<Field, Integer> ();
            if(what == Aggregator.Op.AVG) {
                this.cntAggregator = new HashMap<Field, Integer> ();
                this.sumAggregator = new HashMap<Field, Integer> ();
            }
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        } else {
            sum = 0;
            cnt = 0;
            if(what == Aggregator.Op.MIN) {
                num = Integer.MAX_VALUE;
            } else if(what == Aggregator.Op.MAX) {
                num = Integer.MIN_VALUE;
            }
            this.td = new TupleDesc(new Type[]{gbfieldtype});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        if(this.gbField == NO_GROUPING) {
            Field appendField = tup.getField(this.aggregateField);
            Integer value = Integer.valueOf(((IntField)(appendField)).getValue());
            if(this.operator == Aggregator.Op.COUNT) {
                num++;
            } else if (this.operator == Aggregator.Op.AVG) {
                cnt++;
                sum += value;
                num = sum/cnt;
            } else if (this.operator == Aggregator.Op.MIN) {
                if(num > value) num = value;
            } else if (this.operator == Aggregator.Op.MAX) {
                if(num < value) num = value;
            } else if (this.operator == Aggregator.Op.SUM) {
                num += value;
            }

        } else {
            Field tmpField = tup.getField(this.gbField);
            Field appendField = tup.getField(this.aggregateField);
            if (this.operator == Aggregator.Op.SUM) {
                if (opAggregator.containsKey(tmpField)) {
                    Integer tmp = opAggregator.get(tmpField) + Integer.valueOf(((IntField) (appendField)).getValue());
                    opAggregator.put(tmpField, tmp);
                } else {
                    opAggregator.put(tmpField, Integer.valueOf(((IntField) (appendField)).getValue()));
                }
            } else if (this.operator == Aggregator.Op.MIN) {
                if (opAggregator.containsKey(tmpField)) {
                    if (Integer.valueOf(((IntField) (appendField)).getValue()) < opAggregator.get(tmpField)) {
                        opAggregator.put(tmpField, Integer.valueOf(((IntField) (appendField)).getValue()));
                    }

                } else {
                    opAggregator.put(tmpField, Integer.valueOf(((IntField) (appendField)).getValue()));
                }
            } else if (this.operator == Aggregator.Op.MAX) {
                if (opAggregator.containsKey(tmpField)) {
                    if (Integer.valueOf(((IntField) (appendField)).getValue()) > opAggregator.get(tmpField))
                        opAggregator.put(tmpField, Integer.valueOf(((IntField) (appendField)).getValue()));
                } else {
                    opAggregator.put(tmpField, Integer.valueOf(((IntField) (appendField)).getValue()));
                }
            } else if (this.operator == Aggregator.Op.AVG) {
                if (opAggregator.containsKey(tmpField)) {
                    Integer cnt = cntAggregator.get(tmpField);
                    Integer sum = sumAggregator.get(tmpField);
                    Integer val = Integer.valueOf(((IntField)(appendField)).getValue());
                    cntAggregator.put(tmpField, cnt + 1);
                    sumAggregator.put(tmpField, sum + val);
                    opAggregator.put(tmpField, (sum + val)/(cnt + 1));
                } else {
                    Integer val = Integer.valueOf(((IntField)(appendField)).getValue());
                    opAggregator.put(tmpField, val);
                    cntAggregator.put(tmpField, 1);
                    sumAggregator.put(tmpField, val);
                }
            } else if (this.operator == Aggregator.Op.COUNT) {
                if(opAggregator.containsKey(tmpField)) {
                    opAggregator.put(tmpField, opAggregator.get(tmpField) + 1);
                } else {
                    opAggregator.put(tmpField, 1);
                }
            } else {
                assert false;
            }
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
        ArrayList<Tuple> tuples = new ArrayList<Tuple> ();
        Tuple currTuple = null;
        if(this.gbField == NO_GROUPING) {
            currTuple = new Tuple(this.td);
            currTuple.setField(0, new IntField(num.intValue()));
            tuples.add(currTuple);
            return new TupleIterator(td, tuples);
        } else {
            for (Map.Entry element : this.opAggregator.entrySet()) {
                Field key = (Field) element.getKey();
                Integer value = (Integer) element.getValue();
                currTuple = new Tuple(this.td);
                currTuple.setField(0, key);
                currTuple.setField(1, new IntField(value.intValue()));
                tuples.add(currTuple);
            }
            return new TupleIterator(td, tuples);
        }
    }

}
