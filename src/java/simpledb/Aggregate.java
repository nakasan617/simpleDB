package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    DbIterator child;
    int aggregateField;
    int groupingField;
    Aggregator.Op aop;
    TupleDesc td;

    private DbIterator content;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.child = child;
        this.aggregateField = afield;
        this.groupingField = gfield;
        this.aop = aop;
        this.td = child.getTupleDesc();
        this.content = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return this.groupingField; // NO_GROUPING should be taken care of by this.
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    if(this.groupingField == Aggregator.NO_GROUPING) {
            return null;
        } else {
            return this.td.getFieldName(this.groupingField);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return this.aggregateField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return this.td.getFieldName(this.aggregateField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        Type type;
	    super.open();
        if(this.content == null) {
            child.open();
            Aggregator agtr = null;
            if(this.groupingField != Aggregator.NO_GROUPING)
                type = this.td.getFieldType(this.groupingField);
            else
                type = null;

            if (this.td.getFieldType(this.aggregateField) == Type.INT_TYPE) {
//                System.out.println("this.groupingField: " + this.groupingField);
                agtr = new IntegerAggregator(this.groupingField, type, this.aggregateField, this.aop);
            } else {
                agtr = new StringAggregator(this.groupingField, type, this.aggregateField, this.aop);
            }

            int cnt = 0;
            while (child.hasNext()) {
                Tuple tuple = child.next();
                agtr.mergeTupleIntoGroup(tuple);
            }

            content = agtr.iterator();
            child.close();
            content.open();
        } else {
            this.content.rewind();
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(content != null && content.hasNext()) {
            return content.next();
        } else if(content == null){
            throw new DbException("it hasn't been opened");
        } else {
            // !content.hasNext()
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    this.content.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() { // you need to get the tuple desc of whatever you have created
        if(this.groupingField == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type [] {Type.INT_TYPE});
        } else {
            return new TupleDesc(new Type [] {this.td.getFieldType(this.groupingField), Type.INT_TYPE});
        }
    }

    public void close() {
        this.content.close();
	    super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator dbIterator [] = new DbIterator[1];
        dbIterator[0] = this.child;
        return dbIterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    this.child = children[0];
    }
    
}
