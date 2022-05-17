package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    JoinPredicate p;
    DbIterator child1;
    DbIterator child2;
    ArrayList<Tuple> tuples; // this is a joined tuples, which is created when it is opened
    int currIndex;
    TupleDesc td;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        this.tuples = null;
        this.currIndex = 0;
        this.td = null;
    }

    public JoinPredicate getJoinPredicate() {
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        if(td == null) {
            td = TupleDesc.merge(this.child1.getTupleDesc(), this.child2.getTupleDesc());
        }
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        // I need to create a tuple that are joined
        if(this.tuples == null) {
            if(this.p.getOperator() == Predicate.Op.EQUALS) {
                hashJoin();
            } else {
                tupleJoin();
            }

        } else {
            this.currIndex = 0;
        }
    }

    private void hashJoin() throws DbException, NoSuchElementException, TransactionAbortedException {
        // we know that for a fact that it only looks at one field
        this.tuples = new ArrayList<Tuple> ();
        this.currIndex = 0;
        child1.open();
        child2.open();

        HashMap<Field, ArrayList<Tuple>> hashMap = new HashMap<Field, ArrayList<Tuple>> ();
        Tuple next;
        Field field;
        while(child1.hasNext()) {
            next = child1.next();
            field = next.getField(this.p.getField1());
            if(!hashMap.containsKey(field)) {
                hashMap.put(field, new ArrayList<Tuple> ());
            }
            hashMap.get(field).add(next);
        }
        child1.close();

        Tuple tuple2;
        Tuple tuple1;
        Tuple newTuple;
        ArrayList<Tuple> match;
        int j;
        int tuple2NumFields;
        while(child2.hasNext()) {
            tuple2 = child2.next();
            field = tuple2.getField(this.p.getField2());
            if(hashMap.containsKey(field)) {
                match = hashMap.get(field);
                for(int x = 0; x < match.size(); x++) {
                    tuple1 = match.get(x);
                    newTuple = new Tuple(this.getTupleDesc());
                    j = tuple1.getTupleDesc().numFields();
                    tuple2NumFields = tuple2.getTupleDesc().numFields();
                    for(int i = 0; i < j; i++) {
                        newTuple.setField(i, tuple1.getField(i));
                    }
                    for(int i = 0; i < tuple2NumFields; i++) {
                        newTuple.setField(j++, tuple2.getField(i));
                    }
                    this.tuples.add(newTuple);
                }
            }

        }
    }
    private void tupleJoin() throws DbException, NoSuchElementException, TransactionAbortedException {
        this.tuples = new ArrayList<Tuple>();
        this.currIndex = 0;

        child1.open();
        child2.open();

        ArrayList<Tuple> column = new ArrayList<Tuple> ();
        while(child2.hasNext()) {
            column.add(child2.next());
        }
        child2.close();

        Tuple columnTuple;
        Tuple rowTuple;
        Tuple newTuple;
        int j;
        while(child1.hasNext()) {
            rowTuple = child1.next();
            for(int x = 0; x < column.size(); x++) {
                columnTuple = column.get(x);
                if(this.p.filter(rowTuple, columnTuple) == true) {
                    newTuple = new Tuple(this.getTupleDesc());
                    j = rowTuple.getTupleDesc().numFields();
                    for(int i = 0; i < j; i++) {
                        newTuple.setField(i, rowTuple.getField(i));
                    }
                    for(int i = 0; i < columnTuple.getTupleDesc().numFields(); i++) {
                        newTuple.setField(j++, columnTuple.getField(i));
                    }
                    this.tuples.add(newTuple);
                }
            }
        }

        child1.close();
    }
    // this does the block join one block at a time
    /*
    private void blockJoin() throws DbException, NoSuchElementException, TransactionAbortedException {
        HeapPage rowBlock;
        HeapPage colBlock;
        Tuple rowTuple;
        Tuple colTuple;
        Tuple newTuple;
        ArrayList<Tuple> rowList;
        ArrayList<Tuple> colList;
        int j;
        this.tuples = new ArrayList<Tuple>();
        this.currIndex = 0;

        HeapFileIterator hfchild1 = (HeapFileIterator)child1;
        HeapFileIterator hfchild2 = (HeapFileIterator)child2;

        while(hfchild1.hasNextPage()) {
            rowBlock = hfchild1.getNextPage();
            while(hfchild2.hasNextPage()) {
                colBlock = hfchild2.getNextPage();
                rowList = rowBlock.arrayList();
                colList = colBlock.arrayList();
                for(int x = 0; x < rowList.size(); x++) {
                    rowTuple = rowList.get(x);
                    for(int y = 0; y < colList.size(); y++) {
                        colTuple = colList.get(y);
                        if(this.p.filter(rowTuple, colTuple) == true) {
                            newTuple = new Tuple(this.getTupleDesc());
                            j = rowTuple.getTupleDesc().numFields();
                            for(int i = 0; i < j; i++) {
                                newTuple.setField(i, rowTuple.getField(i));
                            }
                            for(int i = 0; i < colTuple.getTupleDesc().numFields(); i++) {
                                newTuple.setField(j++, colTuple.getField(i));
                            }
                            this.tuples.add(newTuple);
                        }
                    }
                }
            }
        }
        this.child1.close();
        this.child2.close();
    }
     */

    public void close() {
        super.close();
        this.currIndex = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(this.currIndex >= this.tuples.size())
            return null;
        Tuple tuple = this.tuples.get(this.currIndex);
        this.currIndex++;
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator [] dbIterator = new DbIterator[2];
        dbIterator[0] = this.child1;
        dbIterator[1] = this.child2;
        return dbIterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if(children.length < 2)
        {
            // don't do anything
        }
        else
        {
            this.child1 = children[0];
            this.child2 = children[1];
        }
    }

}
