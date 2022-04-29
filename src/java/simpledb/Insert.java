package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId transactionId;
    DbIterator dbIterator;
    int tableId;
    boolean calledOnce;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.transactionId = t;
        this.dbIterator = child;
        this.tableId = tableid;
        this.calledOnce = false;
    }

    public TupleDesc getTupleDesc() {
//        return Database.getCatalog().getTupleDesc(this.tableId);
        return this.dbIterator.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        dbIterator.open();
    }

    public void close() {
        super.close();
        dbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        dbIterator.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(this.calledOnce == true)
            return null;
        // else
        this.calledOnce = true;
        int numInserted = 0;
        Tuple next = null;
        BufferPool bufferPool = Database.getBufferPool();

        try {
            while (dbIterator.hasNext()) {
                next = dbIterator.next();
                bufferPool.insertTuple(this.transactionId, this.tableId, next);
                numInserted++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        // you need to create the returning tuple
        return Utility.getHeapTuple(numInserted);
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator [] iterators = new DbIterator[1];
        iterators[0] = this.dbIterator;
        return iterators;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.dbIterator = children[0];
    }
}
