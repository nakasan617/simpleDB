package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    TransactionId transactionId;
    DbIterator child;
    boolean calledOnce = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.transactionId = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(this.calledOnce == true) {
            return null;
        }
        // else
        this.calledOnce = true;

        BufferPool bufferPool = Database.getBufferPool();
        int numDeleted = 0;
        Tuple tuple = null;
        while(child.hasNext()) {
            tuple = child.next();
            bufferPool.deleteTuple(this.transactionId, tuple);
            numDeleted++;
        }
        return Utility.getHeapTuple(numDeleted);
        /*
        tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        tuple.setField(0, new IntField(numDeleted));
        return tuple;
        */

    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator [] dbIterator = new DbIterator[1];
        dbIterator[0] = this.child;
        return dbIterator;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        assert children.length > 0;
        this.child = children[0];
    }

}
