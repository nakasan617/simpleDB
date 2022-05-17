package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    Predicate p;
    ArrayList<DbIterator> children;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // getChildren setChildren are not even tested, just use the first element all the time for now.
        this.p = p;
        this.children = new ArrayList<DbIterator> ();
        this.children.add(child);
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return this.children.get(0).getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        this.children.get(0).open();
    }

    public void close() {
        super.close();
        this.children.get(0).close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.children.get(0).rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        DbIterator child = this.children.get(0);

        if(child.hasNext() == false)
            return null;

        Tuple candidate = child.next();
        while(true) {
            if(this.p.filter(candidate) == true)
                return candidate;
            else if(child.hasNext()) {
                candidate = child.next();
            } else {
                return null;
            }
        }

    }

    @Override
    public DbIterator[] getChildren() {
        // this is not even tested...
        return (DbIterator [])this.children.toArray();
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // this is not even tested...
        this.children = new ArrayList<DbIterator> ();
        for(int i = 0; i < children.length; i++) {
            this.children.add(children[i]);
        }
    }

}

