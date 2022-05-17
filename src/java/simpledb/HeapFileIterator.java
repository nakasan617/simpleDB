package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    TransactionId tid; // I don't really know why this is nessesary yet, but it is in the argument so...
    int tableId;
    int numPages;
    int currPageNo;
    HeapFile hf;
    Iterator<Tuple> tuples;
    HeapPageId pid;
    // above is just a tuples of the current page,
    // so you have to renew it everytime you open another page

    HeapFileIterator(TransactionId tid, int tableId, int numPages, HeapFile hf) {
        this.tid = tid;
        this.tableId = tableId;
        this.numPages = numPages;
        this.currPageNo = 0;
        this.hf = hf;
        this.tuples = null;
    }
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
            throws DbException, TransactionAbortedException
    {
        this.currPageNo = 0;
        HeapPageId pid = new HeapPageId(this.tableId, this.currPageNo);
        HeapPage pg = null;
        try {
            pg = (HeapPage) (Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        this.tuples = pg.iterator();
    }

    /** @return true if there are more tuples available. */
    // you have to check for another page too if its not there
    // ill return false if it isn't opened yet
    public boolean hasNext()
            throws DbException, TransactionAbortedException
    {
        if(tuples == null) {
            return false;
        }
        if(tuples.hasNext()) {
            return true;
        }

        if(this.currPageNo + 1 < this.numPages) {
            this.currPageNo++;
            this.pid = new HeapPageId(this.hf.getId(), this.currPageNo);
            try {
                this.tuples = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return this.tuples.hasNext();
        }
        return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
            throws DbException, TransactionAbortedException, NoSuchElementException
    {
        if(!this.hasNext())
            throw new NoSuchElementException("there is no next tuple");
        return this.tuples.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException
    {
        this.currPageNo = 0;
        HeapPageId pid = new HeapPageId(this.tableId, this.currPageNo);
        // creating random page with a page number 0
        HeapPage pg = (HeapPage)this.hf.readPage((PageId)pid);
        this.tuples = pg.iterator();

    }

    /**
     * Closes the iterator.
     */
    public void close()
    {
        this.tuples = null;
        this.currPageNo = 0;
    }

}
