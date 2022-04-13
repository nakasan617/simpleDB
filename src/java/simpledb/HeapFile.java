package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc td;
    int id;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td; 
        this.id = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
        byte data [] = new byte [BufferPool.PAGE_SIZE];
        HeapPage hp = null;
        try
        {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek(offset);
            raf.read(data);
            hp = new HeapPage((HeapPageId)pid, data);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return (Page)hp;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int numPages;
        if(this.file.length() % BufferPool.PAGE_SIZE == 0)
        {
            numPages = ((int)(this.file.length()))/BufferPool.PAGE_SIZE;
        }
        else
        {
            numPages = ((int)this.file.length())/BufferPool.PAGE_SIZE + 1;
        }

        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        return new HeapFileIterator(tid, this.id, this.numPages(), this);
    }
    
    public class HeapFileIterator implements DbFileIterator 
    {
        TransactionId tid;
        int tableId;
        int numPages;       
        int currPageNo;
        HeapFile hf;
        Iterator<Tuple> tuples;

        HeapFileIterator(TransactionId tid, int tableId, int numPages, HeapFile hf)
        {
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
        public boolean hasNext()
            throws DbException, TransactionAbortedException
        {
            if(tuples == null)
            {
                return false;
            } 

            if(!tuples.hasNext() && this.currPageNo + 1 == this.numPages)
            {
                return false;
            }
            else
            {
                return true;
            }
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
            {
                throw new NoSuchElementException();
            }

            if(tuples.hasNext())
            {
                return tuples.next();
            }
            else
            {
                this.currPageNo++;
                
                HeapPageId pid = new HeapPageId(this.tableId, this.currPageNo);

                HeapPage pg = null;
                try {
                    pg = (HeapPage) (Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                tuples = pg.iterator();

                if(!tuples.hasNext())
                {
                    return null;
                }
                else
                {
                    return tuples.next();
                }
            }
            
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException When rewind is unsupported.
         */
        public void rewind() throws DbException, TransactionAbortedException
        {
            this.currPageNo = 0;
            HeapPageId pid = new HeapPageId(this.tableId, this.currPageNo);
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


}


