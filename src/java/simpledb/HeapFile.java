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
    // this is just a real implementation of DbFileIterator since the class does not seem to have any implementations

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
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek(offset);
            raf.read(data);
            hp = new HeapPage((HeapPageId)pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (Page)hp;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        byte[] data = page.getPageData();
        int offset = page.getId().pageNumber() * BufferPool.PAGE_SIZE;
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
            raf.seek(offset);
            raf.write(data);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        if(t == null)
            throw new DbException("tuple cannot be added");
        ArrayList<Page> dirtyPages = new ArrayList<Page> ();
        HeapPage hp = null;
        HeapPageId pid = null;
        //System.out.println("numPages: " + this.numPages());
        for(int i = 0; i < this.numPages(); i++) {
            pid = new HeapPageId(this.id, i);
            try {
                hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if(hp.getNumEmptySlots() > 0) {
                hp.insertTuple(t);
                dirtyPages.add(hp);
                break;
            } else {
                //System.out.println("there is no more empty slots");
            }
        }

        if(dirtyPages.size() == 0) {
            // you need to create a page here
            pid = new HeapPageId(this.id, this.numPages());
            hp = new HeapPage(pid, HeapPage.createEmptyPageData());
            hp.insertTuple(t);
            dirtyPages.add(hp);
            this.writePage(hp);
        }

        return dirtyPages;
    }

    /**
     * Removes the specifed tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // all you need to do is to get the right page, this is what you need to refer to below 
        // public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        PageId pid = t.getRecordId().getPageId();
        HeapPage hp = null;
        try {
            hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            hp.deleteTuple(t);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return hp;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return (DbFileIterator)(new HeapFileIterator(tid, this.id, this.numPages(), this));
    }

}


