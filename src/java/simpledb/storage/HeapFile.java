package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc schema;
    private final int tableId;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.schema = td;
        this.tableId = f.getAbsoluteFile().hashCode();
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
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        final int pageSize = BufferPool.getPageSize();
        final int offset  = pid.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];

        try {
            RandomAccessFile input = new RandomAccessFile(this.file, "r");
            input.skipBytes(offset);
            input.read(data);
            input.close();
            return new HeapPage( (HeapPageId) pid, data);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile output = new RandomAccessFile(file, "rw");
        output.skipBytes(page.getId().getPageNumber() * BufferPool.getPageSize());
        output.write(page.getPageData());
        output.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        BufferPool buf = Database.getBufferPool();
        HeapPage dirtyPage = null;
        // find page that hs empty slot
        for(int pageNo = 0; pageNo < this.numPages(); pageNo++){
            PageId pid = new HeapPageId(this.tableId, pageNo);
            HeapPage page =(HeapPage) buf.getPage(tid, pid, Permissions.READ_WRITE);
                                                  
            if(page.getNumEmptySlots() > 0){
                dirtyPage = page;
                break;
            }
            // release the writer lock, because we don't modify this page
            else buf.unsafeReleasePage(tid, pid);
        }

        if(dirtyPage == null){
            // create a new page
            FileOutputStream output = new FileOutputStream(file, true);
            output.write(HeapPage.createEmptyPageData());
            output.close();

            dirtyPage = (HeapPage) buf.getPage(tid, 
                                               new HeapPageId(tableId, this.numPages()-1), 
                                               Permissions.READ_WRITE);
        }

        dirtyPage.insertTuple(t);
        dirtyPage.markDirty(true, tid);

        List<Page> modified = new ArrayList<Page>();
        modified.add(dirtyPage);
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        page.deleteTuple(t);
        page.markDirty(true, tid);

        ArrayList<Page> modified = new ArrayList<Page>();
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator(){
            private final int pagesPerFile = numPages();
            private final BufferPool buf = Database.getBufferPool();
            private Iterator<Tuple> tupleIter = null;
            /** page number of current opening heap page */
            private int pgNo = 0; 

            @Override
            public void open() throws DbException, TransactionAbortedException {
                PageId pid = new HeapPageId(tableId, this.pgNo);
                HeapPage page = (HeapPage) this.buf.getPage(tid, pid, Permissions.READ_ONLY);

                if(page == null){
                    this.tupleIter = null;
                    throw new DbException("Error: Cannot get page from buffer");
                }
                else{
                    this.tupleIter = page.iterator();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(this.tupleIter == null) return false;
                if(this.tupleIter.hasNext()) return true;
                
                // test whether current page has next
                // if not, open next page
                while(!this.tupleIter.hasNext()){
                    this.pgNo ++;
                    if(this.pgNo >= this.pagesPerFile) return false;
                    this.open();
                }
                
                return true;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {                
                if(this.tupleIter == null){
                    throw new NoSuchElementException();
                }
                return this.tupleIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // reset the page number
                this.pgNo = 0;          
                // reopen the iterator
                this.open();      
            }

            @Override
            public void close() {
                this.pgNo = 0;
                this.tupleIter = null;                
            }
        };
    }
}

