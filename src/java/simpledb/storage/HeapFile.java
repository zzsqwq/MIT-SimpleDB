package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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


    private final File disk_file;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.disk_file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.disk_file;
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
        return this.disk_file.getAbsoluteFile().hashCode();
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
        int tableID = pid.getTableId();
        int pageNum = pid.getPageNumber();
        final int pageSize = Database.getBufferPool().getPageSize();
        byte[] rawPageData = HeapPage.createEmptyPageData();
        Page page = null;

        try {
            RandomAccessFile file = new RandomAccessFile(this.disk_file, "r");
            file.seek(pageNum * pageSize);
            file.read(rawPageData,0,pageSize);
            page = new HeapPage((HeapPageId)pid, rawPageData);
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)this.disk_file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    private class HeapFileIterator implements DbFileIterator {

        int tot_pages_ = numPages();
        int page_cur_ = -1;
        Iterator<Tuple> tuple_iter_;
        TransactionId tid_;

        HeapFileIterator(TransactionId tid) {
            this.tid_ = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            page_cur_++;
            if(tot_pages_ <= 0) {
                throw new NoSuchElementException();
            }
            else {
                tuple_iter_ = ((HeapPage)Database.getBufferPool().getPage(tid_,new HeapPageId(getId(),page_cur_), Permissions.READ_ONLY)).iterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(page_cur_ < 0) {
                return false;
            }
            while(page_cur_ < tot_pages_) {
                if(tuple_iter_.hasNext()) {
                    return true;
                }
                page_cur_++;
                if(page_cur_ >= tot_pages_) {
                    return false;
                }
                tuple_iter_ = ((HeapPage)Database.getBufferPool().getPage(tid_,new HeapPageId(getId(),page_cur_), Permissions.READ_ONLY)).iterator();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(page_cur_ < 0) {
                throw new NoSuchElementException();
            }
            return tuple_iter_.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            page_cur_ = 0;
            tuple_iter_ = ((HeapPage)Database.getBufferPool().getPage(tid_,new HeapPageId(getId(),page_cur_), Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void close() {
            page_cur_ = -1;
            tuple_iter_ = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

