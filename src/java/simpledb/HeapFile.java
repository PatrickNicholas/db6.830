package simpledb;

import java.awt.*;
import java.io.*;
import java.nio.Buffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc typeDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.typeDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return this.file.getAbsoluteFile().hashCode() * 31 +
                this.file.getName().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.typeDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNumber = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pageNumber;
        try {
            RandomAccessFile file = new RandomAccessFile(this.file, "r");
            file.seek(offset);
            byte[] bytes = new byte[pageSize];
            int nread = file.read(bytes, 0, pageSize);
            if (nread != pageSize) {
                throw new IllegalArgumentException();
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pageNumber);
            return new HeapPage(id, bytes);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
        int pageNumber = pageId.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        int offset = pageSize * pageNumber;
        try {
            RandomAccessFile file = new RandomAccessFile(this.file, "rw");
            file.seek(offset);
            byte[] data = page.getPageData();
            file.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        long size = this.file.length();
        int pageSize = BufferPool.getPageSize();
        return (int) (size / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int pages = this.numPages();
        for (int i = 0; i < pages; ++i) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool()
                    .getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return Stream.of(page).collect(Collectors.toCollection(ArrayList::new));
            }
        }

        // need write page.
        byte[] data = HeapPage.createEmptyPageData();
        HeapPageId pageId = new HeapPageId(getId(), pages);
        HeapPage page = new HeapPage(pageId, data);
        page.insertTuple(t);
        writePage(page);
        return Stream.of(page).collect(Collectors.toCollection(ArrayList::new));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Stream.of(page).collect(Collectors.toCollection(ArrayList::new));
    }

    private class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        private int currentPageNumber;
        private Iterator<Tuple> tupleIterator;
        private BufferPool pool;
        private boolean open;

        public HeapFileIterator(TransactionId id) {
            this.tid = id;
            this.open = false;
            this.currentPageNumber = -1;
            this.tupleIterator = null;
            this.pool = Database.getBufferPool();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
            switchToNextPage();
        }

        private void switchToNextPage() throws TransactionAbortedException, DbException {
            this.tupleIterator = null;
            this.currentPageNumber++;
            if (currentPageNumber >= HeapFile.this.numPages()) {
                return;
            }
            int tableId = HeapFile.this.getId();
            HeapPageId pageId = new HeapPageId(tableId, this.currentPageNumber);
            HeapPage page = (HeapPage) this.pool.getPage(this.tid, pageId, Permissions.READ_ONLY);
            this.tupleIterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!open) {
                return false;
            }
            if (this.currentPageNumber >= HeapFile.this.numPages()) {
                return false;
            }
            if (this.tupleIterator != null && this.tupleIterator.hasNext()) {
                return true;
            }
            switchToNextPage();
            return hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return this.tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.currentPageNumber = -1;
            this.tupleIterator = null;
            open();
        }

        @Override
        public void close() {
            open = false;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

}

