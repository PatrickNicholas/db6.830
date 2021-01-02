package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId txnId;
    private OpIterator child;
    private int tableId;
    private boolean finished;
    private TupleDesc desc;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.txnId = t;
        this.child = child;
        this.tableId = tableId;
        Type[] types = new Type[]{Type.INT_TYPE};
        this.desc = new TupleDesc(types);
    }

    public TupleDesc getTupleDesc() {
        return this.desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.finished = false;
        super.open();
    }

    public void close() {
        this.finished = true;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (finished) {
            return null;
        }

        finished = true;
        int affectRows = 0;

        child.open();
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(txnId, tableId, t);
                affectRows++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        child.close();

        Field[] fields = new Field[]{new IntField(affectRows)};
        return new Tuple(desc, fields);
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        assert children.length == 1;
        child = children[0];
    }
}
