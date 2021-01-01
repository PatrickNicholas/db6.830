package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        super();
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.child.open();
        super.open();
    }

    public void close() {
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
        super.close();
        super.open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!this.child.hasNext()) {
            return null;
        }
        Tuple tuple = this.child.next();
        while (!predicate.filter(tuple)) {
            if (this.child.hasNext()) {
                tuple = this.child.next();
                continue;
            }
            return null;
        }
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] its = new OpIterator[1];
        its[0] = this.child;
        return its;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        assert children.length == 1;
        this.child = children[0];
    }

}
