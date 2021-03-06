package simpledb;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private TupleDesc desc;
    private JoinPredicate predicate;
    private OpIterator left;
    private OpIterator right;
    private Tuple leftTuple, rightTuple;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p      The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.desc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        this.predicate = p;
        this.left = child1;
        this.right = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return this.predicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by
     * alias or table name.
     */
    public String getJoinField1Name() {
        int f1 = this.predicate.getField1();

        String leftName = this.left.getTupleDesc().getFieldName(f1);
        return String.join(".", leftName);
    }

    /**
     * @return the field name of join field2. Should be quantified by
     * alias or table name.
     */
    public String getJoinField2Name() {
        int f2 = this.predicate.getField2();
        String rightName = this.right.getTupleDesc().getFieldName(f2);
        return String.join(".", rightName);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     * implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return desc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.left.open();
        this.right.open();
        leftTuple = null;
        rightTuple = null;
        super.open();
    }

    public void close() {
        this.left.close();
        this.right.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
        this.left.rewind();
        this.right.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while (true) {
            if (leftTuple == null) {
                if (!this.left.hasNext()) {
                    // left finished.
                    return null;
                }
                leftTuple = this.left.next();
            }
            if (rightTuple == null) {
                if (!this.right.hasNext()) {
                    this.right.rewind();

                    // fast exit if right is empty.
                    if (!this.right.hasNext()) {
                        return null;
                    }

                    // Start next round loop: switch left to right.
                    leftTuple = null;
                    continue;
                }
                rightTuple = this.right.next();
            }

            Tuple t = rightTuple;
            rightTuple = null;
            if (this.predicate.filter(leftTuple, t)) {
                Field[] fields = Field.concat(leftTuple.fields(), t.fields())
                        .toArray(Field[]::new);
                return new Tuple(desc, fields);
            }
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return Arrays.asList(this.left, this.right).toArray(OpIterator[]::new);
    }

    @Override
    public void setChildren(OpIterator[] children) {
        assert children.length == 2;
        this.left = children[0];
        this.right = children[1];
    }

}
