package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private HashMap<Field, Integer> groupCount;
    private Integer singleCount;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.groupCount = new HashMap<>();
        this.singleCount = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbfield == NO_GROUPING) {
            singleCount++;
        } else {
            Field field = tup.getField(gbfield);
            Integer cnt = groupCount.getOrDefault(field, 0);
            cnt++;
            groupCount.put(field, cnt);
        }
    }

    private class AggOpIterator implements OpIterator {
        private boolean open;
        private TupleDesc desc;
        private Iterator<Field> it;

        public AggOpIterator() {
            this.open = false;
            ArrayList<Type> types = new ArrayList<>();
            if (gbfield == NO_GROUPING) {
                types.add(Type.INT_TYPE);
            } else {
                types.add(Type.INT_TYPE);
                types.add(Type.INT_TYPE);
                it = groupCount.keySet().iterator();
            }
            this.desc = new TupleDesc(types.toArray(Type[]::new));
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            open = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!open) {
                return false;
            }
            if (gbfield == NO_GROUPING) {
                return true;
            }
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (gbfield == NO_GROUPING) {
                open = false;
                Field[] fields = new Field[1];
                fields[0] = new IntField(singleCount);
                return new Tuple(desc, fields);
            } else {
                Field key = it.next();
                Integer value = groupCount.get(key);
                Field[] fields = new Field[2];
                fields[0] = key;
                fields[1] = new IntField(value);
                return new Tuple(desc, fields);
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open = true;
            it = groupCount.keySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return desc;
        }

        @Override
        public void close() {
            open = false;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new AggOpIterator();
    }

}
