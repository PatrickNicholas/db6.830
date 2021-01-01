package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static class Group {
        int sum = 0;
        int count = 0;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;

        void aggragete(IntField field) {
            sum += field.getValue();
            count++;
            max = Math.max(max, field.getValue());
            min = Math.min(min, field.getValue());
        }

        double avg() {
            assert count != 0;
            return (double) sum / count;
        }
    }

    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;

    private HashMap<Field, Group> groupValues;
    private Group singleValue;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupValues = new HashMap<>();
        this.singleValue = new Group();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Group group;
        if (gbfield == NO_GROUPING) {
            group = singleValue;
        } else {
            Field groupField = tup.getField(gbfield);
            group = groupValues.get(groupField);
            if (group == null) {
                group = new Group();
                groupValues.put(groupField, group);
            }
        }
        group.aggragete((IntField) tup.getField(afield));
    }

    private class AggOpIterator implements OpIterator {
        private TupleDesc desc;
        private boolean open;
        private Iterator<Field> it;

        public AggOpIterator() {
            ArrayList<Type> types = new ArrayList<>();
            if (gbfield != NO_GROUPING) {
                types.add(gbfieldtype);
                it = groupValues.keySet().iterator();
            }
            if (what == Op.SUM_COUNT) {
                types.add(Type.INT_TYPE);
                types.add(Type.INT_TYPE);
            } else {
                // FIXME(patrick) add SC_COUNT
                types.add(Type.INT_TYPE);
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

            if (gbfield != NO_GROUPING) {
                return it.hasNext();
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!open) {
                throw new NoSuchElementException();
            }

            Tuple tuple = new Tuple(desc);
            Group group;
            int idx = 0;
            if (gbfield == NO_GROUPING) {
                open = false;
                group = singleValue;
            } else {
                Field field = it.next();
                group = groupValues.get(field);
                tuple.setField(idx, field);
                idx++;
            }

            switch (what) {
                case AVG:
                    tuple.setField(idx, new IntField((int) group.avg()));
                    break;
                case MAX:
                    tuple.setField(idx, new IntField(group.max));
                    break;
                case MIN:
                    tuple.setField(idx, new IntField(group.min));
                    break;
                case SUM:
                    tuple.setField(idx, new IntField(group.sum));
                    break;
                case COUNT:
                    tuple.setField(idx, new IntField(group.count));
                    break;
                case SUM_COUNT:
                    tuple.setField(idx, new IntField(group.count));
                    break;
                case SC_AVG:
                    tuple.setField(idx, new IntField(group.count));
                    break;
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open = true;
            it = groupValues.keySet().iterator();
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        return new AggOpIterator();
    }

}
