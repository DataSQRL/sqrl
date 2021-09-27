package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.accumulator.LogarithmicHistogram;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import lombok.NonNull;
import lombok.Value;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class FieldTypeStats implements Serializable, Cloneable {

    final @NonNull FieldTypeStats.TypeDepth raw;
    final @NonNull FieldTypeStats.TypeDepth detected;

    long count;
    //Only not-null if detected is an array
    LogarithmicHistogram.Accumulator arrayCardinality;
    //Only not-null if detected is a NestedRelation
    RelationStats nestedRelationStats;

    public FieldTypeStats(@NonNull FieldTypeStats.TypeDepth raw, @NonNull FieldTypeStats.TypeDepth detected) {
        this.raw = raw;
        this.detected = detected;
        this.count = 0;
    }

    public FieldTypeStats(FieldTypeStats other) {
        this(other.raw,other.detected);
    }

    public static FieldTypeStats of(@NonNull Type raw, BasicType detected,
                          int arrayDepth) {
        FieldTypeStats.TypeDepth r,d;
        r = TypeDepth.of(raw,arrayDepth);
        if (detected!=null) d = TypeDepth.of(detected,arrayDepth);
        else d = r;
        return new FieldTypeStats(r,d);
    }

    public static FieldTypeStats of(@NonNull Type raw, BasicType detected) {
        return of(raw,detected,0);
    }

    public void add() {
        count++;
    }

    public void addNested(@NonNull Map<String,Object> nested, @NonNull NameCanonicalizer canonicalizer) {
        if (nestedRelationStats==null) nestedRelationStats=new RelationStats(canonicalizer);
        nestedRelationStats.add(nested);
    }

    public void add(int numArrayElements, RelationStats relationStats) {
        count++;
        addArrayCardinality(numArrayElements);
        if (relationStats!=null) {
            assert raw instanceof NestedRelation;
            if (nestedRelationStats==null) nestedRelationStats = relationStats;
            else nestedRelationStats.merge(relationStats);
        }
    }

    public static final int ARRAY_CARDINALITY_BASE = 4;
    public static final int ARRAY_CARDINALITY_BUCKETS = 8;

    private void addArrayCardinality(int numElements) {
        if (arrayCardinality==null) arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
        arrayCardinality.add(numElements);
    }

    public void merge(@NonNull FieldTypeStats other, @NonNull NameCanonicalizer canonicalizer) {
        assert equals(other);
        count+=other.count;
        if (other.arrayCardinality!=null) {
            if (arrayCardinality==null) arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
            arrayCardinality.merge(other.arrayCardinality);
        }
        if (other.nestedRelationStats !=null) {
            if (nestedRelationStats ==null) nestedRelationStats = new RelationStats(canonicalizer);
            nestedRelationStats.merge(other.nestedRelationStats);
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldTypeStats that = (FieldTypeStats) o;
        return raw.equals(that.raw) && detected.equals(that.detected);
    }

    @Override
    public int hashCode() {
        return Objects.hash(raw, detected);
    }

    public interface TypeDepth {

        boolean isBasic();

        BasicTypeDepth getType();

        int getArrayDepth();

        default boolean isArray() {
            return getArrayDepth()>0;
        }

        public static TypeDepth of(@NonNull Type type, int arrayDepth) {
            if (type instanceof RelationType) return NestedRelation.INSTANCE;
            else if (type instanceof BasicType) return new BasicTypeDepth(arrayDepth, (BasicType) type);
            else throw new IllegalArgumentException("Unsupported type: " + type);
        }

    }

    @Value
    public static class BasicTypeDepth implements TypeDepth {

        int arrayDepth;
        BasicType type;

        @Override
        public boolean isBasic() {
            return true;
        }
    }

    @Value
    public static class NestedRelation implements TypeDepth {

        static final NestedRelation INSTANCE = new NestedRelation();

        private NestedRelation() {}

        @Override
        public boolean isBasic() {
            return false;
        }

        @Override
        public BasicTypeDepth getType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getArrayDepth() {
            return 1;
        }
    }


}


