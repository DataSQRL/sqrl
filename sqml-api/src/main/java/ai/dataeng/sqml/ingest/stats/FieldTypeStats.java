package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.accumulator.LogarithmicHistogram;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import lombok.*;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class FieldTypeStats implements Serializable, Cloneable {

    FieldTypeStats.TypeDepth raw;
    FieldTypeStats.TypeDepth detected;

    long count;
    //Only not-null if detected is an array
    LogarithmicHistogram.Accumulator arrayCardinality;
    //Only not-null if detected is a NestedRelation
    RelationStats nestedRelationStats;

    public FieldTypeStats() {} //For Kryo;

    public FieldTypeStats(@NonNull FieldTypeStats.TypeDepth raw) {
        this(raw,raw);
    }

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

    public boolean hasDetected() {
        return !raw.equals(detected);
    }

    public void addNested(@NonNull Map<String,Object> nested, @NonNull NameCanonicalizer canonicalizer) {
        if (nestedRelationStats==null) nestedRelationStats=new RelationStats();
        nestedRelationStats.add(nested, canonicalizer);
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

    public void merge(@NonNull FieldTypeStats other) {
        assert equals(other);
        count+=other.count;
        if (other.arrayCardinality!=null) {
            if (arrayCardinality==null) arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
            arrayCardinality.merge(other.arrayCardinality);
        }
        if (other.nestedRelationStats !=null) {
            if (nestedRelationStats ==null) nestedRelationStats = new RelationStats();
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

    @Override
    public String toString() {
        String result = "{" + raw.toString();
        if (!raw.equals(detected)) {
            result+= "|" + detected.toString();
        }
        result += "}";
        return result;
    }

    public interface TypeDepth {

        boolean isBasic();

        BasicType getBasicType();

        default Type getType() {
            if (isBasic()) return getBasicType();
            else return RelationType.EMPTY;
        }

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

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class BasicTypeDepth implements TypeDepth {

        int arrayDepth;
        BasicType basicType;

        private BasicTypeDepth() {} //For Kryo

        @Override
        public boolean isBasic() {
            return true;
        }
    }

    @EqualsAndHashCode
    @ToString
    public static class NestedRelation implements TypeDepth {

        static final NestedRelation INSTANCE = new NestedRelation();

        private NestedRelation() {} //For Kryo


        @Override
        public boolean isBasic() {
            return false;
        }

        @Override
        public BasicType getBasicType() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getArrayDepth() {
            return 1;
        }
    }


}


