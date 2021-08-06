package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.sketches.LogarithmicHistogram;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.TypeMapping;
import lombok.ToString;
import lombok.Value;
import org.apache.flink.api.common.accumulators.LongCounter;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Value
public class FieldStats implements Serializable {

    long count;
    long numNulls;
    Map<SqmlType.ScalarSqmlType, Long> presentedScalarTypes;
    Map<SqmlType.ScalarSqmlType, Long> inferredScalarTypes;
    LogarithmicHistogram arrayCardinality;
    RelationStats nestedRelationStats;

    @ToString
    public static class Accumulator implements org.apache.flink.api.common.accumulators.Accumulator<Object, FieldStats> {

        public static final int ARRAY_CARDINALITY_BASE = 4;
        public static final int ARRAY_CARDINALITY_BUCKETS = 8;

        long count;
        long numNulls;
        Map<SqmlType.ScalarSqmlType, LongCounter> presentedScalarTypes;
        Map<SqmlType.ScalarSqmlType, LongCounter> inferredScalarTypes;

        //Only needed if value is an array
        LogarithmicHistogram.Accumulator arrayCardinality;
        //Only needed if value is a nested relation
        RelationStats.Accumulator relationAccum;


        @Override
        public void add(Object o) throws IllegalArgumentException {
            count++;
            if (o==null) {
                numNulls++;
            } else {
                if (o instanceof Collection || o.getClass().isArray()) {
                    Iterator<Object> elements;
                    int numElements;
                    if (o instanceof Collection) {
                        Collection col = (Collection)o;
                        elements = col.iterator();
                        numElements = col.size();
                    } else { //is an array
                        Object[] arr = (Object[])o;
                        elements = Arrays.asList(arr).iterator();
                        numElements = arr.length;
                    }
                    //Data type for all elements in list
                    SqmlType type = null;
                    SqmlType.ScalarSqmlType inferredType = null;
                    while (elements.hasNext()) {
                        Object next = elements.next();
                        if (next instanceof Map) {
                            addNested((Map)next);
                            if (type == null) {
                                type = SqmlType.RelationSqmlType.INSTANCE;
                            } else if (!(type instanceof SqmlType.RelationSqmlType)) {
                                throw new IllegalArgumentException(String.format("Elements with incompatible types in same list: %s", o));
                            }
                        } else {
                            //not an array or map => must be scalar, let's find the common scalar type for all elements
                            SqmlType.ScalarSqmlType singleType = TypeMapping.scalar2Sqml(o);
                            if (type == null) {
                                type = singleType;
                            } else if (!(type.getClass().isInstance(singleType))) {
                                throw new IllegalArgumentException(String.format("Elements with incompatible types in same list: %s", o));
                            }
                            SqmlType.ScalarSqmlType inferredSingleType = TypeMapping.inferType(singleType, next);
                            //Need to be careful with how we infer types for lists - only infer if all elements agree
                            if (inferredType == null) {
                                inferredType = inferredSingleType;
                            } else if (!inferredSingleType.equals(inferredType)) {
                                inferredType = (SqmlType.ScalarSqmlType) type;
                            }
                        }
                    }
                    if (type instanceof SqmlType.ScalarSqmlType) incPresentedType((SqmlType.ScalarSqmlType) type);
                    if (inferredType!=null && !inferredType.equals(type)) incInferredType(inferredType);
                    addArrayCardinality(numElements);
                } else {
                    //Single element
                    if (o instanceof Map) {
                        addNested((Map)o);
                    } else {
                        //not an array or map => must be scalar
                        SqmlType.ScalarSqmlType type = TypeMapping.scalar2Sqml(o);
                        incPresentedType(type);
                        SqmlType.ScalarSqmlType inferredType = TypeMapping.inferType(type, o);
                        if (!inferredType.equals(type)) {
                            incInferredType(inferredType);
                        }
                    }
                }
            }
        }

        /*
        We use deferred initialization of the data structures below to save space.
         */

        private void incPresentedType(SqmlType.ScalarSqmlType type) {
            if (presentedScalarTypes==null) presentedScalarTypes=new HashMap<>(4);
            incType(type, presentedScalarTypes);
        }

        private void incInferredType(SqmlType.ScalarSqmlType type) {
            if (inferredScalarTypes==null) inferredScalarTypes=new HashMap<>(4);
            incType(type, inferredScalarTypes);
        }

        private static void incType(SqmlType.ScalarSqmlType type, Map<SqmlType.ScalarSqmlType, LongCounter> countMap) {
            LongCounter count = countMap.get(type);
            if (count == null) {
                count = new LongCounter(0);
                countMap.put(type,count);
            }
            count.add(1);
        }

        private void addArrayCardinality(int numElements) {
            if (arrayCardinality==null) arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
            arrayCardinality.add(numElements);
        }

        private void addNested(Map<String,Object> nested) {
            if (relationAccum==null) relationAccum = new RelationStats.Accumulator();
            relationAccum.add(nested);
        }

        private static Map<SqmlType.ScalarSqmlType, Long> convert(Map<SqmlType.ScalarSqmlType, LongCounter> typeCounter) {
            if (typeCounter==null) return Collections.EMPTY_MAP;
            return typeCounter.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getLocalValue()));
        }

        @Override
        public FieldStats getLocalValue() {
            return new FieldStats(count,numNulls, convert(presentedScalarTypes), convert(inferredScalarTypes),
                    (arrayCardinality==null?LogarithmicHistogram.EMPTY:arrayCardinality.getLocalValue()),
                    (relationAccum==null?RelationStats.EMPTY:relationAccum.getLocalValue()));
        }

        @Override
        public void resetLocal() {
            count = 0;
            numNulls = 0;
            presentedScalarTypes = null;
            inferredScalarTypes = null;
            arrayCardinality = null;
            relationAccum = null;
        }

        @Override
        public void merge(org.apache.flink.api.common.accumulators.Accumulator<Object, FieldStats> accumulator) {
            Accumulator acc = (Accumulator) accumulator;
            count+=acc.count;
            numNulls+=acc.numNulls;
            if (acc.presentedScalarTypes!=null) {
                if (presentedScalarTypes==null) presentedScalarTypes = new HashMap<>(acc.presentedScalarTypes.size());
                mergeCounterMaps(presentedScalarTypes, acc.presentedScalarTypes);
            }
            if (acc.inferredScalarTypes!=null) {
                if (inferredScalarTypes==null) inferredScalarTypes = new HashMap<>(acc.inferredScalarTypes.size());
                mergeCounterMaps(inferredScalarTypes, acc.inferredScalarTypes);
            }
            if (acc.arrayCardinality!=null) {
                if (arrayCardinality==null) arrayCardinality = new LogarithmicHistogram.Accumulator(ARRAY_CARDINALITY_BASE, ARRAY_CARDINALITY_BUCKETS);
                arrayCardinality.merge(acc.arrayCardinality);
            }
            if (acc.relationAccum!=null) {
                if (relationAccum==null) relationAccum = new RelationStats.Accumulator();
                relationAccum.merge(acc.relationAccum);
            }
        }

        private static void mergeCounterMaps(Map<SqmlType.ScalarSqmlType, LongCounter> to, Map<SqmlType.ScalarSqmlType, LongCounter> from) {
            from.forEach((k,v) -> {
               LongCounter counter = to.get(k);
               if (counter==null) {
                   counter = new LongCounter(0);
                   to.put(k,counter);
               }
               counter.merge(v);
            });
        }


        @Override
        public Accumulator clone() {
            Accumulator newAcc = new Accumulator();
            newAcc.count = count;
            newAcc.numNulls = numNulls;
            if (presentedScalarTypes!=null) newAcc.presentedScalarTypes = copyCounter(presentedScalarTypes);
            if (inferredScalarTypes!=null) newAcc.inferredScalarTypes = copyCounter(inferredScalarTypes);
            if (arrayCardinality!=null) newAcc.arrayCardinality = arrayCardinality.clone();
            if (relationAccum!=null) newAcc.relationAccum = relationAccum.clone();
            return newAcc;
        }

        private static Map<SqmlType.ScalarSqmlType, LongCounter> copyCounter(Map<SqmlType.ScalarSqmlType, LongCounter> counter) {
            Map<SqmlType.ScalarSqmlType, LongCounter> newMap = new HashMap<>(counter.size());
            counter.forEach((k,v) -> newMap.put(k,v.clone()));
            return newMap;
        }

    }




}
