package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.ingest.sketches.LogarithmicHistogram;
import ai.dataeng.sqml.type.ArrayType;
import ai.dataeng.sqml.type.ScalarType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.TypeMapping;
import com.google.common.base.Preconditions;
import lombok.ToString;
import lombok.Value;
import org.apache.calcite.interpreter.Scalar;
import org.apache.flink.api.common.accumulators.LongCounter;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Value
public class FieldStats implements Serializable {

    long count;
    long numNulls;
    Map<ScalarType, Long> presentedScalarTypes;
    Map<ScalarType, Long> inferredScalarTypes;
    LogarithmicHistogram arrayCardinality;
    RelationStats nestedRelationStats;

    public Type resolveType(NamePath name) {
        boolean isArray = arrayCardinality.getCount() > 0;

        Type type = null;
        if (nestedRelationStats.getCount()>0) {
            Preconditions.checkArgument(presentedScalarTypes.isEmpty() && inferredScalarTypes.isEmpty(),
                    "Cannot mix scalar values and nested relations");
            type = RelationType.INSTANCE;
        } else {
            //Must be a scalar - for now we expect scalars to resolve to one type that can encompass all values.
            //TODO: Generalize to allow for multiple incompatible types (e.g. "STRING | INT")
            long nonNull = count - numNulls;

            //We only use the inferred types if they cover all of the seen (non-null) values:
            long total = 0;
            for (Map.Entry<ScalarType, Long> infer : inferredScalarTypes.entrySet()) {
                if (type == null) type = infer.getKey();
                else {
                    type = type.combine(infer.getKey());
                    if (type == null) break; //for now, we don't allow dual types, so abandon type inference
                }
                total += infer.getValue();
            }
            assert total <= nonNull;

            //If we cannot unambiguously infer a type, take the presented type
            if (type == null || total < nonNull) {
                for (Map.Entry<ScalarType, Long> infer : presentedScalarTypes.entrySet()) {
                    Type newtype = type==null?infer.getKey():type.combine(infer.getKey());
                    Preconditions.checkArgument(newtype!=null,"Incompatible types detected in input data: %s vs %s", type, infer.getKey());
                    type = newtype;
                }
            }
            if (type == null) {
                if (nonNull==0) throw new IllegalArgumentException("Null-type are not supported yet");
                else throw new IllegalArgumentException("Missing type in input data");
            }
        }

        if (isArray) return new ArrayType(type);
        else return type;
    }

    public void collectSchema(SourceTableSchema.Builder builder, String fieldName) {
        boolean isArray = arrayCardinality.getCount() > 0;
        boolean notNull = numNulls==0;


        if (nestedRelationStats.getCount()>0) {
            Preconditions.checkArgument(presentedScalarTypes.isEmpty() && inferredScalarTypes.isEmpty(),
                    "Cannot mix scalar values and nested relations");
            SourceTableSchema.Builder nestedBuilder = builder.addNestedTable(fieldName, isArray, notNull);
            nestedRelationStats.collectSchema(nestedBuilder);
        } else {
            //Must be a scalar - for now we expect scalars to resolve to one type that can encompass all values.
            //TODO: Generalize to allow for multiple incompatible types (e.g. "STRING | INT")
            ScalarType type = null;
            long nonNull = count - numNulls;

            //We only use the inferred types if they cover all of the seen (non-null) values:
            long total = 0;
            for (Map.Entry<ScalarType, Long> infer : inferredScalarTypes.entrySet()) {
                if (type == null) type = infer.getKey();
                else {
                    type = (ScalarType) type.combine(infer.getKey());
                    if (type == null) break; //for now, we don't allow dual types, so abandon type inference
                }
                total += infer.getValue();
            }
            assert total <= nonNull;

            //If we cannot unambiguously infer a type, take the presented type
            if (type == null || total < nonNull) {
                for (Map.Entry<ScalarType, Long> infer : presentedScalarTypes.entrySet()) {
                    ScalarType newtype = type==null?infer.getKey():(ScalarType)type.combine(infer.getKey());
                    Preconditions.checkArgument(newtype!=null,"Incompatible types detected in input data: %s vs %s", type, infer.getKey());
                    type = newtype;
                }
            }
            if (type == null) {
                if (nonNull==0) throw new IllegalArgumentException("Null-type are not supported yet");
                else throw new IllegalArgumentException("Missing type in input data");
            }
            builder.addField(fieldName, isArray, notNull, type);
        }
    }

    @ToString
    public static class Accumulator implements org.apache.flink.api.common.accumulators.Accumulator<Object, FieldStats> {

        public static final int ARRAY_CARDINALITY_BASE = 4;
        public static final int ARRAY_CARDINALITY_BUCKETS = 8;

        long count;
        long numNulls;
        Map<ScalarType, LongCounter> presentedScalarTypes;
        Map<ScalarType, LongCounter> inferredScalarTypes;

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
                    Type type = null;
                    ScalarType inferredType = null;
                    while (elements.hasNext()) {
                        Object next = elements.next();
                        if (next instanceof Map) {
                            addNested((Map)next);
                            if (type == null) {
                                type = RelationType.INSTANCE;
                            } else if (!(type instanceof RelationType)) {
                                throw new IllegalArgumentException(String.format("Elements with incompatible types in same list: %s", o));
                            }
                        } else {
                            //not an array or map => must be scalar, let's find the common scalar type for all elements
                            ScalarType singleType = TypeMapping.scalar2Sqml(o);
                            if (type == null) {
                                type = singleType;
                            } else if (!(type.getClass().isInstance(singleType))) {
                                throw new IllegalArgumentException(String.format("Elements with incompatible types in same list: %s", o));
                            }
                            ScalarType inferredSingleType = TypeMapping.inferType(singleType, next);
                            //Need to be careful with how we infer types for lists - only infer if all elements agree
                            if (inferredType == null) {
                                inferredType = inferredSingleType;
                            } else if (!inferredSingleType.equals(inferredType)) {
                                inferredType = (ScalarType) type;
                            }
                        }
                    }
                    if (type instanceof ScalarType) incPresentedType((ScalarType) type);
                    if (inferredType!=null && !inferredType.equals(type)) incInferredType(inferredType);
                    addArrayCardinality(numElements);
                } else {
                    //Single element
                    if (o instanceof Map) {
                        addNested((Map)o);
                    } else {
                        //not an array or map => must be scalar
                        ScalarType type = TypeMapping.scalar2Sqml(o);
                        incPresentedType(type);
                        ScalarType inferredType = TypeMapping.inferType(type, o);
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

        private void incPresentedType(ScalarType type) {
            if (presentedScalarTypes==null) presentedScalarTypes=new HashMap<>(4);
            incType(type, presentedScalarTypes);
        }

        private void incInferredType(ScalarType type) {
            if (inferredScalarTypes==null) inferredScalarTypes=new HashMap<>(4);
            incType(type, inferredScalarTypes);
        }

        private static void incType(ScalarType type, Map<ScalarType, LongCounter> countMap) {
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

        private static Map<ScalarType, Long> convert(Map<ScalarType, LongCounter> typeCounter) {
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

        private static void mergeCounterMaps(Map<ScalarType, LongCounter> to, Map<ScalarType, LongCounter> from) {
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

        private static Map<ScalarType, LongCounter> copyCounter(Map<ScalarType, LongCounter> counter) {
            Map<ScalarType, LongCounter> newMap = new HashMap<>(counter.size());
            counter.forEach((k,v) -> newMap.put(k,v.clone()));
            return newMap;
        }

    }




}
