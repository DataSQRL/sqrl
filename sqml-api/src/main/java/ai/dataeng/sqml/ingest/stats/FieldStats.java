package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.schema.SourceTableSchema;
import ai.dataeng.sqml.ingest.accumulator.LogarithmicHistogram;
import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.RelationType;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.accumulators.LongCounter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FieldStats {

    private final NameCanonicalizer canonicalizer;

    long count = 0;
    long numNulls = 0;
    Map<FieldTypeStats,FieldTypeStats> types = new HashMap<>(4);

    public FieldStats(NameCanonicalizer canonicalizer) {
        this.canonicalizer = canonicalizer;
    }

    public static void validate(Object o, DocumentPath path, ConversionError.Bundle<StatsIngestError> errors,
                                NameCanonicalizer canonicalizer) {
        if (isArray(o)) {
            Type type = null;
            Pair<Stream<Object>,Integer> array = flatMapArray(o);
            Iterator<Object> arrIter = array.getLeft().iterator();
            while (arrIter.hasNext()) {
                Object next = arrIter.next();
                if (next==null) continue;
                Type elementType;
                if (next instanceof Map) {
                    elementType = RelationType.EMPTY;
                    if (array.getRight()!=1) {
                        errors.add(StatsIngestError.fatal(path,"Nested arrays of objects are not supported: [%s]", o));
                    }
                    RelationStats.validate((Map)next,path,errors, canonicalizer);
                } else {
                    //since we flatmapped, this must be a scalar
                    elementType = getBasicType(next, errors, path);
                    if (elementType == null) return;
                }
                if (type == null) type = elementType;
                else if (!elementType.equals(type)) {
                    Type newType;
                    if (type instanceof BasicType && elementType instanceof BasicType
                            && (newType=((BasicType)type).combine((BasicType)elementType))!=null) {
                        type = newType;
                    } else {
                        errors.add(StatsIngestError.fatal(path,"Array contains elements with incompatible types: [%s]. Found [%s] and [%s]", o, type, elementType));
                    }
                }
            }
        } else if (o != null) {
            //Single element
            if (o instanceof Map) {
                RelationStats.validate((Map)o,path,errors, canonicalizer);
            } else {
                //not an array or map => must be scalar
                getBasicType(o, errors, path);
            }
        }
    }

    public void add(Object o) {
        count++;
        if (o==null) {
            numNulls++;
        } else {
            if (isArray(o)) {
                Pair<Stream<Object>,Integer> array = flatMapArray(o);
                int arrayDepth = array.getRight();
                Iterator<Object> arrIter = array.getLeft().iterator();
                //Data type for all elements in list
                Type elementType = null;
                BasicType inferredType = null;
                RelationStats relationStats = null;
                int numElements = 0;
                while (arrIter.hasNext()) {
                    Object next = arrIter.next();
                    if (next==null) continue;
                    if (next instanceof Map) {
                        Map map = (Map)next;
                        if (numElements==0) {
                            elementType = RelationType.EMPTY;
                            relationStats = new RelationStats(canonicalizer);
                            //Try to infer type
                            inferredType = BasicType.inferType(map);
                        } else if (inferredType != null) {
                            BasicType infer2 = BasicType.inferType(map);
                            if (infer2==null || !infer2.equals(inferredType)) inferredType = null;
                        }
                        relationStats.add(map);
                    } else {
                        //not an array or map => must be scalar, let's find the common scalar type for all elements
                        if (numElements==0) {
                            elementType = getBasicType(next);
                            //Try to infer type
                            if (next instanceof String) inferredType = BasicType.inferType((String)next);
                        } else if (inferredType != null) {
                            BasicType infer2 = BasicType.inferType((String)next);
                            if (infer2==null || !infer2.equals(inferredType)) inferredType = null;
                        }
                    }
                    numElements++;
                }
                FieldTypeStats fieldStats = setOrGet(FieldTypeStats.of(elementType,inferredType));
                fieldStats.add(numElements,relationStats);
            } else {
                //Single element
                Type elementType;
                BasicType inferredType = null;
                if (o instanceof Map) {
                    elementType = RelationType.EMPTY;
                    inferredType = BasicType.inferType((Map)o);
                    addNested((Map)o);
                } else {
                    //not an array or map => must be scalar
                    elementType = getBasicType(o);
                    //Try to infer type
                    if (o instanceof String) inferredType = BasicType.inferType((String)o);
                }
                FieldTypeStats fieldStats = setOrGet(FieldTypeStats.of(elementType,inferredType));
                fieldStats.add();
                if (o instanceof Map) fieldStats.addNested((Map)o,canonicalizer);
            }
        }
    }

    private FieldTypeStats setOrGet(FieldTypeStats stats) {
        FieldTypeStats existing = types.get(stats);
        if (existing!=null) return existing;
        types.put(stats,stats);
        return stats;
    }

    public static BasicType getBasicType(@NonNull Object o) {
        return getBasicType(o, null,null);
    }

    public static BasicType getBasicType(@NonNull Object o, ConversionError.Bundle<StatsIngestError> errors, DocumentPath path) {
        BasicType elementType = BasicType.JAVA_TO_TYPE.get(o.getClass());
        if (elementType == null) {
            if (errors != null) {
                errors.add(StatsIngestError.fatal(path, "Unsupported data type for value: %s [%s]", o, o.getClass()));
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + o.getClass());
            }
        }
        return elementType;
    }

    public static boolean isArray(Object arr) {
        return arr!=null && (arr instanceof Collection || arr.getClass().isArray());
    }

    public static Collection<Object> array2Collection(Object arr) {
        Preconditions.checkArgument(isArray(arr));
        final Collection col;
        if (arr instanceof Collection) col = (Collection)arr;
        else col = Arrays.asList((Object[])arr);
        return col;
    }

    public static Pair<Stream<Object>,Integer> flatMapArray(Object arr) {
        if (isArray(arr)) {
            Collection col = array2Collection(arr);
            if (col.stream().noneMatch(FieldStats::isArray)) {
                return new ImmutablePair<>(col.stream(), 1);
            } else {
                AtomicInteger depth = new AtomicInteger(0);
                Stream<Pair<Stream<Object>,Integer>> sub = col.stream().map(FieldStats::flatMapArray);
                Stream<Object> res = sub.flatMap(p -> {
                            depth.getAndAccumulate(p.getRight(), Math::max);
                            return p.getLeft();
                        });
                return new ImmutablePair<>(res,depth.get()+1);
            }
        } else return new ImmutablePair<>(Stream.of(arr),0);
    }

    public void merge(FieldStats acc) {
        count+=acc.count;
        numNulls+=acc.numNulls;
        for (FieldTypeStats fstats : acc.types.keySet()) {
            FieldTypeStats thisStats = types.get(fstats);
            if (thisStats==null) {
                thisStats = new FieldTypeStats(fstats);
                types.put(thisStats,thisStats);
            }
            thisStats.merge(fstats,canonicalizer);
        }
    }



    public void collectSchema(SourceTableSchema.Builder builder, Name fieldName) {
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
            for (Map.Entry<ScalarType, LongCounter> infer : inferredScalarTypes.entrySet()) {
                if (type == null) type = infer.getKey();
                else {
                    type = (ScalarType) type.combine(infer.getKey());
                    if (type == null) break; //for now, we don't allow dual types, so abandon type inference
                }
                total += infer.getValue().getLocalValue();
            }
            assert total <= nonNull;

            //If we cannot unambiguously infer a type, take the presented type
            if (type == null || total < nonNull) {
                for (Map.Entry<ScalarType, LongCounter> infer : presentedScalarTypes.entrySet()) {
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




}
