package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.BasicTypeManager;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.RelationType;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class FieldStats implements Serializable {

    long count = 0;
    long numNulls = 0;
    Map<FieldTypeStats,FieldTypeStats> types = new HashMap<>(4);
    Map<String, AtomicLong> nameCounts = new HashMap<>(2);

    public FieldStats() {
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
                    if (type instanceof BasicType && elementType instanceof BasicType) {
                        type = BasicTypeManager.combine((BasicType)type,(BasicType)elementType, true);
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

    public void add(Object o, @NonNull String displayName, NameCanonicalizer canonicalizer) {
        count++;
        addNameCount(displayName,1);
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
                            relationStats = new RelationStats();
                            //Try to infer type
                            inferredType = BasicTypeManager.inferType(map);
                        } else if (inferredType != null) {
                            BasicType infer2 = BasicTypeManager.inferType(map);
                            if (infer2==null || !infer2.equals(inferredType)) inferredType = null;
                        }
                        relationStats.add(map, canonicalizer);
                    } else {
                        //not an array or map => must be scalar, let's find the common scalar type for all elements
                        if (numElements==0) {
                            elementType = getBasicType(next);
                            //Try to infer type
                            if (next instanceof String) inferredType = BasicTypeManager.inferType((String)next);
                        } else if (inferredType != null) {
                            elementType = BasicTypeManager.combine((BasicType)elementType,getBasicType(next),true);
                            BasicType infer2 = BasicTypeManager.inferType((String)next);
                            if (infer2==null || !infer2.equals(inferredType)) inferredType = null;
                        }
                    }
                    numElements++;
                }
                FieldTypeStats fieldStats = setOrGet(FieldTypeStats.of(elementType, inferredType, arrayDepth));
                fieldStats.add(numElements,relationStats);
            } else {
                //Single element
                Type elementType;
                BasicType inferredType = null;
                if (o instanceof Map) {
                    elementType = RelationType.EMPTY;
                    inferredType = BasicTypeManager.inferType((Map)o);
                } else {
                    //not an array or map => must be scalar
                    elementType = getBasicType(o);
                    //Try to infer type
                    if (o instanceof String) {
                        inferredType = BasicTypeManager.inferType((String)o);
                    }
                }
                FieldTypeStats fieldStats = setOrGet(FieldTypeStats.of(elementType, inferredType, 0));
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
        BasicType elementType = BasicTypeManager.getTypeByJavaClass(o.getClass());
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
            thisStats.merge(fstats);
        }
        acc.nameCounts.forEach( (n,c) -> addNameCount(n, c.get()));
    }

    private void addNameCount(@NonNull String name, long count) {
        name = name.trim();
        AtomicLong counter = nameCounts.get(name);
        if (counter==null) {
            counter = new AtomicLong(0);
            nameCounts.put(name,counter);
        }
        counter.addAndGet(count);
    }

    String getDisplayName() {
        return nameCounts.entrySet().stream().max(new Comparator<Map.Entry<String, AtomicLong>>() {
            @Override
            public int compare(Map.Entry<String, AtomicLong> o1, Map.Entry<String, AtomicLong> o2) {
                return Long.compare(o1.getValue().get(),o2.getValue().get());
            }
        }).get().getKey();
    }

}
