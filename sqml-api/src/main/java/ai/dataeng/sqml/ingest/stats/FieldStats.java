package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.schema2.basic.BasicType;
import ai.dataeng.sqml.schema2.basic.BasicTypeManager;
import ai.dataeng.sqml.schema2.basic.ConversionError;
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
import java.util.function.Function;
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

    public static TypeSignature.Simple detectTypeSignature(Object o,
                                                           Function<String,BasicType> detectFromString,
                                                           Function<Map<String,Object>,BasicType> detectFromComposite) {
        Type rawType = null;
        BasicType detectedType = null;
        int arrayDepth = 0;
        if (isArray(o)) {
            Pair<Stream<Object>,Integer> array = flatMapArray(o);
            arrayDepth = array.getRight();
            Iterator<Object> arrIter = array.getLeft().iterator();
            int numElements = 0;
            while (arrIter.hasNext()) {
                Object next = arrIter.next();
                if (next==null) continue;
                if (next instanceof Map) {
                    Map map = (Map)next;
                    if (numElements==0) {
                        rawType = RelationType.EMPTY;
                        //Try to detect type
                        detectedType = detectFromComposite.apply(map);
                    } else if (detectedType != null) {
                        BasicType detect2 = detectFromComposite.apply(map);
                        if (detect2==null || !detect2.equals(detectedType)) detectedType = null;
                    }
                } else {
                    //not an array or map => must be scalar, let's find the common scalar type for all elements
                    if (numElements==0) {
                        rawType = getBasicType(next);
                        //Try to detect type
                        if (next instanceof String) detectedType = detectFromString.apply((String)next);
                    } else if (detectedType != null) {
                        rawType = BasicTypeManager.combine((BasicType)rawType,getBasicType(next),true);
                        BasicType detect2 = detectFromString.apply((String)next);
                        if (detect2==null || !detect2.equals(detectedType)) detectedType = null;
                    }
                }
                numElements++;
            }
        } else {
            //Single element
            if (o instanceof Map) {
                rawType = RelationType.EMPTY;
                detectedType = detectFromComposite.apply((Map)o);
            } else {
                //not an array or map => must be scalar
                rawType = getBasicType(o);
                //Try to detect type
                if (o instanceof String) {
                    detectedType = detectFromString.apply((String)o);
                }
            }
        }
        return new TypeSignature.Simple(rawType, detectedType==null?rawType:detectedType, arrayDepth);
    }

    public void add(Object o, @NonNull String displayName, NameCanonicalizer canonicalizer) {
        count++;
        addNameCount(displayName,1);
        if (o==null) {
            numNulls++;
        } else {
            TypeSignature.Simple typeSignature = detectTypeSignature(o, BasicTypeManager::detectType,
                    BasicTypeManager::detectType);
            FieldTypeStats fieldStats = setOrGet(FieldTypeStats.of(typeSignature));
            if (isArray(o)) {
                Iterator<Object> arrIter = flatMapArray(o).getLeft().iterator();
                int numElements = 0;
                while (arrIter.hasNext()) {
                    Object next = arrIter.next();
                    if (next==null) continue;
                    if (next instanceof Map) {
                        fieldStats.addNested((Map)next, canonicalizer);
                    }
                    numElements++;
                }
                fieldStats.add(numElements);
            } else {
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
