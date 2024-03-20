package com.datasqrl.schema.input;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.schema.input.TypeSignature.Simple;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BasicType;
import com.datasqrl.schema.type.basic.BasicTypeManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class TypeSignatureUtil {

  public static Optional<Simple> detectSimpleTypeSignature(Object o,
      Function<String, BasicType> detectFromString,
      Function<Map<String, Object>, BasicType> detectFromComposite) {
    if (o == null) return Optional.empty();
    Type rawType = null;
    BasicType detectedType = null;
    int arrayDepth = 0;
    if (isArray(o)) {
      Pair<Stream<Object>, Integer> array = flatMapArray(o);
      arrayDepth = array.getRight();
      Iterator<Object> arrIter = array.getLeft().iterator();
      int numElements = 0;
      while (arrIter.hasNext()) {
        Object next = arrIter.next();
        if (next == null) {
          continue;
        }
        if (next instanceof Map) {
          Map map = (Map) next;
          if (numElements == 0) {
            rawType = RelationType.EMPTY;
            //Try to detect type
            detectedType = detectFromComposite.apply(map);
          } else if (detectedType != null) {
            BasicType detect2 = detectFromComposite.apply(map);
            if (detect2 == null || !detect2.equals(detectedType)) {
              detectedType = null;
            }
          }
        } else {
          //not an array or map => must be scalar, let's find the common scalar type for all elements
          if (numElements == 0) {
            rawType = getBasicType(next);
            //Try to detect type
            if (next instanceof String) {
              detectedType = detectFromString.apply((String) next);
            }
          } else if (detectedType != null) {
            rawType = BasicTypeManager.combineForced((BasicType) rawType, getBasicType(next));
            BasicType detect2 = detectFromString.apply((String) next);
            if (detect2 == null || !detect2.equals(detectedType)) {
              detectedType = null;
            }
          }
        }
        numElements++;
      }
      if (numElements==0) {
        //empty array/list
        return Optional.empty();
      }
    } else {
      //Single element
      if (o instanceof Map) {
        rawType = RelationType.EMPTY;
        detectedType = detectFromComposite.apply((Map) o);
      } else {
        //not an array or map => must be scalar
        rawType = getBasicType(o);
        //Try to detect type
        if (o instanceof String) {
          detectedType = detectFromString.apply((String) o);
        }
      }
    }
    return Optional.of(new TypeSignature.Simple(rawType, detectedType == null ? rawType : detectedType,
        arrayDepth));
  }

  public static boolean isArray(Object arr) {
    return arr != null && (arr instanceof Collection || arr.getClass().isArray());
  }

  public static Collection<Object> array2Collection(Object arr) {
//    Preconditions.checkArgument(isArray(arr));
    final Collection col;
    if (arr instanceof Collection) {
      col = (Collection) arr;
    } else {
      col = Arrays.asList((Object[]) arr);
    }
    return col;
  }

  public static BasicType getBasicType(@NonNull Object o) {
    return getBasicType(o, null);
  }

  public static BasicType getBasicType(@NonNull Object o, ErrorCollector errors) {
    BasicType elementType = BasicTypeManager.getTypeByJavaClass(o.getClass());
    if (elementType == null) {
      if (errors != null) {
        errors.fatal("Unsupported data type for value: %s [%s]", o, o.getClass());
      } else {
        throw new IllegalArgumentException("Unsupported data type: " + o.getClass());
      }
    }
    return elementType;
  }

  public static Pair<Stream<Object>, Integer> flatMapArray(Object arr) {
    if (isArray(arr)) {
      Collection col = array2Collection(arr);
      if (col.stream().noneMatch(TypeSignatureUtil::isArray)) {
        return new ImmutablePair<>(col.stream(), 1);
      } else {
        AtomicInteger depth = new AtomicInteger(0);
        Stream<Pair<Stream<Object>, Integer>> sub = col.stream()
            .map(TypeSignatureUtil::flatMapArray);
        Stream<Object> res = sub.flatMap(p -> {
          depth.getAndAccumulate(p.getRight(), Math::max);
          return p.getLeft();
        });
        return new ImmutablePair<>(res, depth.get() + 1);
      }
    } else {
      return new ImmutablePair<>(Stream.of(arr), 0);
    }
  }
}
