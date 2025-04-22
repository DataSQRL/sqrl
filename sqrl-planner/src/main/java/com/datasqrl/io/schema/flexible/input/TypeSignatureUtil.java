package com.datasqrl.io.schema.flexible.input;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.input.TypeSignature.Simple;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BasicType;
import com.datasqrl.io.schema.flexible.type.basic.BasicTypeManager;

import lombok.NonNull;

public class TypeSignatureUtil {

  public static Optional<Simple> detectSimpleTypeSignature(Object o,
      Function<String, BasicType> detectFromString,
      Function<Map<String, Object>, BasicType> detectFromComposite) {
    if (o == null) {
		return Optional.empty();
	}
    Type rawType = null;
    BasicType detectedType = null;
    var arrayDepth = 0;
    if (isArray(o)) {
      var array = flatMapArray(o);
      arrayDepth = array.getRight();
      var arrIter = array.getLeft().iterator();
      var numElements = 0;
      while (arrIter.hasNext()) {
        var next = arrIter.next();
        if (next == null) {
          continue;
        }
        if (next instanceof Map map) {
          if (numElements == 0) {
            rawType = RelationType.EMPTY;
            //Try to detect type
            detectedType = detectFromComposite.apply(map);
          } else if (detectedType != null) {
            var detect2 = detectFromComposite.apply(map);
            if (detect2 == null || !detect2.equals(detectedType)) {
              detectedType = null;
            }
          }
        } else {
          //not an array or map => must be scalar, let's find the common scalar type for all elements
          if (numElements == 0) {
            rawType = getBasicType(next);
            //Try to detect type
            if (next instanceof String string) {
              detectedType = detectFromString.apply(string);
            }
          } else if (detectedType != null) {
            rawType = BasicTypeManager.combineForced((BasicType) rawType, getBasicType(next));
            var detect2 = detectFromString.apply((String) next);
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
      if (o instanceof Map map) {
        rawType = RelationType.EMPTY;
        detectedType = detectFromComposite.apply(map);
      } else {
        //not an array or map => must be scalar
        rawType = getBasicType(o);
        //Try to detect type
        if (o instanceof String string) {
          detectedType = detectFromString.apply(string);
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
    if (arr instanceof Collection collection) {
      col = collection;
    } else {
      col = Arrays.asList((Object[]) arr);
    }
    return col;
  }

  public static BasicType getBasicType(@NonNull Object o) {
    return getBasicType(o, null);
  }

  public static BasicType getBasicType(@NonNull Object o, ErrorCollector errors) {
    var elementType = BasicTypeManager.getTypeByJavaClass(o.getClass());
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
        var depth = new AtomicInteger(0);
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
