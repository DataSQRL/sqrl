package ai.dataeng.sqml.schema2.basic;

import ai.dataeng.sqml.schema2.Type;
import com.google.common.collect.Multimap;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface BasicType<JavaType> extends Type {

    String getName();

    BasicType parentType();

    TypeConversion<JavaType> conversion();


}