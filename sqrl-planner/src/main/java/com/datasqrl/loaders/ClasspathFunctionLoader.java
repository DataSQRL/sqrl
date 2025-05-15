package com.datasqrl.loaders;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

@AllArgsConstructor
public class ClasspathFunctionLoader {

  public static final Set<Class<? extends FunctionDefinition>> flinkUdfClasses = Set.of(
      ScalarFunction.class,
      AggregateFunction.class,
      UserDefinedFunction.class,
      TableFunction.class,
      TableAggregateFunction.class,
      AsyncScalarFunction.class
  );

  public static final List<String> truncatedPackagePrefix = List.of(
    "com.datasqrl.flinkrunner.",
    "com.datasqrl."
  );

  private final HashMultimap<NamePath, FunctionDefinition> functionsByPackage;

  private static NamePath getNamePathFromFunction(FunctionDefinition function) {
    String packageName = function.getClass().getPackageName();
    //Remove certain pre-defined prefixes for more concise names of system functions
    for (String prefix : truncatedPackagePrefix) {
      if (packageName.startsWith(prefix)) {
        packageName = packageName.substring(prefix.length());
      }
    }
    return NamePath.parse(packageName);
  }

  public ClasspathFunctionLoader() {
    functionsByPackage = flinkUdfClasses.stream()
        .flatMap(this::loadClasses)
        .collect(
            HashMultimap::create,
            (multimap, function) -> multimap.put(getNamePathFromFunction(function), function), 
            Multimap::putAll);
  }

  private Stream<? extends FunctionDefinition> loadClasses(Class<? extends FunctionDefinition> serviceInterface) {
    return StreamSupport.stream(ServiceLoader.load(serviceInterface).spliterator(), false);
  }

  public Set<NamePath> loadedLibraries() {
    return functionsByPackage.keySet();
  }

  public List<NamespaceObject> load(NamePath namePath) {
    if (functionsByPackage.containsKey(namePath)) {
      List<NamespaceObject> fctObjetcs = functionsByPackage.get(namePath).stream()
          .map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList());
      return fctObjetcs;
    }
    return List.of();
  }
}