package org.apache.calcite.jdbc;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlTableFactory;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;

@Getter
public class SqrlSchema extends SimpleCalciteSchema {
  private final SqrlFramework sqrlFramework;
  private final TypeFactory typeFactory;

  private final List<ResolvedExport> exports = new ArrayList<>();
  private final Set<URL> jars = new HashSet<>();

  //Current table mapping
  private final Map<NamePath, NamePath> pathToAbsolutePathMap = new HashMap<>();
  //Required for looking up tables
  private final Map<NamePath, String> pathToSysTableMap = new HashMap<>();

  public SqrlSchema(SqrlFramework framework, TypeFactory typeFactory) {
    super(null, CalciteSchema.createRootSchema(false, false).plus(), "");
    sqrlFramework = framework;
    this.typeFactory = typeFactory;
  }

  //backwards compatibility classes for migration
  public RelOptPlanner getPlanner() {
    return sqrlFramework.getQueryPlanner().getPlanner();
  }

  public RelOptCluster getCluster() {
    return sqrlFramework.getQueryPlanner().getCluster();
  }

  public RelDataTypeFactory getTypeFactory() {
    return sqrlFramework.getTypeFactory();
  }
  // end backwards compat

  public<T extends TableFunction> Stream<T> getFunctionStream(Class<T> clazz) {
    return StreamUtil.filterByClass(getFunctionNames().stream()
        .flatMap(name -> getFunctions(name, false).stream()), clazz);
  }

  public<T extends Table> Stream<T> getTableStream(Class<T> clazz) {
    return StreamUtil.filterByClass(getTableNames().stream()
        .map(t->getTable(t,false).getTable()), clazz);
  }

  public List<ResolvedExport> getExports() {
    return exports;
  }

  public void add(ResolvedExport export) {
    this.exports.add(export);
  }

  public void addTable(RootSqrlTable root) {
    pathToAbsolutePathMap.put(root.getFullPath(), root.getAbsolutePath());
    plus().add(root.getDisplayName(), root);
  }

  public void clearFunctions(NamePath path) {
    String fncName = path.getDisplay();
    if (this.functionMap.containsKey(fncName, false)) {
      NamePath prefix = path;
      for (NamePath key : pathToAbsolutePathMap.keySet()) {
        if (key.size() >= prefix.size() && key.subList(0, prefix.size()).equals(prefix)) {
          String name = key.getDisplay();
          List<FunctionEntry> functionEntries = this.functionMap.map().get(name);
          for (FunctionEntry entry : new ArrayList<>(functionEntries)) {
            this.functionMap.remove(name, entry);
          }
        }
      }
      removePrefix(pathToAbsolutePathMap.keySet(), prefix);
    }

  }

  private void removePrefix(Set<NamePath> set, NamePath prefix) {
    set.removeIf(
        key -> key.size() >= prefix.size() && key.subList(0, prefix.size()).equals(prefix));
  }

  public Set<URL> getJars() {
    return jars;
  }

  public void addJar(URL url) {
    jars.add(url);
  }

  public void addRelationship(Relationship relationship) {
    pathToAbsolutePathMap.put(relationship.getFullPath(), relationship.getAbsolutePath());

    plus().add(relationship.getDisplayName(), relationship);
  }

  public static List<SqrlFunctionParameter> getExternalParams(List<FunctionParameter> params) {
    return params.stream()
        .map(p -> (SqrlFunctionParameter) p)
        .filter(p -> !p.isInternal())
        .collect(Collectors.toList());
  }

  public void addTableMapping(NamePath path, String nameId) {
    this.pathToSysTableMap.put(path, nameId);
  }

  public List<SqrlTableMacro> getTableFunctions() {
    return getFunctionNames().stream()
        .flatMap(f->getFunctions(f, false).stream())
        .filter(f->f instanceof SqrlTableMacro)
        .map(f->(SqrlTableMacro)f)
        .collect(Collectors.toList());
  }

  public SqrlTableMacro getTableFunction(String name) {
    return (SqrlTableMacro)Iterables.getOnlyElement(getFunctions(name, false));
  }
}
