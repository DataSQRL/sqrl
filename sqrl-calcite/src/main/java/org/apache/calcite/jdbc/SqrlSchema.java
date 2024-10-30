package org.apache.calcite.jdbc;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.util.PrimaryKeyMap.Builder;
import com.datasqrl.plan.validate.ResolvedImport;
import com.datasqrl.plan.validate.ScriptPlanner.Mutation;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.inject.Singleton;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NameMultimap;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;

@Getter
@Singleton
public class SqrlSchema extends SimpleCalciteSchema {
  private final NameCanonicalizer nameCanonicalizer;

  private final List<ResolvedExport> exports = new ArrayList<>();
  private final List<ResolvedImport> imports = new ArrayList<>();
  private final Set<URL> jars = new LinkedHashSet<>();

  //Current table mapping
  private final Map<NamePath, NamePath> pathToAbsolutePathMap = new LinkedHashMap<>();
  //Required for looking up tables
  private final Map<NamePath, String> pathToSysTableMap = new LinkedHashMap<>();

  private final Map<String, UserDefinedFunction> udf = new LinkedHashMap<>();
  private final Map<List<String>, SqlOperator> udfListMap = new LinkedHashMap<>();
  private final Map<List<String>, SqlOperator> internalNames = new LinkedHashMap<>();

  private final AtomicInteger uniqueCompilerId = new AtomicInteger(0);
  private final AtomicInteger uniquePkId = new AtomicInteger(0);
  private final AtomicInteger uniqueMacroInt = new AtomicInteger(0);
  private final Map<Name, AtomicInteger> tableNameToIdMap = new LinkedHashMap<>();

  //API

  private final Map<APIMutation, Object> mutations = new LinkedHashMap<>();
  private final Map<NamePath, SqrlModule> modules = new LinkedHashMap<>();
  private final Map<APISubscription, Object> subscriptions = new LinkedHashMap<>();
  private final Map<SqrlTableMacro, Object> apiExports = new LinkedHashMap<>();
  private final List<APIQuery> queries = new ArrayList<>();
  private Set<SqlNode> addlSql = new LinkedHashSet<>();

  public SqrlSchema(TypeFactory typeFactory, NameCanonicalizer nameCanonicalizer) {
    super(null, CalciteSchema.createRootSchema(false, false).plus(), "");
    this.nameCanonicalizer = nameCanonicalizer;
  }

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
  public void add(ResolvedImport imp) {
    this.imports.add(imp);
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

  public List<SqrlTableMacro> getTableFunctions(NamePath path) {
    return getFunctions(path.getDisplay(), false)
        .stream().filter(f->f instanceof SqrlTableMacro)
        .map(f->(SqrlTableMacro)f)
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  public SqrlTableMacro getTableFunction(String name) {
    return (SqrlTableMacro)Iterables.getOnlyElement(getFunctions(name, false));
  }

  public void addFunction(String canonicalName, SqlOperator function) {
//    this.udf.put(nameCanonicalizer.getCanonical(canonicalName), function);
    this.udfListMap.put(List.of(nameCanonicalizer.getCanonical(canonicalName)), function);
//    this.internalNames.put(List.of(function.getName()), function);
  }

  public void addAdditionalSql(Set<SqlNode> addlSql) {
    this.addlSql.addAll(addlSql);
  }

  public NameMultimap<FunctionEntry> getFunctionMap() {
    return this.functionMap;
  }

  @Getter
  Map<String, String> fncAlias = new HashMap<>();
  public void addFunctionAlias(String name, String function) {
    fncAlias.put(name, function);
  }
}
