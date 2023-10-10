package org.apache.calcite.jdbc;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.util.StreamUtil;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;

import java.util.List;
import java.util.stream.Stream;

@Getter
public class SqrlSchema extends SimpleCalciteSchema {
  private final SqrlFramework sqrlFramework;

  private final List<ResolvedExport> exports = new ArrayList<>();
  private final Set<URL> jars = new HashSet<>();

  //Current table mapping
  private final Map<NamePath, NamePath> pathToAbsolutePathMap = new HashMap<>();
  private final Map<NamePath, String> pathToSysTableMap = new HashMap<>();

  //Full table mapping
  private final Map<String, NamePath> sysTableToPathMap = new HashMap<>();
  private final Multimap<String, Relationship> sysTableToRelationshipMap = LinkedHashMultimap.create();

  private final Map<String, List<String>> sysTableToFieldNameMap = new HashMap<>();

  public SqrlSchema(SqrlFramework framework) {
    super(null, CalciteSchema.createRootSchema(false, false).plus(), "");
    sqrlFramework = framework;
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
    removePrefix(this.pathToAbsolutePathMap.keySet(),root.getName().toNamePath());
    plus().add(String.join(".", root.getPath().toStringList()) + "$"
        + sqrlFramework.getUniqueMacroInt().incrementAndGet(), root
        );
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
    NamePath toTable = sysTableToPathMap.get(relationship.getToTable());
    pathToAbsolutePathMap.put(relationship.getPath(), toTable);
    this.sysTableToRelationshipMap.put(relationship.getFromTable(), relationship);
    plus().add(String.join(".", relationship.getPath().toStringList()) + "$"
        + sqrlFramework.getUniqueMacroInt().incrementAndGet(), relationship);
  }

  public void addTableMapping(NamePath path, String nameId) {
    this.pathToSysTableMap.put(path, nameId);
    this.sysTableToPathMap.put(nameId, path);
  }
}
