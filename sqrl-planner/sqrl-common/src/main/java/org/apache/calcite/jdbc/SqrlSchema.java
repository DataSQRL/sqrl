package org.apache.calcite.jdbc;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.metadata.MetadataStore;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.util.StreamUtil;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
  private final List<SQRLTable> sqrlTables = new ArrayList<>();
  private final Map<List<String>, List<String>> relationships = new HashMap();
  private final Set<URL> jars = new HashSet<>();
  private final Map<List<String>, Relationship> relFncs = new HashMap<>();

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

  public<T extends Table> List<T> getTables(Class<T> clazz) {
    return getTableStream(clazz).collect(Collectors.toList());
  }

  public List<SQRLTable> getAllTables() {
    return this.sqrlTables;
  }

  public List<SQRLTable> getRootTables() {
    return this.sqrlTables.stream()
        .filter(s->s.getPath().size() == 1)
        .collect(Collectors.toList());
  }

  public void addSqrlTable(SQRLTable root) {
    if (root instanceof RootSqrlTable) {
      plus().add(String.join(".", root.getPath().toStringList()) + "$"
          + sqrlFramework.getUniqueTableInt().incrementAndGet(),(RootSqrlTable)root
          );
    }

    for (int i = 0; i < sqrlTables.size(); i++) {
      SQRLTable table = sqrlTables.get(i);
      if (table.getPath().equals(root.getPath())) {
        sqrlTables.remove(i);
      }
    }

    this.sqrlTables.add(root);
  }

  public SQRLTable getSqrlTable(NamePath path) {
    for (SQRLTable table : sqrlTables) {
      if (table.getPath().equals(path)) {
        return table;
      }
    }

    return null;
  }

  public <R, C> R accept(CalciteSchemaVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public Set<URL> getJars() {
    return jars;
  }

  public void addJar(URL url) {
    jars.add(url);
  }

  public void addRelationship(Relationship relationship) {
    if (relationships.containsKey(relationship)) {
      return;
    }

    relationships.put(relationship.getPath().toStringList(), relationship.getToTable().getPath().toStringList());
    this.relFncs.put(relationship.getPath().toStringList(), relationship);
    plus().add(String.join(".", relationship.getPath().toStringList()) + "$"
        + sqrlFramework.getUniqueTableInt().incrementAndGet(), relationship);
  }
}
