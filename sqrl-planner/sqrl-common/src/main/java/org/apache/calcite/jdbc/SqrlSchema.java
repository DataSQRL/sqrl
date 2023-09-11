package org.apache.calcite.jdbc;

import static org.apache.calcite.sql.validate.SqlValidatorUtil.ATTEMPT_SUGGESTER;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.local.generate.ResolvedExport;
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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

@Getter
public class SqrlSchema extends SimpleCalciteSchema {
  private final SqrlFramework sqrlFramework;
  private final List<ResolvedExport> exports = new ArrayList<>();
  private final List<SQRLTable> sqrlTables = new ArrayList<>();
  private final Map<List<String>, List<String>> relationships = new HashMap();
  private final Set<URL> jars = new HashSet<>();

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

  public void addRelationship(List<String> from, List<String> to) {
    this.relationships.put(from, to);
  }

  public String getUniqueFunctionName(List<String> path) {
    int attempt = 1;
    Set<String> names = plus().getFunctionNames()
        .stream().map(s-> Name.system(s).getCanonical())
        .collect(Collectors.toSet());
    while (true) {
      String name = String.join(".", path) + "$" + attempt;
      //todo fix
      if (names.contains(Name.system(name).getCanonical())) {
        attempt++;
      } else {
        return name;
      }
    }
  }

  public Set<URL> getJars() {
    return jars;
  }

  public void addJar(URL url) {
    jars.add(url);
  }
}
