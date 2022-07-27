package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.ArraySqlType;
import scala.annotation.meta.field;

/**
 *
 */
@Getter
public class StubTable implements Table, Schema, ScannableTable {

  private RelDataType dataType;
  private Optional<StubTable> parent;

  public StubTable() {

  }

  public StubTable(RelDataType dataType) {
    this.dataType = dataType;
    this.parent = Optional.empty();
  }

  public StubTable(RelDataType dataType, StubTable parent) {

    this.dataType = dataType;
    this.parent = Optional.of(parent);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    List list = new ArrayList(dataType.getFieldList());
    getParent().map(p->list.add(new RelDataTypeFieldImpl(
        "PARENT", list.size(), convertToStruct(p.getDataType()))));
    return new RelRecordType(StructKind.PEEK_FIELDS_NO_EXPAND, list);
  }

  private RelDataType convertToStruct(RelDataType dataType) {
    //parent may refer to root but is actually workable
    RelRecordType r = (RelRecordType)dataType;
    SqrlTypeFactory t = new SqrlTypeFactory(new SqrlTypeSystem());
    return t.createStructType(StructKind.PEEK_FIELDS_NO_EXPAND,
        r.getFieldList().stream()
            .map(e->e.getType())
            .collect(Collectors.toList()),
        r.getFieldNames());
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public TableType getJdbcTableType() {
    return TableType.TYPED_TABLE;
  }

  @Override
  public boolean isRolledUp(String s) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(String s, SqlCall sqlCall, SqlNode sqlNode,
      CalciteConnectionConfig calciteConnectionConfig) {
    return false;
  }

  @Override
  public Table getTable(String s) {
    if (s.equalsIgnoreCase(ReservedName.PARENT.getCanonical()) && parent.isPresent()) {
      return parent.get();
    }

    Optional<RelDataType> field = Optional.ofNullable(dataType.getField(s, false, false))
        .filter(f->
            f.getType() instanceof ArraySqlType || f.getType() instanceof RelRecordType)
        .map(f->f.getType() instanceof ArraySqlType ? f.getType().getComponentType() : f.getType())
        ;
    if (field.isPresent()) {
      return new StubTable(field.get(), this);
    }
    return null;
  }

  @Override
  public Set<String> getTableNames() {
    Set<String> names = this.dataType.getFieldList().stream()
        .filter(f->
            f.getType() instanceof ArraySqlType || f.getType() instanceof RelRecordType)
        .map(f->f.getName())
        .collect(Collectors.toSet());
    parent.map(p->names.add(ReservedName.PARENT.getCanonical()));

    return names;
  }

  @Override
  public RelProtoDataType getType(String s) {
    return null;
  }

  @Override
  public Set<String> getTypeNames() {
    return null;
  }

  @Override
  public Collection<Function> getFunctions(String s) {
    return null;
  }

  @Override
  public Set<String> getFunctionNames() {
    return null;
  }

  @Override
  public Schema getSubSchema(String s) {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return null;
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return null;
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion schemaVersion) {
    return null;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }
}
