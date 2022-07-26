package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import ai.datasqrl.schema.Relationship.JoinType;
import lombok.Getter;
import lombok.NonNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

@Getter
public class ScriptTable implements Table, org.apache.calcite.schema.Schema, ScannableTable {

  @NonNull
  NamePath path;
  @NonNull
  final FieldList fields = new FieldList();

  private RelDataType dataType;
  private Optional<ScriptTable> parent;
//  private RelDataType type;

  public ScriptTable() {

  }

  public ScriptTable(RelDataType dataType) {
    this.dataType = dataType;
    this.parent = Optional.empty();
  }

  public ScriptTable(RelDataType dataType, ScriptTable parent) {

    this.dataType = dataType;
    this.parent = Optional.of(parent);
  }

  public ScriptTable(@NonNull NamePath path) {
    this.path = path;
  }

  public Name getName() {
    return path.getLast();
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append("Table[path=").append(path).append("]{\n");
    for (Field f : fields.getAccessibleFields()) s.append("\t").append(f).append("\n");
    s.append("}");
    return s.toString();
  }

  private int getNextFieldVersion(Name name) {
    return fields.nextVersion(name);
  }

  public Column addColumn(Name name, boolean visible, RelDataType type) {
    Column col = new Column(name, getNextFieldVersion(name), visible, type);
    fields.addField(col);
    return col;

//    this.relFields.add(new RelDataTypeFieldImpl(name.getDisplay(), relFields.size(), sqlType));
//    return null;
  }

  public Column addColumn(Name name, boolean visible) {
    throw new RuntimeException("no");
  }
//  public void addRelationship(Relationship relationship) {
//    fields.addField(relationship);
//  }
  public Relationship addRelationship(Name name, ScriptTable toTable, Relationship.JoinType joinType,
                                      Relationship.Multiplicity multiplicity) {
    Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, joinType, multiplicity);
    fields.addField(rel);
    return rel;
  }

  public void buildType() {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());
    FieldInfoBuilder b = new FieldInfoBuilder(typeFactory);
    for (Column field : getColumns(true)) {
      b.add(field.getName().getDisplay(), field.getType());
    }

    getAllRelationships().forEach(r->
        //remap to PEEK_FIELDS_NO_EXPAND?
        b.add(r.getName().getDisplay(), convertToStruct(r.getToTable().getFieldRowType(typeFactory))));

    dataType = b.build();
  }

  private RelDataType getFieldRowType(SqrlTypeFactory typeFactory) {
    FieldInfoBuilder b = new FieldInfoBuilder(typeFactory);
    for (Column field : getColumns(true)) {
      b.add(field.getName().getDisplay(), field.getType());
    }

    return b.build();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    if (dataType == null) {
      buildType();
    }

   return dataType;
  }

  private RelDataType convertToStruct(RelDataType dataType) {
    //parent may refer to root but is actually workable
    if (dataType == null) {
      return null;
    }
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
//    if (s.equalsIgnoreCase(ReservedName.PARENT.getCanonical()) && parent.isPresent()) {
//      return parent.get();
//    }
    Optional<ScriptTable> rel = this.getAllRelationships().filter(e->e.getName().getCanonical().equalsIgnoreCase(s))
        .map(r->r.getToTable())
        .findAny();

    return rel.orElse(null);
//
//    Optional<RelDataType> field = Optional.ofNullable(dataType.getField(s, false, false))
//        .filter(f->
//            f.getType() instanceof ArraySqlType || f.getType() instanceof RelRecordType)
//        .map(f->f.getType() instanceof ArraySqlType ? f.getType().getComponentType() : f.getType())
//        ;
//    if (field.isPresent()) {
//      return new ScriptTable(field.get(), this);
//    }
//    return null;
  }

  @Override
  public Set<String> getTableNames() {

    return this.getAllRelationships().map(s->s.getName().getDisplay()).collect(Collectors.toSet());
//    Set<String> names = this.dataType.getFieldList().stream()
//        .filter(f->
//            f.getType() instanceof ArraySqlType || f.getType() instanceof RelRecordType)
//        .map(f->f.getName())
//        .collect(Collectors.toSet());
//    parent.map(p->names.add(ReservedName.PARENT.getCanonical()));

//    return names;
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
  public org.apache.calcite.schema.Schema getSubSchema(String s) {
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
  public org.apache.calcite.schema.Schema snapshot(SchemaVersion schemaVersion) {
    return null;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return null;
  }

  public Optional<Field> getField(Name name) {
    return getField(name,false);
  }

  public Optional<Field> getField(Name name, boolean fullColumn) {
    return fields.getAccessibleField(name);
  }

  public Optional<ScriptTable> walkTable(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.of(this);
    }
    Optional<Field> field = getField(namePath.getFirst());
    if (field.isEmpty() || !(field.get() instanceof Relationship)) {
      return Optional.empty();
    }
    Relationship rel = (Relationship) field.get();
    ScriptTable target = rel.getToTable();
    return target.walkTable(namePath.popFirst());
  }

  public Stream<Relationship> getAllRelationships() {
    return fields.getFields(true).filter(Relationship.class::isInstance).map(Relationship.class::cast);
  }

//  public Optional<ScriptTable> getParent() {
//    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT).map(Relationship::getToTable).findFirst();
//  }

  public Collection<ScriptTable> getChildren() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.CHILD).map(Relationship::getToTable).collect(Collectors.toList());
  }

  public List<Column> getVisibleColumns() {
    return getColumns(true);
  }

  public List<Column> getColumns(boolean onlyVisible) {
    return fields.getFields(onlyVisible).filter(Column.class::isInstance).map(Column.class::cast).collect(Collectors.toList());
  }

  public List<Field> walkField(List<String> names) {
    List<Field> fields = new ArrayList<>();
    ScriptTable t = this;
    for (String n : names) {
      Field field = t.getField(Name.system(n)).get();
      fields.add(field);
      if (field instanceof Relationship) {
        t = ((Relationship) field).getToTable();
      }
    }
    return fields;
  }
}
