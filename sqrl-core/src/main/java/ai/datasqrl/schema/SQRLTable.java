package ai.datasqrl.schema;

import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.local.HasToTable;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.util.Pair;

/**
 * A {@link SQRLTable} represents a logical table in the SQRL script which contains fields that are
 * either columns or relationships.
 *
 * Note, that SQRLTables are always flat and hierarchical data is represented as multiple SQRLTables
 * with parent-child relationships between them.
 *
 */
@Getter
public class SQRLTable implements Table, org.apache.calcite.schema.Schema, ScannableTable
    , CustomColumnResolvingTable, HasToTable
{

  @NonNull
  NamePath path;
  @NonNull
  final FieldList fields = new FieldList();

  private RelDataType fullDataType;
  private Optional<SQRLTable> parent;
  private Optional<List<TableFunctionArgument>> tableArguments = Optional.empty();
  private VirtualRelationalTable vt;

  public SQRLTable() {

  }

  public SQRLTable(RelDataType fullDataType) {
    this.fullDataType = fullDataType;
    this.parent = Optional.empty();
  }

  public SQRLTable(RelDataType fullDataType, SQRLTable parent) {
    this.fullDataType = fullDataType;
    this.parent = Optional.of(parent);
  }

  public SQRLTable(@NonNull NamePath path) {
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
    rebuildType();
    return col;
  }

  public Relationship addRelationship(Name name, SQRLTable toTable, JoinType joinType,
                                      Multiplicity multiplicity, SqlNode node) {
    Relationship rel = new Relationship(name, getNextFieldVersion(name), this, toTable, joinType, multiplicity, node);
    fields.addField(rel);
    rebuildType();
    return rel;
  }

  public void setVT(VirtualRelationalTable vt) {
    this.vt = vt;
    rebuildType();
  }

  public void rebuildType() {
    RelDataTypeFactory typeFactory = PlannerFactory.getTypeFactory();
    FieldInfoBuilder b = new FieldInfoBuilder(typeFactory);
//    for (Column field : getColumns(true)) {
//      b.add(field.getName().getDisplay(), field.getType())
//          .nullable(field.isNullable());
//    }

    if (vt != null) {
      b.addAll(vt.getRowType().getFieldList());
    }
    getAllRelationships().forEach(r->
//        remap to PEEK_FIELDS_NO_EXPAND?
    {
      b.add(new RelPlus(r.getName().getDisplay(), b.getFieldCount(),
          convertToStruct(r.getToTable().getFieldRowType(typeFactory))));
    });

    fullDataType = b.build();
  }

  public class RelPlus extends RelDataTypeFieldImpl {

    public RelPlus(String name, int index, RelDataType type) {
      super(name, index, type);
    }
  }

  private RelDataType getFieldRowType(RelDataTypeFactory typeFactory) {
    FieldInfoBuilder b = new FieldInfoBuilder(typeFactory);
    for (Column field : getColumns(true)) {
      b.add(new RelPlus(field.getName().getDisplay(), b.getFieldCount(), field.getType()));
    }
    return b.build();
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return vt.getRowType();
  }

  private RelDataType convertToStruct(RelDataType dataType) {
    //parent may refer to root but is actually workable
    if (dataType == null) {
      return null;
    }
    RelRecordType r = (RelRecordType)dataType;
    RelDataTypeFactory t = PlannerFactory.getTypeFactory();

    return new RelTypeWithHidden(dataType,
        r.getFieldList().stream()
            .collect(Collectors.toList()));
  }

  @Override
  public SQRLTable getToTable() {
    return this;
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
    Optional<SQRLTable> rel = this.getAllRelationships().filter(e->e.getName().getCanonical().equalsIgnoreCase(s))
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
//      return new SQRLTable(field.get(), this);
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

  public Optional<SQRLTable> walkTable(NamePath namePath) {
    if (namePath.isEmpty()) {
      return Optional.of(this);
    }
    Optional<Field> field = getField(namePath.getFirst());
    if (field.isEmpty() || !(field.get() instanceof Relationship)) {
      return Optional.empty();
    }
    Relationship rel = (Relationship) field.get();
    SQRLTable target = rel.getToTable();
    return target.walkTable(namePath.popFirst());
  }

  public Stream<Relationship> getAllRelationships() {
    return StreamUtil.filterByClass(fields.getFields(true),Relationship.class);
  }

//  public Optional<SQRLTable> getParent() {
//    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT).map(Relationship::getToTable).findFirst();
//  }

  public Collection<SQRLTable> getChildren() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.CHILD).map(Relationship::getToTable).collect(Collectors.toList());
  }

  public Optional<SQRLTable> getParent() {
    return getAllRelationships().filter(r -> r.getJoinType() == JoinType.PARENT).map(Relationship::getFromTable).findFirst();
  }

  public List<Column> getVisibleColumns() {
    return getColumns(true);
  }

  public List<Column> getColumns(boolean onlyVisible) {
    return StreamUtil.filterByClass(fields.getFields(onlyVisible),Column.class).collect(Collectors.toList());
  }

  public List<Field> walkField(List<String> names) {
    List<Field> fields = new ArrayList<>();
    SQRLTable t = this;
    for (String n : names) {
      Field field = t.getField(Name.system(n)).get();
      fields.add(field);
      if (field instanceof Relationship) {
        t = ((Relationship) field).getToTable();
      }
    }
    return fields;
  }

  /**
   * This can return an empty list if no results are found (eg when trying to resolve an alias)
   */
  @Override
  public List<Pair<RelDataTypeField, List<String>>> resolveColumn(RelDataType relDataType,
      RelDataTypeFactory relDataTypeFactory, List<String> list) {

    RelDataTypeField field = fullDataType.getField(list.get(0), false, false);
    if (field == null) {
      return List.of();
    }
    if (list.size() > 2) {
      RelDataType fieldType = buildDeepType(list);
      if (fieldType == null) {
        return List.of();
      }

      return List.of(Pair.of(
          fieldType.getField(list.get(0), false, false),
          list.subList(1, list.size())));
    }

    return List.of(Pair.of(
        field,
        list.subList(1, list.size())));
  }

  private RelDataType buildDeepType(List<String> columns) {
    RelDataTypeFactory typeFactory = PlannerFactory.getTypeFactory();
    FieldInfoBuilder b = new FieldInfoBuilder(typeFactory);
    if (vt != null) {
      b.addAll(vt.getRowType().getFieldList());
    }

    Optional<Field> field = this.getField(Name.system(columns.get(0)));
    if (field.isEmpty()) {
      return null;
    }
    else if (field.get() instanceof Relationship) {
      Relationship r = (Relationship)field.get();
      if (columns.isEmpty()) {
        return null;
      }
      RelDataType newField = r.getToTable().buildDeepType(columns.subList(1, columns.size()));

      b.add(r.getName().getDisplay(), newField);
    } else if (field.get() instanceof Column) {
      if (columns.size() > 1) {
        return null;
      }
      b.add(this.getVt().getRowType().getField(columns.get(0), false, false));
    }
    return b.build();
  }
}
