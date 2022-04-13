package ai.dataeng.sqml.parser.sqrl.schema;

import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import ai.dataeng.sqml.parser.operator.Shredder;
import ai.dataeng.sqml.planner.CalcitePlanner;
import ai.dataeng.sqml.parser.sqrl.schema.StreamTable.StreamDataType;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.constraint.Cardinality;
import ai.dataeng.sqml.type.constraint.Constraint;
import ai.dataeng.sqml.type.constraint.ConstraintHelper;
import ai.dataeng.sqml.type.constraint.NotNull;
import ai.dataeng.sqml.type.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.type.schema.FlexibleSchemaHelper;
import graphql.com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexNode;
import ai.dataeng.sqml.planner.SqrlRelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

@AllArgsConstructor
public class SourceTableFactory {
  CalcitePlanner planner;
  public final static AtomicInteger tableIdCounter = new AtomicInteger(0);

  public Table create(SourceTableImport sourceTableImport, Optional<Name> alias) {
    Table table = createTable(sourceTableImport, alias);

    FlinkTableConverter tbConverter = new FlinkTableConverter();
    Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(sourceTableImport.getSourceSchema());

    planner.addTable(table.getId().toString(), new StreamTable(toDataType(ordersSchema.getKey().getColumns()),
        sourceTableImport));
    //todo hack in identifier for later recall.
    planner.addTable(table.getId().toString() + "_stream", new StreamTable(toDataType(ordersSchema.getKey().getColumns()), sourceTableImport));
    SqrlRelBuilder builder = planner.createRelBuilder();
    RelNode node = builder
        .scanStream(table.getId().toString(), sourceTableImport, table)
        .project(projectScalars(ordersSchema.getKey(), builder))
        //Project only scalars, shred remaining
        .build();
    table.setRelNode(node);

    setShredRelNodes(table, ordersSchema);
    return table;
  }

  private List<RexNode> projectScalars(Schema schema, SqrlRelBuilder builder) {
    List<RexNode> projects = new ArrayList<>();
    for (UnresolvedColumn col : schema.getColumns()) {
      UnresolvedPhysicalColumn column = (UnresolvedPhysicalColumn) col;
      if (column.getDataType() instanceof AtomicDataType) {
        projects.add(builder.field(1, 0, column.getName()));
      }
    }

    return projects;
  }

  private StreamDataType toDataType(List<UnresolvedColumn> c) {
    List<UnresolvedPhysicalColumn> columns = c.stream()
        .map(column->(UnresolvedPhysicalColumn) column)
        .collect(Collectors.toList());

    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<RelDataTypeField> fields = new ArrayList<>();
    for (UnresolvedPhysicalColumn column : columns) {
      fields.add(new RelDataTypeFieldImpl(column.getName(), fields.size(),
          factory.createFieldTypeFromLogicalType(((DataType)column.getDataType()).getLogicalType())));
    }

    return new StreamDataType(null, fields);
  }

  private void setShredRelNodes(Table table,
      Pair<Schema, TypeInformation> schema) {
    Map<String, UnresolvedColumn> fieldMap = Maps.uniqueIndex(schema.getLeft().getColumns(), e->e.getName());
    for (Field field : table.getFields().visibleList()) {
      if (field instanceof Relationship) {
        Relationship rel = (Relationship)  field;
        Table toTable = rel.toTable;
//        planner.addTable(table.getId().toString(), new StreamTable(ordersSchema.getKey()));
        UnresolvedColumn col = fieldMap.get(field.getName().getCanonical());
        RowType row = unboxArray(col);
        List<UnresolvedColumn> pks = rel.getTable().getPrimaryKeys().stream()
            .map(e-> fieldMap.get(e.getName().getCanonical()))
            .collect(Collectors.toList());

        planner.addTable(toTable.getId().toString(), new StreamTable(toDataTypeField(row.getFields(), toPk(pks, row.getFields().size())),
            null));
        planner.addTable(toTable.getId().toString() + "_stream", new StreamTable(toDataTypeField(row.getFields(), toPk(pks, row.getFields().size())),
            null));

        RelNode node = planner.createRelBuilder()
            .scanShred(rel.getTable(), toTable.getId().toString())
            //Project only scalars, shred remaining
            .build();
        toTable.setRelNode(node);
      }
    }
  }

  private List<RelDataTypeFieldImpl> toPk(List<UnresolvedColumn> list, int size) {
    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<RelDataTypeFieldImpl> result = new ArrayList<>();
    for (UnresolvedColumn col : list) {
      UnresolvedPhysicalColumn column = (UnresolvedPhysicalColumn) col;
      RelDataTypeFieldImpl relDataTypeField = new RelDataTypeFieldImpl(column.getName(), size++,
          factory.createFieldTypeFromLogicalType(((AtomicDataType)column.getDataType()).getLogicalType()));
      result.add(relDataTypeField);
    }

    RelDataTypeFieldImpl relDataTypeField = new RelDataTypeFieldImpl("_idx1", size++,
        factory.createFieldTypeFromLogicalType(new IntType(false)));
    RelDataTypeFieldImpl relDataTypeField2 = new RelDataTypeFieldImpl("_ingest_time", size++,
        factory.createFieldTypeFromLogicalType(new IntType(false)));
    result.add(relDataTypeField);
    result.add(relDataTypeField2);
    return result;

  }

  private StreamDataType toDataTypeField(List<RowField> c, List<RelDataTypeFieldImpl> additional) {
    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<RelDataTypeField> fields = new ArrayList<>();

    for (RowField column : c) {
      fields.add(new RelDataTypeFieldImpl(column.getName(), fields.size(), factory.createFieldTypeFromLogicalType(column.getType())));
    }
    fields.addAll(additional);

    return new StreamDataType(null, fields);
  }

  private RowType unboxArray(UnresolvedColumn col) {
    UnresolvedPhysicalColumn physicalColumn = (UnresolvedPhysicalColumn)  col;
    if (physicalColumn.getDataType() instanceof CollectionDataType) {
      CollectionDataType c = (CollectionDataType) physicalColumn.getDataType();
      FieldsDataType fields = (FieldsDataType) c.getElementDataType();
      return (RowType)fields.getLogicalType();
    }
    throw new RuntimeException("todo: unbox me");
  }


  private Table createTable(SourceTableImport sourceImport, Optional<Name> asName) {
    Map<NamePath, Column[]> outputSchema = new HashMap<>();
    Table rootTable = tableConversion(sourceImport.getSourceSchema().getFields(),outputSchema,
        asName.orElse(sourceImport.getTableName()), NamePath.ROOT, null);
    //Add shredder for each entry in outputSchema
    for (Map.Entry<NamePath, Column[]> entry : outputSchema.entrySet()) {
      Shredder.shredAtPath(outputSchema.get(entry.getKey()), entry.getKey(), rootTable);
    }

    //Set relational nodes
    for (Map.Entry<NamePath, Column[]> entry : outputSchema.entrySet()) {
      Column[] inputSchema = outputSchema.get(entry.getKey());
      assert inputSchema!=null && inputSchema.length>0;
    }
//    setParentChildRelation(rootTable);

    return rootTable;
  }

  private Table tableConversion(RelationType<FlexibleDatasetSchema.FlexibleField> relation,
      Map<NamePath, Column[]> outputSchema,
      Name name, NamePath path, Table parent) {
    NamePath namePath = getNamePath(name, Optional.ofNullable(parent));

    //Only the root table (i.e. without a parent) is visible in the schema
    Table table = new Table(tableIdCounter.incrementAndGet(), name, namePath, parent != null);
    List<Column> columns = new ArrayList<>();
    for (FlexibleDatasetSchema.FlexibleField field : relation) {
      for (Field f : fieldConversion(field, outputSchema, path, table)) {
        table.fields.add(f);
        if (f instanceof Column) columns.add((Column) f);
      }
    }
    Column ingestTime = Column.createTemp("_ingest_time", DateTimeType.INSTANCE, table, 0);
    columns.add(ingestTime);
    table.fields.add(ingestTime);
    outputSchema.put(path,columns.toArray(new Column[columns.size()]));
    return table;
  }

  private NamePath getNamePath(Name name, Optional<Table> parent) {
    NamePath namePath;
    if (parent.isPresent()) {
      namePath = parent.get().getPath().concat(name);
    } else {
      namePath = NamePath.of(name);
    }
    return namePath;
  }

  private List<Field> fieldConversion(FlexibleDatasetSchema.FlexibleField field,
      Map<NamePath, Column[]> outputSchema,
      NamePath path, Table parent) {
    List<Field> result = new ArrayList<>(field.getTypes().size());
    for (FlexibleDatasetSchema.FieldType ft : field.getTypes()) {
      result.add(fieldTypeConversion(field,ft, field.getTypes().size()>1, outputSchema, path, parent));
    }
    return result;
  }

  private Field fieldTypeConversion(FlexibleDatasetSchema.FlexibleField field, FlexibleDatasetSchema.FieldType ftype,
      final boolean isMixedType,
      Map<NamePath, Column[]> outputSchema,
      NamePath path, Table parent) {
    List<Constraint> constraints = ftype.getConstraints().stream()
        .filter(c -> {
          //Since we map mixed types onto multiple fields, not-null no longer applies
          if (c instanceof NotNull && isMixedType) return false;
          return true;
        })
        .collect(Collectors.toList());
    Name name = FlexibleSchemaHelper.getCombinedName(field,ftype);

    if (ftype.getType() instanceof RelationType) {
      Table table = tableConversion((RelationType<FlexibleDatasetSchema.FlexibleField>) ftype.getType(),
          outputSchema, name, path.concat(name), parent);
      //Add parent relationship
      Relationship parentField = new Relationship(PARENT_RELATIONSHIP, table, parent,
          Relationship.Type.PARENT, Relationship.Multiplicity.ONE
      );
      table.fields.add(parentField);
      //Return child relationship
      Relationship.Multiplicity multiplicity = Relationship.Multiplicity.MANY;
      Cardinality cardinality = ConstraintHelper.getCardinality(constraints);
      if (cardinality.isSingleton()) {
        multiplicity = Relationship.Multiplicity.ZERO_ONE;
        if (cardinality.isNonZero()) {
          multiplicity = Relationship.Multiplicity.ONE;
        }
      }
      Relationship child = new Relationship(name, parent, table,
          Relationship.Type.CHILD, multiplicity);
      return child;
    } else {
      assert ftype.getType() instanceof BasicType;
      return new Column(name, parent,0,(BasicType)ftype.getType(),ftype.getArrayDepth(), constraints, false,
          false, null, false);
    }
  }
}
