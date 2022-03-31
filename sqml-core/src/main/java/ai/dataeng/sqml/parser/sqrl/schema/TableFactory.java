package ai.dataeng.sqml.parser.sqrl.schema;

import static ai.dataeng.sqml.parser.macros.SqlNodeUtils.childParentJoin;
import static ai.dataeng.sqml.parser.macros.SqlNodeUtils.parentChildJoin;
import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Type;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import ai.dataeng.sqml.parser.operator.Shredder;
import ai.dataeng.sqml.parser.sqrl.calcite.CalcitePlanner;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;

@AllArgsConstructor
public class TableFactory {
  CalcitePlanner planner;
  public final static AtomicInteger tableIdCounter = new AtomicInteger(0);

  public Table create(SourceTableImport sourceTableImport, Optional<Name> alias) {
    Table table = createTable(sourceTableImport, alias);

    FlinkTableConverter tbConverter = new FlinkTableConverter();
    Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(sourceTableImport.getSourceSchema());

    planner.addTable(table.getId().toString(), new StreamTable(ordersSchema.getKey()));

    RelNode node = planner.createRelBuilder().scanStream(table.getId().toString(), sourceTableImport).build();
    table.setRelNode(node);
    return table;
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

  private void setParentChildRelation(Table table) {
    for (Field field : table.getFields()) {
      //RelNode added after added to table
      if (field instanceof Relationship) {
        Relationship col = (Relationship) field;
        if (col.getType() == Type.CHILD) {
          Pair<Map<Column, String>, SqlNode> child = parentChildJoin(col);
          col.setSqlNode(child.getRight());
          col.setPkNameMapping(child.getLeft());
          setParentChildRelation(col.toTable);
        } else if (col.getType() == Type.PARENT) {
          Pair<Map<Column, String>, SqlNode> child = childParentJoin(col);
          col.setSqlNode(child.getRight());
          col.setPkNameMapping(child.getLeft());
        }
      }
    }
  }

  private NamePath getNamePath(Name name, Optional<Table> parent) {
    NamePath namePath;
    if (parent.isPresent()) {
      namePath = parent.get().getPath().resolve(name);
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
          outputSchema, name, path.resolve(name), parent);
      //Add parent relationship
      Relationship parentField = new Relationship(PARENT_RELATIONSHIP, table, parent,
          Relationship.Type.PARENT, Relationship.Multiplicity.ONE, null
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
          Relationship.Type.CHILD, multiplicity, null);
      return child;
    } else {
      assert ftype.getType() instanceof BasicType;
      return new Column(name, parent,0,(BasicType)ftype.getType(),ftype.getArrayDepth(), constraints, false,
          false, null, false);
    }
  }

  public Table create(NamePath name, Query node) {
    return null;
  }

  public Table create(NamePath namePath, Name table) {
    //Check if this is in the logical dag

    return null;
  }
}
