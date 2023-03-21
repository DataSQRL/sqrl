/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.table;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.io.stats.TableStatistic;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.name.ReservedName;
import com.datasqrl.plan.calcite.TypeFactory;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.plan.calcite.util.ContinuousIndexMap;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.plan.local.generate.SqrlTableNamespaceObject;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.UniversalTable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@Singleton
public class CalciteTableFactory {

  private final AtomicInteger tableIdCounter = new AtomicInteger(0);
  private final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM; //TODO: make constructor argument and configure correctly
  private final RelDataTypeFactory typeFactory;

  @Inject
  public CalciteTableFactory() {
    this.typeFactory = TypeFactory.getTypeFactory();
  }

  private Name getTableId(@NonNull Name name) {
    return getTableId(name, null);
  }

  private Name getTableId(@NonNull Name name, String type) {
    if (!StringUtils.isEmpty(type)) {
      name = name.suffix(type);
    }
    return name.suffix(Integer.toString(tableIdCounter.incrementAndGet()));
  }

  public static int getTableOrdinal(String tableId) {
    int idx = tableId.lastIndexOf(Name.NAME_DELIMITER);
    return Integer.parseInt(tableId.substring(idx + 1));
  }

  public ScriptTableDefinition importTable(TableSource tableSource, Optional<Name> tblAlias,
      ExecutionPipeline pipeline, RelBuilder relBuilder) {
    UniversalTable rootTable = tableSource.getSchema().getSchema()
        .createUniversalTable(tableSource.hasSourceTimestamp(), tblAlias);
    RelDataType rootType = convertTable(rootTable, true, true);
    ImportedRelationalTableImpl source = new ImportedRelationalTableImpl(
        getTableId(rootTable.getName(), "i"), rootType, tableSource);
    ProxyImportRelationalTable impTable = new ProxyImportRelationalTable(
        getTableId(rootTable.getName(), "q"), getTimestampHolder(rootTable),
        relBuilder.values(rootType).build(), source,
        pipeline.getStage(ExecutionEngine.Type.STREAM).get(),
        TableStatistic.of(1000));

    Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, impTable,
        Optional.empty());
    return new ScriptTableDefinition(impTable, tables);
  }

  public ScriptTableDefinition defineTable(NamePath tablePath, AnnotatedLP rel,
      List<Name> fieldNames, Optional<SQRLTable> parentTable) {
    ContinuousIndexMap selectMap = rel.getSelect();
    Preconditions.checkArgument(fieldNames.size() == selectMap.getSourceLength());

    Name tableid = getTableId(tablePath.getLast(), "q");
    TimestampHolder.Base timestamp = TimestampHolder.Base.ofDerived(rel.getTimestamp());
    TableStatistic statistic = TableStatistic.of(rel.estimateRowCount());
    QueryRelationalTable baseTable = new QueryRelationalTable(tableid, rel.getType(),
        rel.getRelNode(), rel.getPullups(), timestamp,
        rel.getPrimaryKey().getSourceLength(), statistic,
        rel.getExec().getStage());

    LinkedHashMap<Integer, Name> index2Name = new LinkedHashMap<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      index2Name.put(selectMap.map(i), fieldNames.get(i));
    }
    UniversalTable rootTable = convert2TableBuilder(tablePath, baseTable.getRowType(),
        baseTable.getNumPrimaryKeys(), index2Name);

    Optional<Pair<SQRLTable, Multiplicity>> parent = parentTable.map(pp ->
        Pair.of(pp, pp.getVt().getNumPrimaryKeys() == baseTable.getNumPrimaryKeys() ?
            Multiplicity.ZERO_ONE : Multiplicity.MANY)
    );
    Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, baseTable,
        parent);
    ScriptTableDefinition tblDef = new ScriptTableDefinition(baseTable, tables);
    //Currently, we do NOT preserve the order of the fields as originally defined by the user in the script.
    //This may not be an issue, but if we need to preserve the order, it is probably easiest to re-order the fields
    //of tblDef.getTable() based on the provided list of fieldNames
    return tblDef;
  }

  private static final Map<Name, Integer> defaultTimestampPreference = ImmutableMap.of(
      ReservedName.SOURCE_TIME, 6,
      ReservedName.INGEST_TIME, 3,
      Name.system("timestamp"), 20,
      Name.system("time"), 8);

  protected static int getTimestampScore(Name columnName) {
    return Optional.ofNullable(defaultTimestampPreference.get(columnName)).orElse(1);
  }

  public static Optional<Integer> getTimestampScore(Name columnName, RelDataType datatype) {
    if (!CalciteUtil.isTimestamp(datatype)) {
      return Optional.empty();
    }
    return Optional.of(getTimestampScore(columnName));
  }

  public TimestampHolder.Base getTimestampHolder(UniversalTable tblBuilder) {
    Preconditions.checkArgument(tblBuilder.getParent() == null,
        "Can only be invoked on root table");
    TimestampHolder.Base tsh = new TimestampHolder.Base();
    tblBuilder.getAllIndexedFields().forEach(indexField -> {
      if (indexField.getField() instanceof UniversalTable.Column) {
        UniversalTable.Column column = (UniversalTable.Column) indexField.getField();
        Optional<Integer> score = getTimestampScore(column.getName(), column.getType());
        score.ifPresent(s -> tsh.addCandidate(indexField.getIndex(), s));
      }
    });
    return tsh;
  }

  public UniversalTable convert2TableBuilder(@NonNull NamePath path,
      RelDataType type, int numPrimaryKeys,
      LinkedHashMap<Integer, Name> index2Name) {
    RelDataType2UTBConverter converter = new RelDataType2UTBConverter(typeFactory, numPrimaryKeys,
        canonicalizer);
    return converter.convert(path, type, index2Name);
  }

  public Map<SQRLTable, VirtualRelationalTable> createVirtualTables(UniversalTable rootTable,
      QueryRelationalTable baseTable,
      Optional<Pair<SQRLTable, Multiplicity>> parent) {
    return build(rootTable, new VirtualTableConstructor(baseTable), parent);
  }

  public RelDataType convertTable(UniversalTable tblBuilder, boolean includeNested,
      boolean onlyVisible) {
    return new UTB2RelDataTypeConverter().convertSchema(tblBuilder, includeNested, onlyVisible);
  }

  public class UTB2RelDataTypeConverter implements UniversalTable.TypeConverter<RelDataType>,
      UniversalTable.SchemaConverter<RelDataType> {

    @Override
    public RelDataType convertBasic(RelDataType type) {
      return type;
    }

    @Override
    public RelDataType nullable(RelDataType type, boolean nullable) {
      return typeFactory.createTypeWithNullability(type, nullable);
    }

    @Override
    public RelDataType wrapArray(RelDataType type) {

      return typeFactory.createArrayType(type, -1);
    }

    @Override
    public RelDataType nestedTable(List<Pair<String, RelDataType>> fields) {
      CalciteUtil.RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
      for (Pair<String, RelDataType> column : fields) {
        typeBuilder.add(column.getKey(), column.getRight());
      }
      ;
      return typeBuilder.build();
    }

    @Override
    public RelDataType convertSchema(UniversalTable tblBuilder) {
      return convertSchema(tblBuilder, true, true);
    }

    public RelDataType convertSchema(UniversalTable tblBuilder, boolean includeNested,
        boolean onlyVisible) {
      return nestedTable(tblBuilder.convert(this, includeNested, onlyVisible));
    }
  }

  @Value
  private final class VirtualTableConstructor {

    QueryRelationalTable baseTable;

    public VirtualRelationalTable make(@NonNull UniversalTable tblBuilder) {
      RelDataType rowType = convertTable(tblBuilder, false, false);
      return new VirtualRelationalTable.Root(getTableId(tblBuilder.getName()), rowType, baseTable);
    }

    public VirtualRelationalTable make(@NonNull UniversalTable tblBuilder,
        VirtualRelationalTable parent, Name shredFieldName) {
      RelDataType rowType = convertTable(tblBuilder, false, false);
      return VirtualRelationalTable.Child.of(getTableId(tblBuilder.getName()), rowType, parent,
          shredFieldName.getCanonical());
    }
  }

  public Map<SQRLTable, VirtualRelationalTable> build(UniversalTable builder,
      VirtualTableConstructor vtableBuilder,
      Optional<Pair<SQRLTable, Multiplicity>> parent) {
    Map<SQRLTable, VirtualRelationalTable> createdTables = new HashMap<>();
    build(builder, null, null, null, vtableBuilder, createdTables);
    if (parent.isPresent()) {
      SQRLTable root = createdTables.keySet().stream().filter(t -> t.getParent().isEmpty())
          .findFirst().get();
      SQRLTable parentTbl = parent.get().getKey();
      createChildRelationship(root.getName(), root, parentTbl, parent.get().getValue());
      createParentRelationship(root, parentTbl);
    }
    return createdTables;
  }

  private void build(UniversalTable builder, SQRLTable parent,
      VirtualRelationalTable vParent,
      UniversalTable.ChildRelationship childRel,
      VirtualTableConstructor vtableBuilder,
      Map<SQRLTable, VirtualRelationalTable> createdTables) {
    VirtualRelationalTable vTable;
    if (parent == null) {
      vTable = vtableBuilder.make(builder);
    } else {
      vTable = vtableBuilder.make(builder, vParent, childRel.getId());
    }
    SQRLTable tbl = new SQRLTable(builder.getPath());
    createdTables.put(tbl, vTable);
    if (parent != null) {
      //Add child relationship
      createChildRelationship(childRel.getName(), tbl, parent, childRel.getMultiplicity());
    }
    //Add all fields to proxy
    for (Field field : builder.getAllFields()) {
      if (field instanceof UniversalTable.Column) {
        UniversalTable.Column c = (UniversalTable.Column) field;
        tbl.addColumn(c.getName(), c.getName(), c.isVisible(), c.getType());
      } else {
        UniversalTable.ChildRelationship child = (UniversalTable.ChildRelationship) field;
        build(child.getChildTable(), tbl, vTable, child, vtableBuilder, createdTables);
      }
    }
    //Add parent relationship if not overwriting column
    if (parent != null) {
      createParentRelationship(tbl, parent);
    }
  }

  public static final Name parentRelationshipName = ReservedName.PARENT;

  public static Optional<Relationship> createParentRelationship(SQRLTable childTable,
      SQRLTable parentTable) {
    //Avoid overwriting an existing "parent" column on the child
    if (childTable.getField(parentRelationshipName).isEmpty()) {
      return Optional.of(childTable.addRelationship(parentRelationshipName, parentTable,
          Relationship.JoinType.PARENT,
          Multiplicity.ONE, Optional.empty()));
    }
    return Optional.empty();
  }


  public static Relationship createChildRelationship(Name childName, SQRLTable childTable,
      SQRLTable parentTable,
      Multiplicity multiplicity) {
    return parentTable.addRelationship(childName, childTable,
        Relationship.JoinType.CHILD, multiplicity, Optional.empty());
  }

  public NamespaceObject createTable(SqrlQueryPlanner planner, Namespace ns, NamePath namePath, AnnotatedLP processedRel,
      Optional<SQRLTable> parentTable) {
    return new SqrlTableNamespaceObject(namePath.getLast(),
        createScriptDef(planner, ns, namePath, processedRel, parentTable));
  }

  public ScriptTableDefinition createScriptDef(SqrlQueryPlanner planner, Namespace ns, NamePath namePath,
      AnnotatedLP processedRel, Optional<SQRLTable> parentTable) {
    List<String> relFieldNames = processedRel.getRelNode().getRowType().getFieldNames();
    List<Name> fieldNames = processedRel.getSelect().targetsAsList().stream()
        .map(idx -> relFieldNames.get(idx))
        .map(n -> Name.system(n)).collect(Collectors.toList());

    return defineTable(namePath, processedRel, fieldNames, parentTable);
  }
}
