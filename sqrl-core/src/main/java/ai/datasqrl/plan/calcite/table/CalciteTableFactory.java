package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.SqrlType2Calcite;
import ai.datasqrl.plan.calcite.rules.AnnotatedLP;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.ContinuousIndexMap;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import ai.datasqrl.schema.builder.AbstractTableFactory;
import ai.datasqrl.schema.builder.NestedTableBuilder;
import ai.datasqrl.schema.builder.VirtualTableFactory;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.SqrlTypeConverter;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.h2.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class CalciteTableFactory extends VirtualTableFactory<RelDataType, VirtualRelationalTable> {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    private final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM; //TODO: make constructor argument and configure correctly
    @Getter
    private final RelDataTypeFactory typeFactory;

    public CalciteTableFactory(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    private Name getTableId(@NonNull Name name) {
        return getTableId(name,null);
    }

    private Name getTableId(@NonNull Name name, String type) {
        if (!StringUtils.isNullOrEmpty(type)) name = name.suffix(type);
        return name.suffix(Integer.toString(tableIdCounter.incrementAndGet()));
    }

    public static int getTableOrdinal(String tableId) {
        int idx = tableId.lastIndexOf(Name.NAME_DELIMITER);
        return Integer.parseInt(tableId.substring(idx+1,tableId.length()));
    }

    public ScriptTableDefinition importTable(ImportManager.SourceTableImport sourceTable, Optional<Name> tblAlias, RelBuilder relBuilder,
                                             ExecutionPipeline pipeline) {
        CalciteSchemaGenerator schemaGen = new CalciteSchemaGenerator(this);
        RelDataType rootType = new FlexibleTableConverter(sourceTable.getSchema(),tblAlias).apply(
                schemaGen).get();
        AbstractTableFactory.UniversalTableBuilder<RelDataType> rootTable = schemaGen.getRootTable();
        ImportedSourceTable source = new ImportedSourceTable(getTableId(rootTable.getName(),"i"),rootType,sourceTable);
        ProxyImportRelationalTable impTable = new ProxyImportRelationalTable(getTableId(rootTable.getName(),"q"), getTimestampHolder(rootTable),
                relBuilder.values(rootType).build(), source, pipeline.getStage(ExecutionEngine.Type.STREAM).get());

        Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, impTable, Optional.empty());
        return new ScriptTableDefinition(impTable, tables);
    }

    public ScriptTableDefinition defineTable(NamePath tablePath, AnnotatedLP rel,
                                      List<Name> fieldNames, Optional<Pair<SQRLTable,VirtualRelationalTable>> parentPair) {
        ContinuousIndexMap selectMap = rel.getSelect();
        Preconditions.checkArgument(fieldNames.size()==selectMap.getSourceLength());

        Name tableid = getTableId(tablePath.getLast(),"q");
        TimestampHolder.Base timestamp = TimestampHolder.Base.ofDerived(rel.getTimestamp());
        TableStatistic statistic = TableStatistic.of(rel.estimateRowCount());
        QueryRelationalTable baseTable = new QueryRelationalTable(tableid, rel.getType(),
                rel.getRelNode(), rel.getPullups(), timestamp,
                rel.getPrimaryKey().getSourceLength(), statistic,
                rel.getExec().getStage());

        LinkedHashMap<Integer,Name> index2Name = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            index2Name.put(selectMap.map(i), fieldNames.get(i));
        }
        AbstractTableFactory.UniversalTableBuilder<RelDataType> rootTable = convert2TableBuilder(tablePath, baseTable.getRowType(),
                baseTable.getNumPrimaryKeys(), index2Name);
        Optional<Pair<SQRLTable, Relationship.Multiplicity>> parent = parentPair.map(pp ->
            Pair.of(pp.getLeft(), pp.getRight().getNumPrimaryKeys()==baseTable.getNumPrimaryKeys()?
                    Relationship.Multiplicity.ZERO_ONE: Relationship.Multiplicity.MANY)
        );
        Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, baseTable, parent);
        ScriptTableDefinition tblDef = new ScriptTableDefinition(baseTable, tables);
        //Currently, we do NOT preserve the order of the fields as originally defined by the user in the script.
        //This may not be an issue, but if we need to preserve the order, it is probably easiest to re-order the fields
        //of tblDef.getTable() based on the provided list of fieldNames
        return tblDef;
    }

    public Map<SQRLTable, VirtualRelationalTable> createVirtualTables(UniversalTableBuilder<RelDataType> rootTable,
                                                                      QueryRelationalTable baseTable,
                                                                      Optional<Pair<SQRLTable, Relationship.Multiplicity>> parent) {
        return build(rootTable, new VirtualTableConstructor(baseTable),parent);
    }

    public RelDataType convertTable(AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder, boolean forNested) {
        CalciteUtil.RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
        List<NestedTableBuilder.Column<RelDataType>> columns = tblBuilder.getColumns(forNested,forNested);
        for (NestedTableBuilder.Column<RelDataType> column : columns) {
            typeBuilder.add(column.getId(), column.getType(), column.isNullable());
        };
        return typeBuilder.build();
    }

    @Override
    protected boolean isTimestamp(RelDataType datatype) {
        return CalciteUtil.isTimestamp(datatype);
    }

    @Value
    private final class VirtualTableConstructor implements VirtualTableBuilder<RelDataType, VirtualRelationalTable> {

        QueryRelationalTable baseTable;

        @Override
        public VirtualRelationalTable make(@NonNull AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder) {
            RelDataType rowType = convertTable(tblBuilder,false);
            return new VirtualRelationalTable.Root(getTableId(tblBuilder.getName()), rowType, baseTable);
        }

        @Override
        public VirtualRelationalTable make(@NonNull AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder, VirtualRelationalTable parent, Name shredFieldName) {
            RelDataType rowType = convertTable(tblBuilder,false);
            return VirtualRelationalTable.Child.of(getTableId(tblBuilder.getName()),rowType,parent,shredFieldName.getCanonical());
        }
    }

    public UniversalTableBuilder<RelDataType> convert2TableBuilder(@NonNull NamePath path,
                                                                   RelDataType type, int numPrimaryKeys,
                                                                   LinkedHashMap<Integer,Name> index2Name) {
        return createBuilder(path, null, type, numPrimaryKeys, index2Name,
                new CalciteTypeIntrospector());
    }

    public class CalciteTypeIntrospector implements TypeIntrospector<RelDataType,RelDataTypeField> {

        @Override
        public Iterable<RelDataTypeField> getFields(RelDataType relDataType) {
            Preconditions.checkArgument(relDataType.isStruct(),"Not a table: %s",relDataType);
            return relDataType.getFieldList();
        }

        @Override
        public Name getName(RelDataTypeField field) {
            return Name.of(field.getName(),canonicalizer);
        }

        @Override
        public RelDataType getType(RelDataTypeField field) {
            return field.getType();
        }

        @Override
        public boolean isNullable(RelDataTypeField field) {
            return field.getType().isNullable();
        }

        @Override
        public Optional<Pair<RelDataType, Relationship.Multiplicity>> getNested(RelDataTypeField field) {
            if (CalciteUtil.isNestedTable(field.getType())) {
                Optional<RelDataType> componentType = CalciteUtil.getArrayElementType(field.getType());
                RelDataType nestedType = componentType.orElse(field.getType());
                Relationship.Multiplicity multi = Relationship.Multiplicity.ZERO_ONE;
                if (componentType.isPresent()) multi = Relationship.Multiplicity.MANY;
                else if (!nestedType.isNullable()) multi = Relationship.Multiplicity.ONE;
                return Optional.of(Pair.of(nestedType,multi));
            } else {
                return Optional.empty();
            }

        }

        @Override
        public SqrlTypeConverter<RelDataType> getTypeConverter() {
            return new SqrlType2Calcite(typeFactory);
        }
    }



}
