package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.io.sources.stats.TableStatistic;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.plan.calcite.SqrlTypeRelDataTypeConverter;
import ai.datasqrl.plan.calcite.rules.AnnotatedLP;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.calcite.util.ContinuousIndexMap;
import ai.datasqrl.plan.local.ScriptTableDefinition;
import ai.datasqrl.schema.*;
import ai.datasqrl.schema.input.FlexibleTable2UTBConverter;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CalciteTableFactory {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    private final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM; //TODO: make constructor argument and configure correctly
    @Getter
    private final RelDataTypeFactory typeFactory;
    private final SqrlTypeRelDataTypeConverter typeConverter;

    public CalciteTableFactory(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
        this.typeConverter = new SqrlTypeRelDataTypeConverter(typeFactory);
    }

    private Name getTableId(@NonNull Name name) {
        return getTableId(name,null);
    }

    private Name getTableId(@NonNull Name name, String type) {
        if (!StringUtils.isEmpty(type)) name = name.suffix(type);
        return name.suffix(Integer.toString(tableIdCounter.incrementAndGet()));
    }

    public static int getTableOrdinal(String tableId) {
        int idx = tableId.lastIndexOf(Name.NAME_DELIMITER);
        return Integer.parseInt(tableId.substring(idx+1));
    }

    public ScriptTableDefinition importTable(TableSource tableSource, Optional<Name> tblAlias, RelBuilder relBuilder,
                                             ExecutionPipeline pipeline) {
        FlexibleTable2UTBConverter converter = new FlexibleTable2UTBConverter(typeFactory);
        UniversalTableBuilder rootTable = new FlexibleTableConverter(tableSource.getSchema(),tblAlias).apply(
                converter);
        RelDataType rootType = convertTable(rootTable, true, true);
        ImportedRelationalTable source = new ImportedRelationalTable(getTableId(rootTable.getName(),"i"),rootType, tableSource);
        ProxyImportRelationalTable impTable = new ProxyImportRelationalTable(getTableId(rootTable.getName(),"q"), getTimestampHolder(rootTable),
                relBuilder.values(rootType).build(), source, pipeline.getStage(ExecutionEngine.Type.STREAM).get(), tableSource.getStatistic());

        Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, impTable, Optional.empty());
        return new ScriptTableDefinition(impTable, tables);
    }

    public ScriptTableDefinition defineStreamTable(NamePath tablePath, AnnotatedLP baseRel, StateChangeType changeType,
                                                   RelBuilder relBuilder, ExecutionPipeline pipeline) {
        Preconditions.checkArgument(baseRel.type!=TableType.STREAM,"Underlying table is already a stream");
        Name tableName = tablePath.getLast();

        UniversalTableBuilder rootTable = convertStream2TableBuilder(tablePath,
                baseRel.getRelNode().getRowType());
        RelDataType rootType = convertTable(rootTable, true, true);
        StreamRelationalTable source = new StreamRelationalTable(getTableId(tableName,"s"), baseRel.getRelNode(), rootType, rootTable, changeType);
        TableStatistic statistic = TableStatistic.of(baseRel.estimateRowCount());
        ProxyStreamRelationalTable impTable = new ProxyStreamRelationalTable(getTableId(tableName,"q"), getTimestampHolder(rootTable),
                relBuilder.values(rootType).build(), source, pipeline.getStage(ExecutionEngine.Type.STREAM).get(), statistic);

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
        UniversalTableBuilder rootTable = convert2TableBuilder(tablePath, baseTable.getRowType(),
                baseTable.getNumPrimaryKeys(), index2Name);


        Optional<Pair<SQRLTable, Multiplicity>> parent = parentPair.map(pp ->
            Pair.of(pp.getLeft(), pp.getRight().getNumPrimaryKeys()==baseTable.getNumPrimaryKeys()?
                    Multiplicity.ZERO_ONE: Multiplicity.MANY)
        );
        Map<SQRLTable, VirtualRelationalTable> tables = createVirtualTables(rootTable, baseTable, parent);
        ScriptTableDefinition tblDef = new ScriptTableDefinition(baseTable, tables);
        //Currently, we do NOT preserve the order of the fields as originally defined by the user in the script.
        //This may not be an issue, but if we need to preserve the order, it is probably easiest to re-order the fields
        //of tblDef.getTable() based on the provided list of fieldNames
        return tblDef;
    }

    private final Map<Name,Integer> defaultTimestampPreference = ImmutableMap.of(
            ReservedName.SOURCE_TIME, 6,
            ReservedName.INGEST_TIME, 3,
            Name.system("timestamp"), 20,
            Name.system("time"), 8);

    protected int getTimestampScore(Name columnName) {
        return Optional.ofNullable(defaultTimestampPreference.get(columnName)).orElse(1);
    }

    public Optional<Integer> getTimestampScore(Name columnName, RelDataType datatype) {
        if (!CalciteUtil.isTimestamp(datatype)) return Optional.empty();
        return Optional.of(getTimestampScore(columnName));
    }

    public TimestampHolder.Base getTimestampHolder(UniversalTableBuilder tblBuilder) {
        Preconditions.checkArgument(tblBuilder.getParent()==null,"Can only be invoked on root table");
        TimestampHolder.Base tsh = new TimestampHolder.Base();
        tblBuilder.getAllIndexedFields().forEach(indexField -> {
            if (indexField.getField() instanceof UniversalTableBuilder.Column) {
                UniversalTableBuilder.Column column = (UniversalTableBuilder.Column) indexField.getField();
                Optional<Integer> score = getTimestampScore(column.getName(),column.getType());
                score.ifPresent(s -> tsh.addCandidate(indexField.getIndex(), s));
            }
        });
        return tsh;
    }

    public UniversalTableBuilder convert2TableBuilder(@NonNull NamePath path,
                                                                   RelDataType type, int numPrimaryKeys,
                                                                   LinkedHashMap<Integer,Name> index2Name) {
        RelDataType2UTBConverter converter = new RelDataType2UTBConverter(typeFactory,numPrimaryKeys,canonicalizer);
        return converter.convert(path,type,index2Name);
    }

    public UniversalTableBuilder convertStream2TableBuilder(@NonNull NamePath path, RelDataType type) {
        RelDataType2UTBConverter converter = new RelDataType2UTBConverter(
                new UniversalTableBuilder.ImportFactory(typeFactory,false),canonicalizer);
        return converter.convert(path,type,null);
    }

    public Map<SQRLTable, VirtualRelationalTable> createVirtualTables(UniversalTableBuilder rootTable,
                                                                      QueryRelationalTable baseTable,
                                                                      Optional<Pair<SQRLTable, Multiplicity>> parent) {
        return build(rootTable, new VirtualTableConstructor(baseTable),parent);
    }

    public RelDataType convertTable(UniversalTableBuilder tblBuilder, boolean includeNested, boolean onlyVisible) {
        return new UTB2RelDataTypeConverter().convertSchema(tblBuilder, includeNested, onlyVisible);
    }

    public class UTB2RelDataTypeConverter implements UniversalTableBuilder.TypeConverter<RelDataType>, UniversalTableBuilder.SchemaConverter<RelDataType> {

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
            return typeFactory.createArrayType(type,-1L);
        }

        @Override
        public RelDataType nestedTable(List<Pair<String, RelDataType>> fields) {
            CalciteUtil.RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
            for (Pair<String, RelDataType> column : fields) {
                typeBuilder.add(column.getKey(), column.getRight());
            };
            return typeBuilder.build();
        }

        @Override
        public RelDataType convertSchema(UniversalTableBuilder tblBuilder) {
            return convertSchema(tblBuilder, true, true);
        }

        public RelDataType convertSchema(UniversalTableBuilder tblBuilder, boolean includeNested, boolean onlyVisible) {
            return nestedTable(tblBuilder.convert(this,includeNested, onlyVisible));
        }
    }

    @Value
    private final class VirtualTableConstructor {

        QueryRelationalTable baseTable;

        public VirtualRelationalTable make(@NonNull UniversalTableBuilder tblBuilder) {
            RelDataType rowType = convertTable(tblBuilder,false, false);
            return new VirtualRelationalTable.Root(getTableId(tblBuilder.getName()), rowType, baseTable);
        }

        public VirtualRelationalTable make(@NonNull UniversalTableBuilder tblBuilder, VirtualRelationalTable parent, Name shredFieldName) {
            RelDataType rowType = convertTable(tblBuilder,false, false);
            return VirtualRelationalTable.Child.of(getTableId(tblBuilder.getName()),rowType,parent,shredFieldName.getCanonical());
        }
    }

    public Map<SQRLTable, VirtualRelationalTable> build(UniversalTableBuilder builder, VirtualTableConstructor vtableBuilder,
                                                        Optional<Pair<SQRLTable, Multiplicity>> parent) {
        Map<SQRLTable,VirtualRelationalTable> createdTables = new HashMap<>();
        build(builder,null,null,null,vtableBuilder,createdTables);
        if (parent.isPresent()) {
            SQRLTable root = createdTables.keySet().stream().filter(t -> t.getParent().isEmpty()).findFirst().get();
            SQRLTable parentTbl = parent.get().getKey();
            createChildRelationship(root.getName(), root, parentTbl, parent.get().getValue());
            createParentRelationship(root, parentTbl);
        }
        return createdTables;
    }

    private void build(UniversalTableBuilder builder, SQRLTable parent, VirtualRelationalTable vParent,
                       UniversalTableBuilder.ChildRelationship childRel,
                       VirtualTableConstructor vtableBuilder,
                       Map<SQRLTable,VirtualRelationalTable> createdTables) {
        VirtualRelationalTable vTable;
        if (parent==null) vTable = vtableBuilder.make(builder);
        else vTable = vtableBuilder.make(builder,vParent,childRel.getId());
        SQRLTable tbl = new SQRLTable(builder.getPath());
        createdTables.put(tbl,vTable);
        if (parent!=null) {
            //Add child relationship
            createChildRelationship(childRel.getName(), tbl, parent, childRel.getMultiplicity());
        }
        //Add all fields to proxy
        for (Field field : builder.getAllFields()) {
            if (field instanceof UniversalTableBuilder.Column) {
                UniversalTableBuilder.Column c = (UniversalTableBuilder.Column)field;
                tbl.addColumn(c.getName(), c.getName(), c.isVisible(), c.getType());
            } else {
                UniversalTableBuilder.ChildRelationship child = (UniversalTableBuilder.ChildRelationship)field;
                build(child.getChildTable(),tbl,vTable,child,vtableBuilder,createdTables);
            }
        }
        //Add parent relationship if not overwriting column
        if (parent!=null) {
            createParentRelationship(tbl, parent);
        }
    }

    public static final Name parentRelationshipName = ReservedName.PARENT;

    public static Optional<Relationship> createParentRelationship(SQRLTable childTable, SQRLTable parentTable) {
        //Avoid overwriting an existing "parent" column on the child
        if (childTable.getField(parentRelationshipName).isEmpty()) {
            return Optional.of(childTable.addRelationship(parentRelationshipName, parentTable, Relationship.JoinType.PARENT,
                    Multiplicity.ONE, Optional.empty()));
        }
        return Optional.empty();
    }


    public static Relationship createChildRelationship(Name childName, SQRLTable childTable, SQRLTable parentTable,
                                                       Multiplicity multiplicity) {
        return parentTable.addRelationship(childName, childTable,
                Relationship.JoinType.CHILD, multiplicity, Optional.empty());
    }


}
