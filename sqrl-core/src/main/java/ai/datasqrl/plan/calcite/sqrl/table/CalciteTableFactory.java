package ai.datasqrl.plan.calcite.sqrl.table;

import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.SqrlType2Calcite;
import ai.datasqrl.plan.calcite.sqrl.rules.Sqrl2SqlLogicalPlanConverter;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.local.ImportedTable;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ScriptTable;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class CalciteTableFactory extends VirtualTableFactory<RelDataType,VirtualSqrlTable> {

    private final AtomicInteger tableIdCounter = new AtomicInteger(0);
    private final NameCanonicalizer canonicalizer = NameCanonicalizer.SYSTEM; //TODO: make constructor argument and configure correctly
    @Getter
    private final RelDataTypeFactory typeFactory;

    public CalciteTableFactory(RelDataTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    private Name getTableId(Name name) {
        return name.suffix(Integer.toString(tableIdCounter.incrementAndGet()));
    }

    public ImportedTable importTable(ImportManager.SourceTableImport sourceTable, Optional<Name> tblAlias) {
        CalciteSchemaGenerator schemaGen = new CalciteSchemaGenerator(this);
        RelDataType rootType = new FlexibleTableConverter(sourceTable.getSchema(),tblAlias).apply(
                schemaGen).get();
        AbstractTableFactory.UniversalTableBuilder<RelDataType> rootTable = schemaGen.getRootTable();

        ImportedSqrlTable impTable = new ImportedSqrlTable(getTableId(rootTable.getName()), getTimestampHolder(rootTable),
                sourceTable, rootType);

        Map<ScriptTable,VirtualSqrlTable> tables = createVirtualTables(rootTable, impTable);
        return new ImportedTable(impTable, tables);
    }

    public QuerySqrlTable getQueryTable(Name tableName, Sqrl2SqlLogicalPlanConverter.ProcessedRel rel) {
        return new QuerySqrlTable(getTableId(tableName),rel.getType(),rel.getRelNode(),
                TimestampHolder.Base.ofDerived(rel.getTimestamp()),
                rel.getTopN(),rel.getPrimaryKey().getSourceLength());
    }

    public Map<ScriptTable,VirtualSqrlTable> createVirtualTables(UniversalTableBuilder<RelDataType> rootTable,
                                                                 QuerySqrlTable baseTable) {
        return build(rootTable, new VirtualTableConstructor(baseTable));
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
        return !datatype.isStruct() && datatype.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    @Value
    private final class VirtualTableConstructor implements VirtualTableBuilder<RelDataType,VirtualSqrlTable> {

        QuerySqrlTable baseTable;

        @Override
        public VirtualSqrlTable make(@NonNull AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder) {
            RelDataType rowType = convertTable(tblBuilder,false);
            return new VirtualSqrlTable.Root(getTableId(tblBuilder.getName()), rowType, baseTable);
        }

        @Override
        public VirtualSqrlTable make(@NonNull AbstractTableFactory.UniversalTableBuilder<RelDataType> tblBuilder, VirtualSqrlTable parent, Name shredFieldName) {
            RelDataType rowType = convertTable(tblBuilder,false);
            return VirtualSqrlTable.Child.of(getTableId(tblBuilder.getName()),rowType,parent,shredFieldName.getCanonical());
        }
    }

    public UniversalTableBuilder<RelDataType> convert2TableBuilder(@NonNull NamePath path,
                                                                   RelDataType type, int numPrimaryKeys) {
        return createBuilder(path, null, type, numPrimaryKeys, new CalciteTypeIntrospector());
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
