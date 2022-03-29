package ai.dataeng.sqml.parser.operator;

import static ai.dataeng.sqml.parser.macros.SqlNodeUtils.childParentJoin;
import static ai.dataeng.sqml.parser.macros.SqlNodeUtils.parentChildJoin;
import static ai.dataeng.sqml.tree.name.Name.PARENT_RELATIONSHIP;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.parser.CalciteTools;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Dataset;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Type;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.nodes.StreamTableScan;
import ai.dataeng.sqml.schema.Namespace;
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
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.AtomicDataType;

/**
 * Resolve imports in SQRL scripts
 */
public class ImportResolver {
    private final ImportManager importManager;
    private final ErrorCollector errors;

    public ImportResolver(ImportManager importManager) {
        this(importManager, ErrorCollector.root());
    }
    public ImportResolver(ImportManager importManager,
                          ErrorCollector errors) {
        this.importManager = importManager;
        this.errors = errors;
    }

    public ImportManager getImportManager() {
        return importManager;
    }

    public void resolveImport(ImportMode importMode, Name datasetName,
        Optional<Name> tableName, Optional<Name> asName,
        Namespace namespace) {
//
//        ErrorCollector schemaErrors = ErrorCollector.root();
//        if (importMode==ImportMode.DATASET || importMode==ImportMode.ALLTABLE) {
//            List<ImportManager.TableImport> tblimports = importManager.importAllTables(datasetName, schemaErrors);
//
//            List<Table> tables = new ArrayList<>();
//            for (ImportManager.TableImport tblimport : tblimports) {
//                Table table = createTable(namespace, tblimport, Optional.empty());
//                tables.add(table);
//            }
//            Dataset ds = new Dataset(asName.orElse(datasetName), tables);
//            if (importMode == ImportMode.ALLTABLE) {
//                namespace.addDataset(ds);
//            } else {
//                namespace.scope(ds);
//            }
//        } else {
//            assert importMode == ImportMode.TABLE;
//            SourceTableImport tblimport = importManager.importTable(datasetName, tableName.get(), schemaErrors);
//            Table table = createTable(namespace, tblimport, asName);
//            List<Table> tables = new ArrayList<>();
//            tables.add(table);
//            Dataset ds = new Dataset(asName.orElse(datasetName), tables);
//            namespace.addDataset(ds);
//            table.setRelNode(createRelNode(tblimport, namespace));
//        }
//        errors.addAll(schemaErrors);
    }

    //The relation is:
    // a StreamTableScan. We unwrap it later.
    private RelNode createRelNode(SourceTableImport tblimport, Namespace namespace) {
        SqrlTypeFactory typeFactory = new SqrlTypeFactory();
        RelOptCluster cluster = CalciteTools.createHepCluster(typeFactory);
        CalciteCatalogReader catalog = CalciteTools.getCalciteCatalogReader(Optional.empty(), namespace, typeFactory);
        RelOptTable relOptTable = catalog.getTableForMember(List.of(tblimport.getTableName().getCanonical()));
        return new StreamTableScan(cluster, RelTraitSet.createEmpty(), List.of(), relOptTable, tblimport);
    }
//
//    private List<RexNode> projectFields(SourceTableImport tblimport,
//        RexBuilder rexBuilder, RelDataType rowType) {
//
//        FlinkTableConverter converter = new FlinkTableConverter();
//        Pair<Schema, TypeInformation> schema = converter.tableSchemaConversion(tblimport.getSourceSchema());
//
//        List<RexNode> projects = new ArrayList<>();
//        List<UnresolvedColumn> columns = schema.getKey().getColumns();
//        for (int i = 0, columnsSize = columns.size(); i < columnsSize; i++) {
//            UnresolvedColumn column = columns.get(i);
//            UnresolvedPhysicalColumn physicalColumn = (UnresolvedPhysicalColumn) column;
//            AbstractDataType dataType = physicalColumn.getDataType();
//            if (dataType instanceof AtomicDataType) {
//                projects.add(rexBuilder.makeInputRef(rowType, i));
//            }
//        }
//        return projects;
//    }


    public enum ImportMode {
        DATASET, TABLE, ALLTABLE
    }
}
