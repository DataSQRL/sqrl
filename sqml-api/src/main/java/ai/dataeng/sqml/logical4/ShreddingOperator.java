package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.physical.flink.FieldProjection;
import ai.dataeng.sqml.physical.flink.RecordShredderFlatMap;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import static ai.dataeng.sqml.schema2.TypeHelper.getNestedType;

public class ShreddingOperator extends LogicalPlan.RowNode {

    final LogicalPlan.DocumentNode input;
    final NamePath tableIdentifier;
    final FieldProjection[] projections;
    final LogicalPlan.Column[] outputSchema;

    private ShreddingOperator(LogicalPlan.DocumentNode input, NamePath tableIdentifier,
                             FieldProjection[] projections, LogicalPlan.Column[] outputSchema) {
        assert projections.length == outputSchema.length;
        this.input = input;
        this.tableIdentifier = tableIdentifier;
        this.projections = projections;
        this.outputSchema = outputSchema;
    }

    List<LogicalPlan.RowNode> getConsumers() {
        return (List)consumers;
    }

    @Override
    List<LogicalPlan.DocumentNode> getInputs() {
        return List.of(input);
    }

    @Override
    LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[][]{outputSchema};
    }

    public RecordShredderFlatMap getFlinkProcess() {
        return new RecordShredderFlatMap(tableIdentifier,projections);
    }

    public static ShreddingOperator shredAtPath(SourceOperator source, NamePath tableIdentifier, LogicalPlan.Table rootTable) {
        int maxdepth = tableIdentifier.getLength();
        List<FieldProjection> projections = new ArrayList<>();
        List<LogicalPlan.Column> outputSchema = new ArrayList<>();

        LogicalPlan.Column[] inputSchema = source.outputSchema.get(tableIdentifier);
        assert inputSchema!=null && inputSchema.length>0;

        LogicalPlan.Table targetTable = inputSchema[0].table;

        LogicalPlan.Table currentTable = rootTable;
        for (int depth = 0; depth <= maxdepth; depth++) {
            FieldProjection.SpecialCase fp;
            if (depth==0) {
                fp = FieldProjection.ROOT_UUID;
            } else {
                Name child = tableIdentifier.get(depth-1);
                LogicalPlan.Relationship childRel = LogicalPlanUtil.getChildRelationship(currentTable,child);
                if (childRel.multiplicity == LogicalPlan.Relationship.Multiplicity.MANY) {
                    fp = new FieldProjection.ArrayIndex(depth);
                } else {
                    fp = null;
                }
                currentTable = childRel.toTable;
            }
            if (fp != null) {
                projections.add(fp);
                LogicalPlan.Column addColumn = fp.createColumn(targetTable);
                if (targetTable.fields.add(addColumn))
                    throw new IllegalArgumentException(String.format("[%s] is a reserved keyword and should not be a data field", addColumn.name));
                outputSchema.add(addColumn);
            }
        }
        assert targetTable.equals(currentTable);

        //Add all the column to output as per source
        for (LogicalPlan.Column col : inputSchema) {
            outputSchema.add(col);
            projections.add(new FieldProjection.NamePathProjection(NamePath.of(col.name),maxdepth));
        }
        return new ShreddingOperator(source, tableIdentifier,
                projections.toArray(new FieldProjection[0]),
                outputSchema.toArray(new LogicalPlan.Column[0]));
    }

}
