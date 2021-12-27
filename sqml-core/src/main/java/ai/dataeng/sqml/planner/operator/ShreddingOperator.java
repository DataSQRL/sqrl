package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.execution.flink.process.FieldProjection;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanUtil;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

/**
 * Shreds incoming document records into table rows based on the provided {@link ShreddingOperator#projections}
 * at a level within the document tree identified by {@link ShreddingOperator#tableIdentifier}.
 */
@Value
public class ShreddingOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.DocumentNode> {

    final NamePath tableIdentifier;
    final FieldProjection[] projections;
    final Column[] outputSchema;

    public ShreddingOperator(LogicalPlanImpl.DocumentNode input, NamePath tableIdentifier,
                             FieldProjection[] projections, Column[] outputSchema) {
        super(input);
        assert projections.length == outputSchema.length;
        this.tableIdentifier = tableIdentifier;
        this.projections = projections;
        this.outputSchema = outputSchema;
    }

    public List<LogicalPlanImpl.RowNode> getConsumers() {
        return (List)consumers;
    }

    @Override
    public Column[][] getOutputSchema() {
        return new Column[][]{outputSchema};
    }

    public static ShreddingOperator shredAtPath(DocumentSource source, NamePath tableIdentifier, Table rootTable) {
        int maxdepth = tableIdentifier.getLength();
        List<FieldProjection> projections = new ArrayList<>();
        List<Column> outputSchema = new ArrayList<>();

        Column[] inputSchema = source.getOutputSchema().get(tableIdentifier);
        assert inputSchema!=null && inputSchema.length>0;

        Table targetTable = LogicalPlanUtil.getTable(inputSchema);

        Table currentTable = rootTable;
        for (int depth = 0; depth <= maxdepth; depth++) {
            FieldProjection.SpecialCase fp;
            if (depth==0) {
                fp = FieldProjection.ROOT_UUID;
            } else {
                Name child = tableIdentifier.get(depth-1);
                Relationship childRel = LogicalPlanUtil.getChildRelationship(currentTable,child);
                if (childRel.multiplicity == Relationship.Multiplicity.MANY) {
                    fp = new FieldProjection.ArrayIndex(depth);
                } else {
                    fp = null;
                }
                currentTable = childRel.toTable;
            }
            if (fp != null) {
                projections.add(fp);
                Column addColumn = fp.createColumn(targetTable);
                if (targetTable.fields.add(addColumn))
                    throw new IllegalArgumentException(String.format("[%s] is a reserved keyword and should not be a data field", addColumn.name));
                outputSchema.add(addColumn);
            }
        }
        assert targetTable.equals(currentTable);

        //Add all the column to output as per source
        for (Column col : inputSchema) {
            outputSchema.add(col);
            projections.add(new FieldProjection.NamePathProjection(NamePath.of(col.name),maxdepth));
        }
        ShreddingOperator shredder = new ShreddingOperator(source, tableIdentifier,
                projections.toArray(new FieldProjection[0]),
                outputSchema.toArray(new Column[0]));

        //Connect with source node in the DAG
        source.addConsumer(shredder);
        //Point the table at the shredder as most current
        targetTable.updateNode(shredder);
        return shredder;
    }

}
