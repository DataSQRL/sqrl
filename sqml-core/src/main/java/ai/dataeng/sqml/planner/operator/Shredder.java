package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.execution.flink.process.FieldProjection;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanUtil;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;

/**
 * Shreds incoming document records into table rows based on the provided {@link Shredder#projections}
 * at a level within the document tree identified by {@link Shredder#tableIdentifier}.
 */
@Value
public class Shredder {

    public static void shredAtPath(Column[] inputSchema, NamePath tableIdentifier, Table rootTable) {
        int maxdepth = tableIdentifier.getLength();
        List<FieldProjection> projections = new ArrayList<>();
        List<Column> outputSchema = new ArrayList<>();

        assert inputSchema!=null && inputSchema.length>0;

        Table targetTable = LogicalPlanUtil.getTable(inputSchema);

        Table currentTable = rootTable;
        for (int depth = 0; depth <= maxdepth; depth++) {
            FieldProjection.SpecialCase fp;
            if (depth==0) {
                if (targetTable != rootTable) {
                    fp = FieldProjection.foreignKey(rootTable.getPrimaryKeys().get(0));
                } else {
                    fp = FieldProjection.ROOT_UUID;
                }
            } else {
                Name child = tableIdentifier.get(depth-1);
                Relationship childRel = LogicalPlanUtil.getChildRelationship(currentTable,child);
                if (childRel.multiplicity == Relationship.Multiplicity.MANY) {
                    if (depth > 1) {
                        Table table = currentTable;
                        int i = depth;
                        while (i-- != 0) {
                            table = table.getParent().get();
                        }

                        fp = new FieldProjection.ArrayIndex(depth, Optional.of((Column) table.getField(Name.system("idx"+depth))));
                    } else {
                        fp = new FieldProjection.ArrayIndex(depth, Optional.empty());
                    }
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
    }

}
