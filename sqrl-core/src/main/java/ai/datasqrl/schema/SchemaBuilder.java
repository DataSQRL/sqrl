package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.ImportTable;
import ai.datasqrl.schema.operations.AddDatasetOp;
import ai.datasqrl.schema.operations.OperationVisitor;
import ai.datasqrl.schema.operations.SqrlOperation;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

public class SchemaBuilder extends OperationVisitor {
  @Getter
  ShadowingContainer<Table> schema = new ShadowingContainer<>();

  public void apply(SqrlOperation operation) {
    operation.accept(this);
  }

  @Override
  public <T> T visit(AddDatasetOp op) {
    for (ImportTable entry :
        op.getResult().getImportedPaths()) {
      Table table = createTable(entry.getTableName(), entry.getRelNode(),
          entry.getFields());
      schema.add(table);
    }

    return null;
  }

  /**
   * TODO: move to table factory
   */
  private Table createTable(NamePath name, RelNode relNode,
      List<Name> fields) {
    Table table = new Table(SourceTablePlanner.tableIdCounter.incrementAndGet(),
        name.getFirst(), name.getFirst().toNamePath(),
        false, relNode);
    for (Name n : fields) {
      table.addField(Column.createTemp(n, null, table, 0));
    }

    return table;
  }

  public Object build() {
    return null;
  }
}
