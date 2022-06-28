package ai.datasqrl.plan.calcite.memory;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.inmemory.InMemStreamEngine;
import ai.datasqrl.plan.calcite.BasicSqrlCalciteBridge;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.Rules;
import ai.datasqrl.plan.calcite.Rules.Stage;
import ai.datasqrl.plan.calcite.memory.table.DataTable;
import ai.datasqrl.plan.local.operations.AddColumnOp;
import ai.datasqrl.plan.local.operations.AddNestedTableOp;
import ai.datasqrl.plan.local.operations.AddRootTableOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelBuilder;

/**
 * The {@link EnumerableCalciteBridge} plans and immediately executes all {@link ai.datasqrl.plan.local.operations.SchemaUpdateOp}
 * that it visits. It stores the results of that execution as a {@link DataTable} inside the {@link InMemoryCalciteSchema}
 * that it maintains separately from tableMap of the parent class, because that schema is used to initiate the
 * {@link LocalDataContext} required for the enumerable execution by Calcite.
 *
 * This bridge implementation is used primarily for testing and in-memory execution since it eagerly materializes
 * all defined SQRL tables and stores the results in memory.
 *
 */
public class EnumerableCalciteBridge extends BasicSqrlCalciteBridge {

  private final StreamEngine streamEngine;
  private final StreamInputPreparer streamPreparer;

  @Getter
  private final InMemoryCalciteSchema inMemorySchema;

  public EnumerableCalciteBridge(Planner planner, StreamEngine streamEngine, StreamInputPreparer streamPreparer) {
    super(planner);
    this.streamEngine = streamEngine;
    this.streamPreparer = streamPreparer;
    this.inMemorySchema = new InMemoryCalciteSchema();
  }

  /**
   */
  @Override
  @SneakyThrows
  public <T> T visit(SourceTableImportOp op) {
    List<Table> createdTables = super.visit(op);

    RelDataType rootType = new FlexibleTableConverter(op.getSourceTableImport().getSchema()).apply(
        new CalciteSchemaGenerator(planner.getTypeFactory())).get();

    inMemorySchema.registerSourceTable(op.getSourceTableImport().getTable().qualifiedName(),
        rootType.getFieldList(), getDataFromImport(op.getSourceTableImport()));

    for (Table table : createdTables) {
      executeAndRegister(table.getId().getCanonical());
    }

    return null;
  }

  public Collection<Object[]> getDataFromImport(SourceTableImport tableImport) {
    StreamEngine.Builder streamJobBuilder = streamEngine.createJob();
    streamPreparer.importTable(tableImport,streamJobBuilder);
    StreamEngine.Job job = streamJobBuilder.build();
    String tableQualifiedName = tableImport.getTable().qualifiedName();
    job.execute("populate["+tableQualifiedName+"]");

    Collection<Object[]> objects = ((InMemStreamEngine.Job)job).getRecordHolder().get(tableQualifiedName);
    return objects;
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddColumnOp op) {
    super.visit(op);
    executeAndRegister(op.getTable().getId().getCanonical());
    return null;
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddNestedTableOp op) {
    super.visit(op);
    executeAndRegister(op.getTable().getId().getCanonical());
    return super.visit(op);
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddRootTableOp op) {
    super.visit(op);
    executeAndRegister(op.getTable().getId().getCanonical());

    return super.visit(op);
  }

  @SneakyThrows
  public void executeAndRegister(String tableName) {
    RelBuilder builder = planner.getRelBuilder();

    RelNode rel = builder
        .scan(tableName)
        .build();

    System.out.println(rel.explain());

    List<Object[]> data = execute(rel);

    inMemorySchema.registerDataTable(tableName,
        rel.getRowType().getFieldList(),
        data);

    //Override the installed table to indicate that we don't need to expand the query table anymore
    this.tableMap.put(Name.system(tableName), new DataTable(rel.getRowType().getFieldList(), data));
  }

  @SneakyThrows
  public List<Object[]> execute(RelNode node) {
    for (Stage stage : Rules.ENUMERABLE_STAGES) {
      node = planner.transform(stage.getStage(), stage.applyStageTrait(planner.getEmptyTraitSet()),
          node);
      System.out.println(node.explain());
    }


    Bindable<Object[]> bindable = EnumerableInterpretable.toBindable(new HashMap<>(),
        null, (EnumerableRel) node, Prefer.ARRAY);

    List<Object[]> results = new ArrayList<>();

    SchemaPlus rootMemSchema = CalciteSchema.createRootSchema(false, false).plus();
    rootMemSchema.add(planner.getDefaultSchema().getName(), inMemorySchema);

    LocalDataContext ctx = new LocalDataContext(rootMemSchema);
    System.out.println("--Results");
    for (Object o : bindable.bind(ctx)) {
      if (o instanceof Object[]) {
        results.add((Object[]) o);
        System.out.println(Arrays.toString((Object[]) o));
      } else {
        results.add(new Object[]{o});
      }
    }
    return results;
  }
}
