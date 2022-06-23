package ai.datasqrl.plan.calcite.memory;

import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.SourceRecord.Raw;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.physical.stream.inmemory.InMemStreamEngine;
import ai.datasqrl.physical.stream.inmemory.InMemStreamEngine.JobBuilder;
import ai.datasqrl.plan.calcite.BasicSqrlCalciteBridge;
import ai.datasqrl.plan.calcite.CalciteSchemaGenerator;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.Rules;
import ai.datasqrl.plan.calcite.Rules.Stage;
import ai.datasqrl.plan.local.operations.AddColumnOp;
import ai.datasqrl.plan.local.operations.AddNestedTableOp;
import ai.datasqrl.plan.local.operations.AddRootTableOp;
import ai.datasqrl.plan.local.operations.SourceTableImportOp;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.schema.input.SchemaValidator;
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
 * The enumerable DAG helps evaluate calcite plans. Once it does that, it stores the results in the
 * LocalDataContext for future SQRL statements.
 *
 * The EnumerableDag manages an independent calcite schema which contains enumerable data. This is used
 * by the DataContext for data retrieval.
 */
public class EnumerableDag extends BasicSqrlCalciteBridge {

  @Getter
  private final InMemoryCalciteSchema inMemorySchema;

  @Getter
  private final SchemaPlus rootMemSchema;

  public EnumerableDag(Planner planner) {
    super(planner);
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    InMemoryCalciteSchema memorySchema = new InMemoryCalciteSchema();
    rootSchema.add(planner.getDefaultSchema().getName(), memorySchema);
    this.inMemorySchema = memorySchema;
    this.rootMemSchema = rootSchema;
  }

  /**
   * This method works in two parts:
   *  1. Registers the data source with the memory schema
   *  2. Evaluates all tables and stores the result in the memory schema
   */
  @Override
  @SneakyThrows
  public <T> T visit(SourceTableImportOp op) {
    List<Table> createdTables = super.visit(op);

    RelDataType type2 = new FlexibleTableConverter(op.getSourceTableImport().getSchema()).apply(
        new CalciteSchemaGenerator(planner.getTypeFactory())).get();

    inMemorySchema.registerDataSet(op.getSourceTableImport().getTable().qualifiedName(),
        type2.getFieldList(), getDataFromImport(op.getSourceTableImport()));

    for (Table table : createdTables) {
      RelBuilder builder = planner.getRelBuilder();
      /*
       * during shredding rule eval gets transforms from:
       * SELECT * FROM table$1;
       * into:
       * SELECT * FROM dataset.Table;
       */
      RelNode rel = builder
          .scan(table.getId().getCanonical())
          .build();
      List<Object[]> data = execute(rel);
      inMemorySchema.registerDataTable(table.getId().getCanonical(), rel.getRowType().getFieldList(),
          data);
    }

    return null;
  }

  public Collection<Object[]> getDataFromImport(SourceTableImport tableImport) {
    JobBuilder streamJobBuilder = new InMemStreamEngine().createJob();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();

    StreamHolder<Raw> stream = streamPreparer.getRawInput(tableImport.getTable(), streamJobBuilder);
    SchemaValidator schemaValidator = new SchemaValidator(tableImport.getSchema(), SchemaAdjustmentSettings.DEFAULT, tableImport.getTable().getDataset().getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),"schema", SourceRecord.Named.class);

    streamJobBuilder.addAsTable(validate, tableImport.getSchema(), tableImport.getTableName());
    InMemStreamEngine.Job job = streamJobBuilder.build();

//    fills the streams?
    job.execute("test");

    Collection<Object[]> objects = job.getRecordHolder().get(tableImport.getTableName());
    return objects;
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddColumnOp op) {
    super.visit(op);
    executeAndRegister(op.getTable().getId().getCanonical(), op.getJoinedNode());
    return null;
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddNestedTableOp op) {
    super.visit(op);
    executeAndRegister(op.getTable().getId().getCanonical(), op.getNode());
    return super.visit(op);
  }

  @SneakyThrows
  @Override
  public <T> T visit(AddRootTableOp op) {
    super.visit(op);
    executeAndRegister(op.getTable().getId().getCanonical(), op.getNode());

    return super.visit(op);
  }

  @SneakyThrows
  public void executeAndRegister(String tableName, Node n) {
    RelBuilder builder = planner.getRelBuilder();

    RelNode rel = builder
        .scan(tableName)
        .build();

    System.out.println(rel.explain());

    List<Object[]> data = execute(rel);

    inMemorySchema.registerDataTable(tableName,
        rel.getRowType().getFieldList(),
        data);
  }

  @SneakyThrows
  public List<Object[]> execute(RelNode node) {
    for (Stage stage : Rules.ENUMERABLE_STAGES) {
      node = planner.transform(stage.getStage(), stage.applyStageTrait(planner.getEmptyTraitSet()), node);

    }

    Bindable<Object[]> bindable = EnumerableInterpretable.toBindable(new HashMap<>(),
        null, (EnumerableRel)node, Prefer.ARRAY);

    List<Object[]> results = new ArrayList<>();

    LocalDataContext ctx = new LocalDataContext(this.rootMemSchema);
    for (Object o : bindable.bind(ctx)) {
      if (o instanceof Object[]) {
        results.add((Object[])o);
        System.out.println(Arrays.toString((Object[])o));
      } else {
        results.add(new Object[]{o});
      }
    }
    return results;
  }
}
