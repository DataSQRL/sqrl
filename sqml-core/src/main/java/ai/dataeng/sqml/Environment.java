package ai.dataeng.sqml;

import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.api.graphql.SqrlCodeRegistryBuilder;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.execution.flink.environment.DefaultFlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.io.sources.dataset.DatasetLookup;
import ai.dataeng.sqml.execution.flink.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.execution.sql.SQLGenerator;
import ai.dataeng.sqml.parser.ScriptParser;
import ai.dataeng.sqml.parser.processor.ScriptProcessor;
import ai.dataeng.sqml.parser.validator.Validator;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.Script;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.planner.operator.QueryAnalyzer;
import ai.dataeng.sqml.planner.optimize.LogicalPlanOptimizer;
import ai.dataeng.sqml.planner.optimize.MaterializeSource;
import ai.dataeng.sqml.planner.optimize.SimpleOptimizer;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLCodeRegistry;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@AllArgsConstructor
public class Environment {

  private final SqrlSettings settings;

  public static Environment create(SqrlSettings settings) {
    return new Environment(settings);
  }

  public ProcessBundle<ProcessMessage> validate(ScriptNode scriptNode) {
    Validator validator = settings.getValidatorProvider().getValidator();
    ProcessBundle<ProcessMessage> errors = validator.validate(scriptNode);
    ProcessBundle.logMessages(errors);
    return errors;
  }

  public Script compile(ScriptBundle bundle) throws Exception {
    SqmlScript mainScript = bundle.getMainScript();

    //Instantiate import resolver and register user schema
    DatasetLookup dsLookup = settings.getDsLookup();
    ImportResolver importResolver = settings.getImportManagerProvider().createImportManager(dsLookup);
    ProcessBundle<SchemaConversionError> importErrors = importResolver.getImportManager()
            .registerUserSchema(mainScript.parseSchema());
    Preconditions.checkArgument(!importErrors.isFatal(),
            importErrors);

    ScriptParser scriptParser = settings.getScriptParserProvider().createScriptParser();
    ScriptNode scriptNode = scriptParser.parse(mainScript);

    ProcessBundle<ProcessMessage> errors = validate(scriptNode);
    if (errors.isFatal()) {
      throw new Exception("Could not compile script.");
    }
    HeuristicPlannerProvider planner =
        settings.getHeuristicPlannerProvider();
    ScriptProcessor processor = settings.getScriptProcessorProvider().createScriptProcessor(
        settings.getImportProcessorProvider().createImportProcessor(importResolver, planner),
        settings.getQueryProcessorProvider().createQueryProcessor(planner),
        settings.getExpressionProcessorProvider().createExpressionProcessor(planner),
        settings.getJoinProcessorProvider().createJoinProcessor(),
        settings.getDistinctProcessorProvider().createDistinctProcessor(),
        settings.getSubscriptionProcessorProvider().createSubscriptionProcessor(),
        settings.getNamespace());

    Namespace namespace = processor.process(scriptNode);

    LogicalPlanImpl logicalPlan = namespace.getLogicalPlan();

    QueryAnalyzer.addDevModeQueries(logicalPlan);
    Preconditions.checkArgument(!errors.isFatal());

    LogicalPlanOptimizer.Result optimized = new SimpleOptimizer()
        .optimize(logicalPlan);

    SQLGenerator.Result sql = settings.getSqlGeneratorProvider()
        .create()
        .generateDatabase(optimized);
    sql.executeDMLs();

    List<MaterializeSource> sources = optimized.getReadLogicalPlan();

    SqrlCodeRegistryBuilder codeRegistryBuilder = new SqrlCodeRegistryBuilder();
    GraphQLCodeRegistry registry = codeRegistryBuilder.build(settings.getSqlClientProvider(), sources);

    FlinkStreamEngine envProvider = new DefaultFlinkStreamEngine();
    StreamExecutionEnvironment flinkEnv = settings.getFlinkGeneratorProvider()
        .create(envProvider)
        .generateStream(optimized, sql.getSinkMapper());

    flinkEnv.execute();

    return new Script(namespace, registry);
  }


  @SneakyThrows
  public void registerDataset(SourceDataset sourceDataset) {
    settings.getDsLookup().addDataset(sourceDataset);
  }

  public void monitorDatasets() {
    FlinkStreamEngine envProvider = new DefaultFlinkStreamEngine();

    settings.getDsLookup()
        .monitorDatasets(envProvider);
  }
}
