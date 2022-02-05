package ai.dataeng.sqml;

import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.api.graphql.SqrlCodeRegistryBuilder;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.ScriptParserProvider;
import ai.dataeng.sqml.config.provider.ScriptProcessorProvider;
import ai.dataeng.sqml.config.provider.ValidatorProvider;
import ai.dataeng.sqml.execution.flink.environment.DefaultEnvironmentFactory;
import ai.dataeng.sqml.execution.flink.environment.EnvironmentFactory;
import ai.dataeng.sqml.execution.flink.ingest.DatasetLookup;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlexibleDatasetSchema;
import ai.dataeng.sqml.execution.flink.ingest.schema.external.SchemaDefinition;
import ai.dataeng.sqml.execution.flink.ingest.schema.external.SchemaImport;
import ai.dataeng.sqml.execution.flink.ingest.source.SourceDataset;
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
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.constraint.Constraint;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLCodeRegistry;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@AllArgsConstructor
public class Environment {

  private final SqrlSettings settings;
  private final ImportResolver importResolver;

  private final ScriptParserProvider scriptParserProvider;
  private final ValidatorProvider validatorProvider;
  private final ScriptProcessorProvider scriptProcessorProvider;

  public static Environment create(SqrlSettings settings) {
    ImportResolver importResolver = settings.getImportManagerProvider().createImportManager(
        settings.getDsLookup()
    );

    return new Environment(settings, importResolver,
        settings.getScriptParserProvider(), settings.getValidatorProvider(),
        settings.getScriptProcessorProvider());
  }

  public ProcessBundle<ProcessMessage> validate(ScriptNode scriptNode) {
    Validator validator = validatorProvider.getValidator();
    ProcessBundle<ProcessMessage> errors = validator.validate(scriptNode);
    ProcessBundle.logMessages(errors);

    return errors;
  }

  public Script compile(ScriptBundle bundle) throws Exception {
    SqmlScript mainScript = bundle.getMainScript();

    registerUserSchema(mainScript.parseSchema());

    ScriptParser scriptParser = scriptParserProvider.createScriptParser();
    ScriptNode scriptNode = scriptParser.parse(mainScript);

    ProcessBundle<ProcessMessage> errors = validate(scriptNode);
    if (errors.isFatal()) {
      throw new Exception("Could not compile script.");
    }
    HeuristicPlannerProvider planner =
        settings.getHeuristicPlannerProvider();
    ScriptProcessor processor = scriptProcessorProvider.createScriptProcessor(
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

    EnvironmentFactory envProvider = new DefaultEnvironmentFactory();
    StreamExecutionEnvironment flinkEnv = settings.getFlinkGeneratorProvider()
        .create(envProvider)
        .generateStream(optimized, sql.getSinkMapper());

    flinkEnv.execute();

    return new Script(namespace, registry);
  }

  public void registerUserSchema(
      SchemaDefinition schemaDefinition) {
    DatasetLookup dsLookup = importResolver.getImportManager().getDatasetLookup();
    SchemaImport schemaImporter = new SchemaImport(dsLookup, Constraint.FACTORY_LOOKUP);
    Map<Name, FlexibleDatasetSchema> userSchema = schemaImporter.convertImportSchema(
        schemaDefinition);
    Preconditions.checkArgument(!schemaImporter.getErrors().isFatal(),
        schemaImporter.getErrors());

    importResolver.getImportManager().registerUserSchema(userSchema);
  }

  @SneakyThrows
  public void registerDataset(SourceDataset sourceDataset) {
    settings.getDsLookup().addDataset(sourceDataset);
  }

  public void monitorDatasets() {
    EnvironmentFactory envProvider = new DefaultEnvironmentFactory();

    settings.getDsLookup()
        .monitorDatasets(envProvider);
  }
}
