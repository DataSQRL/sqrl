package ai.dataeng.sqml;

import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.EnvironmentSettings;
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
import ai.dataeng.sqml.parser.ScriptParser;
import ai.dataeng.sqml.parser.processor.ScriptProcessor;
import ai.dataeng.sqml.parser.validator.Validator;
import ai.dataeng.sqml.planner.HeuristicPlannerImpl;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.Script;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import ai.dataeng.sqml.type.constraint.Constraint;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class Environment {

  private final EnvironmentSettings settings;
  private final ImportResolver importResolver;

  private final ScriptParserProvider scriptParserProvider;
  private final ValidatorProvider validatorProvider;
  private final ScriptProcessorProvider scriptProcessorProvider;

  public static Environment create(EnvironmentSettings settings) {
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
//
//    Planner heuristicPlanner = heuristicPlannerProvider.createPlanner();
//    PlannerResult result = heuristicPlanner.plan(namespace);
//
//    Optimizer optimizer = optimizerProvider.createOptimizer();
//    OptimizerResult optimizerResult = optimizer.optimize(result);
//
//    Script script = new Script(
//        namespace,
//        optimizerResult.getLogicalPlan(),
//        optimizerResult.getExecutionPlan());
//
//    scriptManager.addMainScript(script);
//
//    streamExecutor.register(script.getExecutionPlan());

    return new Script(namespace, null);
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

  public void registerDataset(SourceDataset sourceDataset) {
    settings.getDsLookup().addDataset(sourceDataset);
  }

  public void monitorDatasets() {
    EnvironmentFactory envProvider = new DefaultEnvironmentFactory();

    settings.getDsLookup()
        .monitorDatasets(envProvider);
  }
}
