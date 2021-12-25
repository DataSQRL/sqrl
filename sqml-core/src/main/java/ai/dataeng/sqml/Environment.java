package ai.dataeng.sqml;

import ai.dataeng.sqml.ScriptBundle.SqmlScript;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.catalog.ScriptManager;
import ai.dataeng.sqml.catalog.ScriptProcessor;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.ScriptParserProvider;
import ai.dataeng.sqml.config.provider.ScriptProcessorProvider;
import ai.dataeng.sqml.config.provider.ValidatorProvider;
import ai.dataeng.sqml.execution.Optimizer;
import ai.dataeng.sqml.execution.StreamExecutor;
import ai.dataeng.sqml.importer.DatasetManager;
import ai.dataeng.sqml.config.EnvironmentSettings;
import ai.dataeng.sqml.parser.ScriptParser;
import ai.dataeng.sqml.parser.validator.Validator;
import ai.dataeng.sqml.planner.OptimizerResult;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.PlannerResult;
import ai.dataeng.sqml.planner.Script;
import ai.dataeng.sqml.planner.optimize.OptimizerProvider;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class Environment {

  private final EnvironmentSettings settings;
  private final DatasetManager datasetManager;
  private final ScriptManager scriptManager;
  private final StreamExecutor streamExecutor;

  private final ScriptParserProvider scriptParserProvider;
  private final ValidatorProvider validatorProvider;
  private final ScriptProcessorProvider scriptProcessorProvider;
  private final HeuristicPlannerProvider heuristicPlannerProvider;
  private final OptimizerProvider optimizerProvider;

  public static Environment create(EnvironmentSettings settings) {
    DatasetManager datasetManager = settings.getImportManagerProvider().createImportManager();
    ScriptManager scriptManager = settings.getScriptManagerProvider().createScriptManager();
    StreamExecutor streamExecutor = settings.getPipelineExecutorProvider().createStreamExecutor();

    return new Environment(settings, datasetManager, scriptManager, streamExecutor,
        settings.getScriptParserProvider(), settings.getValidatorProvider(), settings.getScriptProcessorProvider(),
        settings.getHeuristicPlannerProvider(), settings.getOptimizerProvider());
  }

  public ProcessBundle<ProcessMessage> validate(ScriptNode scriptNode) {
    Validator validator = validatorProvider.getValidator();
    ProcessBundle<ProcessMessage> errors = validator.validate(scriptNode);
    ProcessBundle.logMessages(errors);

    return errors;
  }

  public Script compile(ScriptBundle bundle) throws Exception {
    SqmlScript mainScript = bundle.getMainScript();

    ScriptParser scriptParser = scriptParserProvider.createScriptParser();
    ScriptNode scriptNode = scriptParser.parse(mainScript);

    ProcessBundle<ProcessMessage> errors = validate(scriptNode);
    if (errors.isFatal()) {
      throw new Exception("Could not compile script.");
    }

    ScriptProcessor processor = scriptProcessorProvider.createScriptProcessor(
        settings.getImportProcessorProvider().createImportProcessor(datasetManager),
        settings.getQueryProcessorProvider().createQueryProcessor(),
        settings.getExpressionProcessorProvider().createExpressionProcessor(),
        settings.getJoinProcessorProvider().createJoinProcessor(),
        settings.getDistinctProcessorProvider().createDistinctProcessor(),
        settings.getSubscriptionProcessorProvider().createSubscriptionProcessor());

    Namespace namespace = processor.process(scriptNode);

    Planner heuristicPlanner = heuristicPlannerProvider.createPlanner();
    PlannerResult result = heuristicPlanner.plan(namespace);

    Optimizer optimizer = optimizerProvider.createOptimizer();
    OptimizerResult optimizerResult = optimizer.optimize(result);

    Script script = new Script(
        namespace.getSchema(),
        optimizerResult.getLogicalPlan(),
        optimizerResult.getExecutionPlan());

    scriptManager.addMainScript(script);

    streamExecutor.register(script.getExecutionPlan());

    return script;
  }

  public Optional<Script> getScript(String scriptName) {
    return scriptManager.getScript(scriptName);
  }

  public void registerDataset() {
    datasetManager.register();
  }

  public void execute() {
    streamExecutor.execute();
  }

  public void stop() {
    streamExecutor.stop();
  }
}
