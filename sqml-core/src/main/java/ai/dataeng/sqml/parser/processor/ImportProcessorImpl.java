package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import ai.dataeng.sqml.planner.operator.ImportResolver.ImportMode;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import java.util.Optional;
import lombok.SneakyThrows;

public class ImportProcessorImpl implements ImportProcessor {

  private final HeuristicPlannerProvider plannerProvider;
  private final ImportResolver importResolver;

  public ImportProcessorImpl(ImportResolver importResolver,
      HeuristicPlannerProvider plannerProvider) {
    this.plannerProvider = plannerProvider;
    this.importResolver = importResolver;
  }

  @Override
  @SneakyThrows
  public void process(ImportDefinition statement, Namespace namespace) {
    NamePath name = statement.getNamePath();
    ProcessBundle<ProcessMessage> errors = new ProcessBundle<ProcessMessage> ();

    if (name.getLength() > 2) {
      throw new RuntimeException(String.format("Cannot import identifier: %s", name));
    }
    if (name.getLength() == 1) {
      importResolver.resolveImport(ImportMode.DATASET, name.getFirst(), Optional.empty(),
          statement.getAliasName(), namespace, errors);

    } else if (name.get(1).getDisplay().equals("*")) {
      if (statement.getAliasName().isPresent()) {
        throw new RuntimeException(String.format("Could not alias star (*) import: %s", name));
      }
      //adds to ns
      importResolver.resolveImport(ImportMode.ALLTABLE, name.getFirst(), Optional.empty(),
          Optional.empty(), namespace, errors);

    } else {
      //adds to ns
      importResolver.resolveImport(ImportMode.TABLE, name.getFirst(), Optional.of(name.get(1)),
          statement.getAliasName(), namespace, errors);
    }

    //TODO: Error manager
    if (errors.isFatal()) {
      throw new RuntimeException(String.format("Import errors: %s", errors));
    }

  }
}