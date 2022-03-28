package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.operator.ImportResolver;
import ai.dataeng.sqml.parser.operator.ImportResolver.ImportMode;
import ai.dataeng.sqml.parser.sqrl.operations.SqrlOperation;
import ai.dataeng.sqml.schema.Namespace;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.SqrlStatement;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ImportProcessor {
  Namespace namespace;
  ImportResolver importResolver;

  public List<SqrlOperation> process(SqrlStatement sqrlStatement) {
    ImportDefinition statement = (ImportDefinition) sqrlStatement;
    NamePath name = statement.getNamePath();
//    ProcessBundle<ProcessMessage> errors = new ProcessBundle<ProcessMessage> ();

    if (name.getLength() > 2) {
      throw new RuntimeException(String.format("Cannot import identifier: %s", name));
    }
    if (name.getLength() == 1) {
      importResolver.resolveImport(ImportMode.DATASET, name.getFirst(), Optional.empty(),
          statement.getAliasName(), namespace);

    } else if (name.get(1).getDisplay().equals("*")) {
      if (statement.getAliasName().isPresent()) {
        throw new RuntimeException(String.format("Could not alias star (*) import: %s", name));
      }
      //adds to ns
      importResolver.resolveImport(ImportMode.ALLTABLE, name.getFirst(), Optional.empty(),
          Optional.empty(), namespace);

    } else {
      //adds to ns
      importResolver.resolveImport(ImportMode.TABLE, name.getFirst(), Optional.of(name.get(1)),
          statement.getAliasName(), namespace);
    }

    //TODO: Error manager
//    if (errors.isFatal()) {
//      throw new RuntimeException(String.format("Import errors: %s", null));
//    }

    return List.of();
  }
}
