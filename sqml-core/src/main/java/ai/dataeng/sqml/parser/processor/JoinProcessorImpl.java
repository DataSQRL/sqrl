package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.JoinAssignment;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;

public class JoinProcessorImpl implements JoinProcessor {

  @Override
  public void process(JoinAssignment statement, Namespace namespace) {
    NamePath namePath = statement.getNamePath().getPrefix()
        .orElseThrow(()->new RuntimeException(String.format("Cannot assign join to prefix %s", statement.getNamePath())));

    Table source = namespace.lookup(namePath)
        .orElseThrow(()->new RuntimeException(String.format("Could not find source table: %s", namePath)));

    //TODO: Lookup with context

    if (namePath.get(0).getCanonical().equalsIgnoreCase("@")) {
      throw new RuntimeException("next");
    }
    NamePath destinationPath = statement.getInlineJoin().getJoin().getTable();

    if (destinationPath.get(0).getCanonical().startsWith("@")) {
      destinationPath = statement.getNamePath().getPrefix().get()
          .resolve(destinationPath.popFirst());
    }

    Table destination = namespace.lookup(destinationPath)
        .orElseThrow(
            ()->
              new RuntimeException(
                String.format("Could not find destination table %s",
                    statement.getInlineJoin().getJoin().getTable())));

    source.addField(new Relationship(statement.getNamePath().getLast(),
        source, destination, Type.JOIN, Multiplicity.MANY, Optional.of(statement.getInlineJoin())));

    if (statement.getInlineJoin().getInverse().isPresent()) {
      Name inverseName = statement.getInlineJoin().getInverse().get();
      destination.addField(new Relationship(inverseName,
          destination, source, Type.JOIN, Multiplicity.MANY, Optional.of(statement.getInlineJoin())));
    }

    //TODO: Inverse
  }
}
