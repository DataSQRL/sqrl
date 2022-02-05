package ai.dataeng.sqml.parser.processor;

import static ai.dataeng.sqml.tree.name.Name.SELF_IDENTIFIER;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.JoinAssignment;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;

public class JoinProcessorImpl implements JoinProcessor {

  @Override
  public void process(JoinAssignment statement, Namespace namespace) {
    NamePath namePath = statement.getNamePath().getPrefix()
        .orElseThrow(()->new RuntimeException(String.format("Cannot assign join to prefix %s", statement.getNamePath())));

    Table source = namespace.lookup(namePath)
        .orElseThrow(()->new RuntimeException(String.format("Could not find source table: %s", namePath)));

    NamePath destinationPath = statement.getInlineJoin().getJoin().getTable();

    if (destinationPath.get(0).equals(SELF_IDENTIFIER)) {
      destinationPath = statement.getNamePath().getPrefix().get()
          .resolve(destinationPath.popFirst());
    }

    Table destination = namespace.lookup(destinationPath)
        .orElseThrow(
            ()->
              new RuntimeException(
                String.format("Could not find destination table %s",
                    statement.getInlineJoin().getJoin().getTable())));

    Expression expression = statement.getInlineJoin().getJoin().getCriteria();
    //In calcite, process as a join then extract the conditions. This will be query time conditions?
    //todo: better processing, hack for now
    ComparisonExpression expr = (ComparisonExpression) expression;
    NamePath lhs = NamePath.parse(((Identifier)expr.getLeft()).getValue());
    NamePath rhs = NamePath.parse(((Identifier)expr.getRight()).getValue());
    if (!lhs.getFirst().getCanonical().equals("_")) {
      NamePath tmp = lhs;
      lhs = rhs;
      rhs = tmp;
    }

    Field lhField = source.getField(lhs.getLast());
    Field rhField = source.getField(rhs.getLast());
    Preconditions.checkNotNull(lhField);
    Preconditions.checkNotNull(rhField);

    source.addField(new Relationship(statement.getNamePath().getLast(),
        source, destination, Type.JOIN, Multiplicity.MANY, Optional.of(List.of(lhField)), Optional.of(List.of(rhField))));

    if (statement.getInlineJoin().getInverse().isPresent()) {
      Name inverseName = statement.getInlineJoin().getInverse().get();
      destination.addField(new Relationship(inverseName,
          destination, source, Type.JOIN, Multiplicity.MANY, Optional.empty(), Optional.empty()));
    }

    //TODO: Inverse
  }
}
