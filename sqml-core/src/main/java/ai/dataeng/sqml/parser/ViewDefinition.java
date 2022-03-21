package ai.dataeng.sqml.parser;

import org.apache.calcite.rel.RelNode;

public class ViewDefinition {
  QualifiedIdentifier name;
  RelNode plan;
}
