package ai.dataeng.sqml.parser;

import org.apache.calcite.rel.RelNode;

public class PlanQuery {
  QualifiedIdentifier name;
  RelNode plan;
}
