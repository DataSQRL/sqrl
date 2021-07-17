package ai.dataeng.sqml.model;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import java.util.Map;

public class ModelAnalysis {
  private Map<QualifiedName, Analysis> relationAnalysis;
}
