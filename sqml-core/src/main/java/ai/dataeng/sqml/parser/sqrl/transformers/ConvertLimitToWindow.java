package ai.dataeng.sqml.parser.sqrl.transformers;

import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.QuerySpecification;

public class ConvertLimitToWindow {

  /**
   *
   */
  public QuerySpecification transform(QuerySpecification spec, Table table) {
    return spec;
  }
}
