package ai.dataeng.sqml.api.graphql;

import ai.dataeng.sqml.parser.Table;

public class NameTranslator {
  public String getGraphqlName(Table tbl) {
    return tbl.getName().getCanonical();
  }
  public String getGraphqlTypeName(Table tbl) {
    return tbl.getName().getDisplay();
  }
}
