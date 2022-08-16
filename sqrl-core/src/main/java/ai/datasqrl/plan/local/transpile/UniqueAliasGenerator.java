package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;

public interface UniqueAliasGenerator {
  String generate(String previousAlias);

  String generate(TableWithPK table);

  void notifyTableAlias(String alias);

  String generateFieldName();

  String generatePK();
}
