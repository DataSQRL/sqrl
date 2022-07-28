package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;

public interface UniqueAliasGenerator {
    public String generate(TableWithPK table);
    public String generate(String previousAlias);

  }