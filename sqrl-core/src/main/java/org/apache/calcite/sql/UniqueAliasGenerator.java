package org.apache.calcite.sql;

import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;

public interface UniqueAliasGenerator {
    public String generate(TableWithPK table);
    public String generate(String previousAlias);

  }