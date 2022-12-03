package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.tables.TableSource;

public interface ImportedRelationalTable extends SourceRelationalTable {

    TableSource getTableSource();

}
