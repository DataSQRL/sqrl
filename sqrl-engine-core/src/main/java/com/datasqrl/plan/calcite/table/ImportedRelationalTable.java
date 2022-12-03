package com.datasqrl.plan.calcite.table;

import com.datasqrl.io.sources.dataset.TableSource;

public interface ImportedRelationalTable extends SourceRelationalTable {

    TableSource getTableSource();

}
