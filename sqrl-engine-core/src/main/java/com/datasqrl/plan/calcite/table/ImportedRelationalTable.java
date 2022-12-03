package ai.datasqrl.plan.calcite.table;

import ai.datasqrl.io.sources.dataset.TableSource;

public interface ImportedRelationalTable extends SourceRelationalTable {

    TableSource getTableSource();

}
