package com.datasqrl.plan;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.plan.local.generate.AbstractTableNamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.validate.ScriptPlanner;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqrlColumnDefinition;


@AllArgsConstructor(onConstructor_=@Inject)
public class CreateTableResolverImpl implements CreateTableResolver {
  CalciteTableFactory tableFactory;
  ModuleLoader moduleLoader;

  @Override
  public NamespaceObject create(TableSource table) {
    return new TableSourceNamespaceObject(table, tableFactory, moduleLoader);
  }

}
