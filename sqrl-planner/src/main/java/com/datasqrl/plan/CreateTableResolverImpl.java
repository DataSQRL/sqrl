package com.datasqrl.plan;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;


@AllArgsConstructor(onConstructor_=@Inject)
public class CreateTableResolverImpl implements CreateTableResolver {
  CalciteTableFactory tableFactory;
  ModuleLoader moduleLoader;

  @Override
  public NamespaceObject create(TableSource table) {
    return new TableSourceNamespaceObject(table, tableFactory, moduleLoader);
  }

}
