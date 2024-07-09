package com.datasqrl.plan;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.NamespaceObject;

public interface CreateTableResolver {

  NamespaceObject create(TableSource table);
}
