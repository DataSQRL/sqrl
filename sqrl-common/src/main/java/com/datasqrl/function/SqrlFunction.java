/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import com.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.FunctionNamespaceObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import org.apache.calcite.sql.SqlOperator;

import java.util.Optional;

public interface SqrlFunction {
  StdTimeLibraryImpl stdTimeLibrary = new StdTimeLibraryImpl();

  //
  static Optional<SqrlFunction> lookupTimeFunction(SqlOperator operator) {
    if (operator.getName().equalsIgnoreCase("now")) {
      return Optional.of(StdTimeLibraryImpl.NOW);
    }
    //lookup time fnc
    Optional<NamespaceObject> ns = stdTimeLibrary.getNamespaceObject(Name.system(operator.getName()));
    if (ns.isPresent()) {
      return ns.filter(n->n instanceof FunctionNamespaceObject)
          .map(n->((FunctionNamespaceObject)n).getFunction())
          .filter(f->f instanceof SqrlFunction)
          .map(f->(SqrlFunction)f);
    }

    if (operator instanceof SqrlFunction) {
      return Optional.of((SqrlFunction)operator);
    }
    return Optional.empty();
  }
}
