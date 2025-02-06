/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.functions.secure;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.flink.table.functions.FunctionDefinition;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.secure.SecureFunctions;
import com.google.auto.service.AutoService;

@AutoService(StdLibrary.class)
public class StdSecureLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("secure");
  public static final List<FunctionDefinition> SQRL_FUNCTIONS = List.of(
      SecureFunctions.RANDOM_ID,
      SecureFunctions.UUID
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = List.of(
  );

  public StdSecureLibraryImpl() {
    super(ListUtils.union(SQRL_FUNCTIONS.stream()
        .map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()),SQL_FUNCTIONS));
  }

  @Override
public NamePath getPath() {
    return LIB_NAME;
  }

}
