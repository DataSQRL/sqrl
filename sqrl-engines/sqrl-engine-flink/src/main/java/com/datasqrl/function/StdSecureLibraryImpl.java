/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.SecureFunctions;
import com.datasqrl.TextFunctions;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.ListUtils;

@AutoService(StdLibrary.class)
public class StdSecureLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("secure");
  public static final List<SqrlFunction> SQRL_FUNCTIONS = List.of(
      SecureFunctions.RANDOM_ID
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = List.of(
  );

  public StdSecureLibraryImpl() {
    super(ListUtils.union(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()),SQL_FUNCTIONS));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
