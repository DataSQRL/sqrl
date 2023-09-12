/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.functions.TextFunctions;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.ListUtils;

@AutoService(StdLibrary.class)
public class StdTextLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("text");
  public static final List<SqrlFunction> SQRL_FUNCTIONS = List.of(
      TextFunctions.TEXT_SEARCH,
      TextFunctions.FORMAT,
      TextFunctions.BANNED_WORDS_FILTER
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = List.of(
  );

  public StdTextLibraryImpl() {
    super(ListUtils.union(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()),SQL_FUNCTIONS));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
