/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery.stats;

import java.util.Arrays;

public class DocumentPath {

  public static DocumentPath ROOT = new DocumentPath();

  private final String[] names;

  private DocumentPath(String... names) {
    this.names = names;
  }

  public DocumentPath resolve(String sub) {
    String[] newnames = Arrays.copyOf(names, names.length + 1);
    newnames[names.length] = sub;
    return new DocumentPath(newnames);
  }
}
