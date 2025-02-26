/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util.data;

import java.nio.file.Path;
import java.util.Set;

public class Books extends UseCaseExample {

  public static final Books INSTANCE = new Books();

  public static final Path DATA_DIRECTORY = Path.of("/Users/matthias/Data/books/sample");

  protected Books() {
    super(Set.of("books", "authors", "reviews"), scripts().add("recommendation", "").build());
  }

  @Override
  public Path getDataDirectory() {
    return DATA_DIRECTORY;
  }
}
