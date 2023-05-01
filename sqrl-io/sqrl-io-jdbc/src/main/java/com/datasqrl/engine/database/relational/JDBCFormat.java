package com.datasqrl.engine.database.relational;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.FormatFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

import java.util.Collections;
import java.util.List;

@AutoService(FormatFactory.class)
public class JDBCFormat implements FormatFactory {

  public static final String FORMAT_NAME = "jdbc";

  @Override
  public List<String> getExtensions() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public String getName() {
    return FORMAT_NAME;
  }

  @Override
  public Parser getParser(@NonNull SqrlConfig config) {
    throw new UnsupportedOperationException(getName() + " format does not support parsing");
  }

  @Override
  public Writer getWriter(@NonNull SqrlConfig config) {
    throw new UnsupportedOperationException(getName() + " format does not support writing");
  }
}
