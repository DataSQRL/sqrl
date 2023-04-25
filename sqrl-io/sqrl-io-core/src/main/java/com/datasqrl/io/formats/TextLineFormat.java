/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.config.SqrlConfig;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import lombok.NonNull;

public interface TextLineFormat extends FormatFactory {

  @Override
  Parser getParser(@NonNull SqrlConfig config);

  interface Parser extends FormatFactory.Parser {

    Result parse(@NonNull String line);

  }

  @Override
  Writer getWriter(@NonNull SqrlConfig config);

  interface Writer extends FormatFactory.Writer {


  }

  String CHARSET_KEY = "charset";
  String DEFAULT_CHARSET = "UTF-8";

  default Charset getCharset(@NonNull SqrlConfig config) {
    return Charset.forName(config.asString(CHARSET_KEY).withDefault(DEFAULT_CHARSET)
        .validate(charset -> {
          try {
            return Charset.forName(charset)!=null;
          } catch (Exception e) {
            return false;
          }
        }, "Not a valid charset").get());
  }

}
