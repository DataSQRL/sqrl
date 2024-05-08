package com.datasqrl.discovery.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface RecordReader {
  String getFormat();
  Stream<Map<String, Object>> read(InputStream input) throws IOException;

  Set<String> getExtensions();

}
