package com.datasqrl.util;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import lombok.NonNull;
import lombok.SneakyThrows;

public class WriterUtil {

  public static<O> void writeToFile(@NonNull List<O> objs, @NonNull Path filename,
      String header, String footer) {
    writeToFile(objs,filename, Objects::toString, header, footer);
  }

  public static<O> void writeToFileSorted(@NonNull List<O> objs, @NonNull Path filename,
      Comparator<O> comparator,
      String header, String footer) {
    Collections.sort(objs, comparator);
    writeToFile(objs,filename, Objects::toString, header, footer);
  }


  @SneakyThrows
  public static<O> void writeToFile(@NonNull List<O> objs, @NonNull Path filename,
      @NonNull Function<O,String> printer, String header, String footer) {
    System.out.printf("Writing %s elements to file %s\n", objs.size(), filename);
    try (PrintWriter out = new PrintWriter(new FileWriter(filename.toFile()))) {
      if (header != null) {
        out.println(header);
      }
      for (O o : objs) {
        out.println(printer.apply(o));
      }
      if (footer != null) {
        out.println(footer);
      }
    }
  }

}
