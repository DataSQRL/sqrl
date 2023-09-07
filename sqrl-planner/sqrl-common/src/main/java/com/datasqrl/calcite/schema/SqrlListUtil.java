package com.datasqrl.calcite.schema;

import java.util.List;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class SqrlListUtil {

  public static List<String> popLast(List<String> list) {
    return list.subList(0, list.size()-1);
  }
}
