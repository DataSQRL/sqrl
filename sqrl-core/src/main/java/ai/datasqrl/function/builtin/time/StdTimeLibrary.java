package ai.datasqrl.function.builtin.time;


import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.ArrayListMultimap;
import org.apache.flink.calcite.shaded.com.google.common.collect.Multimap;

public class StdTimeLibrary extends AbstractSchema {

  private final Multimap<String, Function> functions = ArrayListMultimap.create();

  public StdTimeLibrary() {
    //functions.put("ROUNDTOMONTH", RoundMonth.fnc);
  }

  @Override
  protected Multimap<String, Function> getFunctionMultimap() {
    return functions;
  }
}
