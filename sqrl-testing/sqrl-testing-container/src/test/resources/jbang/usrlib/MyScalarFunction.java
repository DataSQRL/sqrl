//JDEPS com.google.code.gson:gson:2.11.0
import com.google.gson.JsonPrimitive;
import org.apache.flink.table.functions.ScalarFunction;

public class MyScalarFunction extends ScalarFunction {

  public Long eval(Long a, Long b) {
    return new JsonPrimitive(a + b).getAsLong();
  }
}
