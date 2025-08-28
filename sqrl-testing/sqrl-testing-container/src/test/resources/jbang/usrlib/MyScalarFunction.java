//DEPS org.apache.flink:flink-table-common:1.19.3
//DEPS com.google.auto.service:auto-service:1.1.1

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class MyScalarFunction extends ScalarFunction {

  public Long eval(Long a, Long b) {
    return a + b;
  }
}
