import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.FlinkMain;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.google.common.io.Resources;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestFlinkMain {

  @Test
  @Disabled //todo: add new flink-plan
  public void testFlinkMain() {
    FlinkMain flinkMain = new FlinkMain();

    ErrorCollector errors = ErrorCollector.root();
    try {
      TableResult result = flinkMain.run(
          errors, Optional.of(Resources.getResource("flink-plan.json").toURI()),
          Optional.empty(),
          Optional.empty());

      assertEquals(ResultKind.SUCCESS_WITH_CONTENT, result.getResultKind());
    } catch (Exception e) {
      fail(e);
      throw new RuntimeException(e);
    }

    assertTrue(!errors.hasErrors(), ErrorPrinter.prettyPrint(errors));

  }

}
