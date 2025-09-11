//DEPS org.apache.hadoop:hadoop-common:3.3.4
//DEPS org.apache.flink:flink-table-common:1.19.3
//DEPS org.apache.iceberg:iceberg-flink-runtime-1.19:1.9.2

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;

public class PartitionDeleteUdf extends ScalarFunction {

  public String eval(String partitionCol, Object... partitions) {
    if (partitions.length == 0) {
      return null;
    }

    try (var catalog = new HadoopCatalog(new Configuration(), "/tmp/duckdb")) {
      var table = catalog.loadTable(TableIdentifier.of("default_database", "MinCdcTable"));

      var del = table.newDelete().deleteFromRowFilter(Expressions.in(partitionCol, partitions));
      del.commit();

      return "done";

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    var inputTypeStrategy =
        InputTypeStrategies.compositeSequence().finishWithVarying(InputTypeStrategies.WILDCARD);

    return TypeInference.newBuilder()
        .inputTypeStrategy(inputTypeStrategy)
        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.CHAR(32)))
        .build();
  }
}
