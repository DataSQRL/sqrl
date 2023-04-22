package com.datasqrl.plan.local.analyze;

import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.JsonLineFormat;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.NamePath;
import java.util.List;

public class ReflectionToTableSource {

  public static TableSource createTableSource(Class clazz, List<?> data) {
    String name = clazz.getSimpleName().toLowerCase();
    TableConfig.Builder builder = TableConfig.builder(name);
    builder.getFormatConfig().setProperty(FormatFactory.FORMAT_NAME_KEY,JsonLineFormat.NAME);
    builder.base(BaseTableConfig.builder().type(ExternalDataType.source.name()).identifier(name).schema("fixed").build());
    builder.getConnectorConfig().setProperty(DataSystemConnectorFactory.SYSTEM_NAME_KEY,"in-mem-" + name);
    TableConfig tableConfig = builder.build();
    return tableConfig.initializeSource(NamePath.of("ecommerce-data"),
        UtbToFlexibleSchema.createFlexibleSchema(ReflectionToUt.reflection(clazz)));
  }

}
