package com.datasqrl;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

public class InitIceberg {

  public static void main(String[] args) throws Exception {
    new InitIceberg().run();
  }

  public void run() throws Exception {
    try (GlueCatalog catalog = new GlueCatalog()) {
      catalog.initialize("", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "loc"));

<#list snowflake["sinks"] as sink>
      Schema schema${sink["name"]} = new Schema(
<#list sink["columns"] as column>
            <#if column["optional"]>optional<#else>required</#if>(${column["index"]}, "${column["name"]}", Types.${column["typeName"]}.get())<#if column_has_next>,</#if>
</#list>
      );
      catalog.createTable(TableIdentifier.of(Namespace.of("${sink["namespace"]}"), "${sink["name"]}"), schema${sink["name"]});

</#list>
    }
  }
}
