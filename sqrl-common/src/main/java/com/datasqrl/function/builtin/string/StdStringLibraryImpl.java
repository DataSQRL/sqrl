package com.datasqrl.function.builtin.string;

import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.CalciteFunctionNsObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

@Getter
public class StdStringLibraryImpl implements SqrlModule {
  private static List<NamespaceObject> namespaceObjects = List.of(
      createNsObject("length", FlinkSqlOperatorTable.CHAR_LENGTH),
      createNsObject("upper", FlinkSqlOperatorTable.UPPER),
      createNsObject("position", FlinkSqlOperatorTable.POSITION),
      createNsObject("trim", FlinkSqlOperatorTable.TRIM),
      createNsObject("ltrim", FlinkSqlOperatorTable.LTRIM),
      createNsObject("rtrim", FlinkSqlOperatorTable.RTRIM),
      createNsObject("repeat", FlinkSqlOperatorTable.REPEAT),
      createNsObject("regexReplace", FlinkSqlOperatorTable.REGEXP_REPLACE),
      createNsObject("overlay", FlinkSqlOperatorTable.OVERLAY),
      createNsObject("substring", FlinkSqlOperatorTable.SUBSTRING),
      createNsObject("replace", FlinkSqlOperatorTable.REPLACE),
      createNsObject("regexpExtract", FlinkSqlOperatorTable.REGEXP_EXTRACT),
      createNsObject("initcap", FlinkSqlOperatorTable.INITCAP),
      createNsObject("concat", FlinkSqlOperatorTable.CONCAT_FUNCTION),
      createNsObject("concatWS", FlinkSqlOperatorTable.CONCAT_WS),
      createNsObject("lpad", FlinkSqlOperatorTable.LPAD),
      createNsObject("rpad", FlinkSqlOperatorTable.RPAD),
      createNsObject("fromBase64", FlinkSqlOperatorTable.FROM_BASE64),
      createNsObject("toBase64", FlinkSqlOperatorTable.TO_BASE64),
      createNsObject("ascii", FlinkSqlOperatorTable.ASCII),
      createNsObject("chr", FlinkSqlOperatorTable.CHR),
      createNsObject("decode", FlinkSqlOperatorTable.DECODE),
      createNsObject("encode", FlinkSqlOperatorTable.ENCODE),
      createNsObject("instr", FlinkSqlOperatorTable.INSTR),
      createNsObject("left", FlinkSqlOperatorTable.LEFT),
      createNsObject("right", FlinkSqlOperatorTable.RIGHT),
      createNsObject("locate", FlinkSqlOperatorTable.LOCATE),
      createNsObject("parseURL", FlinkSqlOperatorTable.PARSE_URL),
      createNsObject("regexp", FlinkSqlOperatorTable.REGEXP),
      createNsObject("reverse", FlinkSqlOperatorTable.REVERSE),
      createNsObject("splitIndex", FlinkSqlOperatorTable.SPLIT_INDEX),
      createNsObject("strToMap", FlinkSqlOperatorTable.STR_TO_MAP),
      createNsObject("substr", FlinkSqlOperatorTable.SUBSTR)
  );

  private static Map<Name, NamespaceObject> fncMap = Maps.uniqueIndex(namespaceObjects,
      NamespaceObject::getName);

  private static NamespaceObject createNsObject(String name, SqlFunction fnc) {
    return new CalciteFunctionNsObject(Name.system(name), fnc);
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return Optional.ofNullable(fncMap.get(name));
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return namespaceObjects;
  }
}
