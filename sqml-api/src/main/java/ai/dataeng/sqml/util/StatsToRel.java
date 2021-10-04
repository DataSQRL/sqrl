package ai.dataeng.sqml.util;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.Type;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class StatsToRel {

  public static RelationType toRelation(SourceTableStatistics stats) {
//    List<Field> fields = stats.getSchema().getTable().getFields()
//        .map(f->toField(f))
//        .collect(Collectors.toList());
//
//    RelationType relationType = new RelationType(fields);
//    return relationType;
    return new RelationType();
  }
//
//  private static Field toField(Entry<String, Element> f) {
//    return Field.newDataField(f.getKey(), toType(f.getValue()));
//  }
//
//  private static Type toType(Element value) {
//    return value.asField().getType();
//  }

}
