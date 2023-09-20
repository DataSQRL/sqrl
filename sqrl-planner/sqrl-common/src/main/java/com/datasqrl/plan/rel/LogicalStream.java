package com.datasqrl.plan.rel;

import static com.datasqrl.calcite.type.TypeFactory.makeTimestampType;
import static com.datasqrl.calcite.type.TypeFactory.makeUuidType;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.model.LogicalStreamMetaData;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import com.datasqrl.model.StreamType;
import org.apache.calcite.util.Litmus;

@Getter
public class LogicalStream extends SingleRel {

  protected final StreamType streamType;
  protected final LogicalStreamMetaData metaData;

  protected LogicalStream(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      StreamType streamType, LogicalStreamMetaData metaData) {
    super(cluster, traits, input);
    assert streamType!=null;
    this.streamType = streamType;
    this.metaData = metaData;
  }

  public static LogicalStream create(RelNode input, StreamType streamType, LogicalStreamMetaData metaData) {
    RelOptCluster cluster = input.getCluster();
    RelMetadataQuery mq = cluster.getMetadataQuery();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalStream(cluster, traitSet, input, streamType, metaData);
  }

  public static LogicalStream create(RelNode input, StreamType streamType) {
    RelOptCluster cluster = input.getCluster();
    RelMetadataQuery mq = cluster.getMetadataQuery();
    RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalStream(cluster, traitSet, input, streamType, null);
  }

  public LogicalStream copy(RelTraitSet traitSet, RelNode input, StreamType streamType, LogicalStreamMetaData metaData) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalStream(this.getCluster(), traitSet, input, streamType, metaData);
  }

  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return this.copy(traitSet, (RelNode)sole(inputs), this.streamType, metaData);
  }

  public LogicalStream copy(RelNode input) {
    return new LogicalStream(this.getCluster(), traitSet, input, streamType, metaData);
  }

  protected RelDataType deriveRowType() {
    return deriveRowType(this.getCluster().getTypeFactory(), this.getInput().getRowType(), this.metaData==null?null:this.metaData.getSelectIdx());
  }

  public static RelDataType deriveRowType(RelDataTypeFactory typeFactory, RelDataType inputRowType, int[] selectIdx) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    Set<String> containedNames = new HashSet();

    builder.add(ReservedName.UUID.getCanonical()+"$0", makeUuidType(typeFactory, false));
    builder.add(ReservedName.SOURCE_TIME.getCanonical()+"$0", makeTimestampType(typeFactory,false));
    containedNames.add(ReservedName.UUID.getCanonical()+"$0");
    containedNames.add(ReservedName.SOURCE_TIME.getCanonical()+"$0");


    List<RelDataTypeField> fieldList = inputRowType.getFieldList();
    if (selectIdx==null) {
      //Select all
      selectIdx = IntStream.range(0, fieldList.size()).toArray();
    }
    for (int selected : selectIdx) {
      RelDataTypeField field = fieldList.get(selected);
      String name = dedupName(field.getName(), containedNames);
      builder.add(name, field.getType());
    }
    return builder.build();
  }

  private static String dedupName(String basename, Set<String> containedNames) {
    String dedupName = basename;
    for(int i = 0; containedNames.contains(dedupName); dedupName = basename + i++) {
    }
    containedNames.add(dedupName);
    return dedupName;
  }


  public boolean isValid(Litmus litmus, RelNode.Context context) {
    if (streamType ==null) {
      return litmus.fail("Need to specify change type");
    }
//    if (timestampIdx<0) {
//      return litmus.fail("Must have timestamp index");
//    }
//    if (selectIdx.length==0 || Arrays.stream(selectIdx).anyMatch(idx -> idx <0)) {
//      return litmus.fail("Invalid select indexes");
//    }
    return litmus.succeed();
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("stream", this.streamType);
  }

  public boolean deepEquals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj != null && this.getClass() == obj.getClass()) {
      LogicalStream o = (LogicalStream)obj;
      return this.traitSet.equals(o.traitSet) && this.input.deepEquals(o.input)
          && this.streamType.equals(o.streamType)
          && this.getRowType().equalsSansFieldNames(o.getRowType())
          && Objects.equals(this.metaData, o.metaData);
    } else {
      return false;
    }
  }

  public int deepHashCode() {
    return Objects.hash(new Object[]{this.traitSet, this.input.deepHashCode(), this.streamType, this.metaData});
  }

}
