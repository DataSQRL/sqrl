//package ai.datasqrl.plan.nodes;
//
//import ai.datasqrl.plan.calcite.SourceCalciteTable;
//import ai.datasqrl.plan.local.BundleTableFactory;
//import java.util.ArrayList;
//import java.util.List;
//import org.apache.calcite.jdbc.CalciteSchema;
//import org.apache.calcite.plan.Context;
//import org.apache.calcite.plan.Convention;
//import org.apache.calcite.plan.RelOptCluster;
//import org.apache.calcite.plan.RelOptSchema;
//import org.apache.calcite.plan.RelOptTable;
//import org.apache.calcite.plan.RelTraitSet;
//import org.apache.calcite.prepare.RelOptTableImpl;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.logical.LogicalTableScan;
//import org.apache.calcite.rex.RexInputRef;
//import org.apache.calcite.schema.Path;
//import org.apache.calcite.schema.Schemas;
//import org.apache.calcite.tools.RelBuilder;
//
//public class SqrlRelBuilder extends RelBuilder {
//
//  private final CalciteSchema calciteSchema;
//
//  public SqrlRelBuilder(Context context,
//      RelOptCluster cluster,
//      RelOptSchema relOptSchema, CalciteSchema calciteSchema) {
//    super(context, cluster, relOptSchema);
//    this.calciteSchema = calciteSchema;
//  }
//
//  public SqrlRelBuilder scanStream(BundleTableFactory.TableBuilder table) {
//    SourceCalciteTable sourceCalciteTable = new SourceCalciteTable(table.getRowType());
//
//    Path path = Schemas.path(
//        calciteSchema, List.of(table.getName().getCanonical()));
//    RelOptTable relOptTable = RelOptTableImpl.create(this.relOptSchema, table.getRowType(),
//        sourceCalciteTable, path);
//
//    RelTraitSet traits = RelTraitSet.createEmpty();
//    traits = traits.plus(Convention.NONE);
//
//    RelNode scan = new LogicalTableScan(this.cluster, traits, List.of(),
//        relOptTable);
//    this.push(scan);
//    return this;
//  }
//
//  /**
//   * Project by index
//   */
//  public SqrlRelBuilder project(List<Integer> index) {
//    List<RexInputRef> indexes = new ArrayList<>();
//    for (Integer i : index) {
//      RexInputRef of = RexInputRef.of(i, this.peek().getRowType());
//      indexes.add(of);
//    }
//
//    this.project(indexes);
//
//    return this;
//  }
//
//
//  //TODO: Keeping this around for the physical planning phase
//
////  public SqrlRelBuilder watermark(int i) {
////    RexNode watermarkExpr = getRexBuilder().makeCall(SqlStdOperatorTable.MINUS,
////        List.of(RexInputRef.of(i, peek().getRowType()), getRexBuilder()
////            .makeIntervalLiteral(BigDecimal.valueOf(10000),
////              new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO))));
////
////    this.push(LogicalWatermarkAssigner
////        .create(cluster, build(), i, watermarkExpr));
////
////    return this;
////  }
//
////  public RelNode planNested(SqrlRelBuilder builder, Name baseStream, String fieldName, Table parentTable) {
////    RexBuilder rexBuilder = builder.getRexBuilder();
////    CorrelationId id = new CorrelationId(0);
////    int indexOfField = getIndex(parentTable.getRowType(), fieldName);
////    RelDataType t = FlinkTypeFactory.INSTANCE().createSqlType(SqlTypeName.INTEGER);
////    RelBuilder b = builder
////            .scanStream(baseStream, parentTable)
////            .watermark(getIndex(parentTable.getRowType(), INGEST_TIME.getCanonical()))
////            .values(List.of(List.of(rexBuilder.makeExactLiteral(BigDecimal.ZERO))),
////                    new RelRecordType(List.of(new RelDataTypeFieldImpl("ZERO", 0, t))))
////            .project(List.of(builder.getRexBuilder().makeFieldAccess(
////                    rexBuilder.makeCorrel(parentTable.getRowType(), id), fieldName, false
////            )), List.of(fieldName))
////            .uncollect(List.of(), false)
////            .correlate(JoinRelType.INNER, id, RexInputRef.of(indexOfField, builder.peek().getRowType()))
////            .project(projectShreddedColumns(rexBuilder, builder.peek()));//, fieldNames(builder.peek()));
////    RelNode node = b.build();
////    return node;
////  }
////
////  public List<RexNode> projectShreddedColumns(RexBuilder rexBuilder,
////                                              RelNode node) {
////    List<RexNode> projects = new ArrayList<>();
////    LogicalCorrelate correlate = (LogicalCorrelate) node;
////    for (int i = 0; i < correlate.getLeft().getRowType().getFieldCount(); i++) {
////      String name = correlate.getLeft().getRowType().getFieldNames().get(i);
////      if (name.equalsIgnoreCase(ReservedName.UUID.getCanonical())) {//|| name.equalsIgnoreCase("_ingest_time")) {
////        projects.add(rexBuilder.makeInputRef(node, i));
////      }
////    }
////
////    //All columns on rhs
////    for (int i = correlate.getLeft().getRowType().getFieldCount();
////         i < correlate.getRowType().getFieldCount(); i++) {
////      projects.add(rexBuilder.makeInputRef(node, i));
////    }
////
////    return projects;
////  }
//}
