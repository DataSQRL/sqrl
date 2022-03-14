package org.apache.calcite.tools;

import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator2.SqrlDistinct;
import ai.dataeng.sqml.planner.operator2.SqrlRelNode;
import ai.dataeng.sqml.planner.operator2.SqrlTableScan;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.SortItem.Ordering;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;

public class SqrlRelBuilder extends RelBuilder {

	public SqrlRelBuilder(Context context, RelOptCluster cluster,
			RelOptSchema relOptSchema) {
		super(context, cluster, relOptSchema);
	}

	/**
	 * Gets a table without added fields
	 */
	public SqrlRelBuilder scan_base(String name) {
		SqrlCalciteCatalogReader schema = (SqrlCalciteCatalogReader) getRelOptSchema();
		TableScan scan = SqrlTableScan.create(cluster, schema.getTable(List.of(name)), List.of());
		push(scan);
		return this;
	}

	public SqrlRelBuilder distinctOn(List<Name> partitionKeys, List<SortItem> order) {
		SqrlRelNode node = (SqrlRelNode)this.peek();
		RelNode relNode = this.peek();

		List<RexInputRef> partition = new ArrayList<>();
		for (Name name : partitionKeys) {
			int index = getIndex(node.getFields(), name);
			Preconditions.checkState(index != -1, "Could not find key: " + name);
			partition.add(RexInputRef.of(index, relNode.getRowType()));
		}

		List<RelFieldCollation> collation = order.stream()
				.map(f-> toOrder(node, f))
				.collect(Collectors.toList());
		RelCollation orderCollation = RelCollations.of(collation);

		//Clears stack
		this.clear();
		push(SqrlDistinct.create(relNode, List.of(), partitionKeys, order, partition, orderCollation));
		return this;
	}

	public static int getIndex(List<Field> fields, Name name) {
		for (int i = 0; i < fields.size(); i++) {
			Field field = fields.get(i);
			if (field.getName().equals(name)) {
				return i;
			}
		}
		return -1;
	}

	private RelFieldCollation toOrder(SqrlRelNode node, SortItem order) {
		String nameStr = ((Identifier)order.getSortKey()).getValue();
		Name name = Name.system(nameStr);
		int index = getIndex(node.getFields(), name);
		Preconditions.checkState(index != -1, "Could not find order: " + order);

		Direction direction = order.getOrdering()
				.map(o-> o == Ordering.ASCENDING ?
						Direction.ASCENDING : Direction.DESCENDING)
				.orElse(Direction.ASCENDING);

		return new RelFieldCollation(index, direction);
	}

	/**
	 * For injecting the relational builder into the SqlToRel converter
	 */
	public RelBuilder transform(UnaryOperator<Config> transform) {
//		final Context context =
//				Contexts.of(struct, transform.apply(config));
		return this;
	}
}