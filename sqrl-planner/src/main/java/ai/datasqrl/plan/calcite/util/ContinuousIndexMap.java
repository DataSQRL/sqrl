package com.datasqrl.plan.calcite.util;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class ContinuousIndexMap implements IndexMap {

    public static final ContinuousIndexMap EMPTY = new ContinuousIndexMap(new int[0]);

    final int[] targets;

    @Override
    public int map(int index) {
        return targets[index];
    }

    public int getSourceLength() {
        return targets.length;
    }

    public List<Pair> getMapping() {
        return IntStream.range(0,targets.length).mapToObj(i -> new Pair(i,targets[i])).collect(Collectors.toList());
    }

    public int[] targetsAsArray() {
        return targets.clone();
    }

    public ImmutableBitSet targetsAsBitSet() {
        return ImmutableBitSet.of(targetsAsArray());
    }

    public List<Integer> targetsAsList() {
        return Ints.asList(targetsAsArray());
    }

    public IndexMap inverseMap() {
        return IndexMap.of(getMapping().stream().collect(Collectors.toMap(Pair::getTarget,Pair::getSource)));
    }

    public boolean containsTarget(int targetIndex) {
        return ArrayUtils.contains(targets,targetIndex);
    }

    public ContinuousIndexMap join(ContinuousIndexMap right, int leftSideWidth) {
        int[] combined = new int[targets.length + right.targets.length];
        //Left map doesn't change
        System.arraycopy(targets, 0, combined, 0, targets.length);
        int offset = targets.length;
        for (int i = 0; i < right.targets.length; i++) {
            combined[offset + i] = leftSideWidth + right.targets[i];
        }
        return new ContinuousIndexMap(combined);
    }

    public ContinuousIndexMap append(ContinuousIndexMap add) {
        int[] combined = new int[targets.length + add.targets.length];
        System.arraycopy(targets, 0, combined, 0, targets.length);
        System.arraycopy(add.targets, 0, combined, targets.length, add.targets.length);
        return new ContinuousIndexMap(combined);
    }

    public ContinuousIndexMap remap(IndexMap remap) {
        Builder b = new Builder(targets.length);
        for (int i = 0; i < targets.length; i++) {
            b.add(remap.map(map(i)));
        }
        return b.build();
    }

    public Optional<ContinuousIndexMap> project(LogicalProject project) {
        List<RexNode> projects = project.getProjects();
        int[] newMap = new int[projects.size()];
        for (int i = 0; i < projects.size(); i++) {
            RexNode exp = projects.get(i);
            if (exp instanceof RexInputRef) {
                newMap[i] = map(((RexInputRef) exp).getIndex());
            } else return Optional.empty(); //This is not a re-mapping projection, hence abort
        }
        return Optional.of(new ContinuousIndexMap(newMap));
    }

    public static Builder builder(int length) {
        return new Builder(length);
    }

    public static Builder builder(ContinuousIndexMap base, int addedLength) {
        Builder b = new Builder(base.getSourceLength() + addedLength);
        return b.addAll(base);
    }

    public static ContinuousIndexMap identity(int numSources, int targetLength) {
        Builder b = builder(numSources);
        for (int i = 0; i < numSources; i++) {
            b.add(i);
        }
        return b.build(targetLength);
    }

    public static ContinuousIndexMap of(Collection<Integer> indexes) {
        return builder(indexes.size()).addAll(indexes).build();
    }

    public static final class Builder {

        final int[] map;
        int offset = 0;

        private Builder(int length) {
            this.map = new int[length];
        }

        public Builder addAll(ContinuousIndexMap indexMap) {
            for (int i = 0; i < indexMap.getSourceLength(); i++) {
                add(indexMap.map(i));
            }
            return this;
        }

        public Builder addAll(Iterable<Integer> mapTo) {
            mapTo.forEach(i-> add(i));
            return this;
        }

        public Builder add(int mapTo) {
            Preconditions.checkArgument(offset < map.length);
            map[offset] = mapTo;
            offset++;
            return this;
        }

        public Builder set(int index, int mapTo) {
            Preconditions.checkArgument(index >= 0 && index < map.length);
            map[index] = mapTo;
            offset = Math.max(offset, index + 1);
            return this;
        }

        public ContinuousIndexMap build(int targetLength) {
            Preconditions.checkArgument(!Arrays.stream(map).anyMatch(i -> i >= targetLength));
            return build();
        }

        public ContinuousIndexMap build() {
            Preconditions.checkArgument(offset == map.length);
            return new ContinuousIndexMap(map);
        }


    }

}
