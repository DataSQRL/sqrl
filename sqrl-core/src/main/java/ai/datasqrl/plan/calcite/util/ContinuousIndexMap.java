package ai.datasqrl.plan.calcite.util;

import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
@Getter
public class ContinuousIndexMap implements IndexMap {

    final int[] targets;
    final int targetLength;

    @Override
    public int map(int index) {
        return targets[index];
    }

    public Mappings.TargetMapping getMapping() {
        List<IntPair> mapping = new ArrayList<>();
        for (int i = 0; i < targets.length; i++) {
            mapping.add(IntPair.of(i,targets[i]));
        }
        return Mappings.target(mapping,targets.length,targetLength);
    }

    public int getSourceLength() {
        return targets.length;
    }

    public ContinuousIndexMap join(ContinuousIndexMap right) {
        int[] combined = new int[targets.length + right.targets.length];
        //Left map doesn't change
        System.arraycopy(targets, 0, combined, 0, targets.length);
        int offset = targets.length;
        for (int i = 0; i < right.targets.length; i++) {
            combined[offset + i] = targetLength + right.targets[i];
        }
        return new ContinuousIndexMap(combined, targetLength + right.targetLength);
    }

    public ContinuousIndexMap remap(int fromIndex, int newTargetLength, IndexMap remap) {
        Builder b = new Builder(targets.length);
        for (int i = 0; i < targets.length; i++) {
            if (i < fromIndex) b.add(map(i));
            else b.add(remap.map(map(i)));
        }
        return b.build(newTargetLength);
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
        return Optional.of(new ContinuousIndexMap(newMap, targetLength));
    }

    public static Builder builder(int length) {
        return new Builder(length);
    }

    public static Builder builder(ContinuousIndexMap base, int addedLength) {
        Builder b = new Builder(base.getSourceLength() + addedLength);
        return b.addAll(base);
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

        public ContinuousIndexMap build(int mapToLength) {
            Preconditions.checkArgument(offset == map.length);
            Preconditions.checkArgument(!Arrays.stream(map).anyMatch(i -> i >= mapToLength));
            return new ContinuousIndexMap(map, mapToLength);
        }


    }

}
