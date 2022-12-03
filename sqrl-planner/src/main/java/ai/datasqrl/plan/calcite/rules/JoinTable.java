package com.datasqrl.plan.calcite.rules;

import com.datasqrl.config.util.AbstractPath;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.IndexMap;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.mapping.IntPair;

import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
@ToString
public class JoinTable implements Comparable<JoinTable> {

    final VirtualRelationalTable table;
    final JoinTable parent;
    final JoinRelType joinType;
    final int offset;

    public static JoinTable ofRoot(VirtualRelationalTable.Root root) {
        return new JoinTable(root, null, JoinRelType.INNER,0);
    }

    public int numColumns() {
        return table.getNumQueryColumns();
    }

    public boolean isRoot() {
        return parent == null;
    }

    public boolean containsIndex(int globalIndex) {
        return getLocalIndex(globalIndex)>=0;
    }

    public boolean isJoinCompatible(JoinTable right) {
        //join type can only be inner or left
        return joinType==JoinRelType.INNER || right.joinType==JoinRelType.LEFT;
    }

    public int getLocalIndex(int globalIndex) {
        int localIndex = globalIndex - offset;
        if (localIndex < 0 || localIndex >= numColumns()) return -1;
        return localIndex;
    }

    public int getGlobalIndex(int localIndex) {
        Preconditions.checkArgument(localIndex<numColumns());
        return offset + localIndex;
    }

    public int getNumLocalPk() {
        return table.getNumLocalPks();
    }

    @Override
    public int compareTo(JoinTable o) {
        return Integer.compare(this.offset, o.offset);
    }

    public static List<JoinTable> getRoots(List<JoinTable> joinTables) {
        return joinTables.stream().filter(JoinTable::isRoot).collect(Collectors.toList());
    }

    public static Set<JoinTable> getLeafs(List<JoinTable> joinTables) {
        Set<JoinTable> leafs = new HashSet<>(joinTables);
        joinTables.stream().filter(jt -> jt.parent!=null).forEach(jt -> leafs.remove(jt.parent));
        return leafs;
    }

    public static Optional<JoinTable> find(List<JoinTable> joinTables, int index) {
        return joinTables.stream().filter(jt -> jt.offset<=index && jt.offset+jt.numColumns()>index).findFirst();
    }

    /**
     * This method tries to map
     * @param left
     * @param rightOffset
     * @param right
     * @param equalities
     * @return
     */
    public static Map<JoinTable, JoinTable> joinTreeMap(List<JoinTable> left, int rightOffset, List<JoinTable> right,
                                                        List<IntPair> equalities) {
        if (equalities.isEmpty()) return Collections.EMPTY_MAP;
        equalities = new ArrayList<>(equalities);
        LinkedHashMap<JoinTable, JoinTable> right2Left = new LinkedHashMap<>();
        //By sorting we make sure we can map from root to leaf and therefore that parents are mapped first
        Collections.sort(equalities, (a,b) -> Integer.compare(a.target,b.target));
        Iterator<IntPair> eqIter = equalities.listIterator();
        while (eqIter.hasNext()) {
            IntPair current = eqIter.next();
            JoinTable rt = find(right,current.target-rightOffset).get();
            JoinTable lt = find(left,current.source).get();
            if (rt.table.equals(lt.table) //Make sure tables map onto the same underlying table...
                    && rt.getLocalIndex(current.target - rightOffset) == 0 // and index is first local primary key column
                    && (rt.parent == null || right2Left.containsKey(rt.parent)) //and parent was mapped
                    && lt.isJoinCompatible(rt) //and the join types are compatible
                ) {
                //See if all the primary keys match
                for (int i = 0; i < rt.getNumLocalPk(); i++) {
                    if (current == null) {
                        if (eqIter.hasNext()) current = eqIter.next();
                        else return Collections.EMPTY_MAP;
                    }
                    //Make sure the equality constrains the same local primary key columns in order
                    if (rt.getLocalIndex(current.target - rightOffset)!=i || lt.getLocalIndex(current.source)!=i) {
                        return Collections.EMPTY_MAP;
                    }
                    current = null;
                }
                //Found a match
                right2Left.put(rt,lt);
            } else return Collections.EMPTY_MAP;
        }
        //Map all nested tables with 0 local primary keys (i.e. singleton child tables)
        for (JoinTable rt: right) {
            if (rt.getNumLocalPk()==0 && right2Left.containsKey(rt.parent)) {
                //Find corresponding left side table if exists
                JoinTable leftParent = right2Left.get(rt.parent);
                Optional<JoinTable> lt = left.stream().filter(jt -> jt.table.equals(rt.table) && jt.parent.equals(leftParent)).findFirst();
                if (lt.isPresent()) right2Left.put(rt,lt.get());
            }
        }
        return right2Left;
    }


    public static class Path extends AbstractPath<JoinTable, Path> /*implements IndexMap*/ {

        public static final Path ROOT = new Path();
        private static final Constructor CONSTRUCTOR = new Constructor();

        private Path(JoinTable... tables) {
            super(tables);
        }

        public static Path of(JoinTable table) {
            List<JoinTable> ancestors = new ArrayList<>();
            ancestors.add(table);
            JoinTable parent = table.parent;
            while (parent!=null) {
                ancestors.add(parent);
                parent = parent.parent;
            }
            Collections.reverse(ancestors);
            return Path.CONSTRUCTOR.of(ancestors);
        }

        public IndexMap mapLeafTable() {
            return index -> {
                Preconditions.checkArgument(index>=0);
                int delta = index;
                for (int i = 0; i < size(); i++) {
                    JoinTable t = get(i);
                    int localColLength;
                    if (i < size()-1) { //It's a parent table, only account for local primary keys
                        localColLength = t.getNumLocalPk();
                    } else {
                        localColLength = t.numColumns();
                    }
                    if (delta < localColLength) {
                        return t.offset + delta;
                    } else {
                        delta -= localColLength;
                    }
                }
                throw new IllegalArgumentException(String.format("Index %d out of range of child path",index));
            };
        }

//        public int map(int index) {
//            Preconditions.checkArgument(index>=0);
//            int delta = index;
//            for (int i = 0; i < size(); i++) {
//                JoinTable t = get(i);
//                if ()
//                if (delta < t.numColumns()) {
//                    return t.offset + delta;
//                } else {
//                    delta -= t.numColumns();
//                }
//            }
//            throw new IllegalArgumentException(String.format("Index %d out of range of child path",index));
//        }

        @Override
        protected Constructor constructor() {
            return CONSTRUCTOR;
        }

        private static final class Constructor extends AbstractPath.Constructor<JoinTable, Path> {

            @Override
            protected Path create(@NonNull JoinTable... elements) {
                return new Path(elements);
            }

            @Override
            protected JoinTable[] createArray(int length) {
                return new JoinTable[length];
            }

            @Override
            protected Path root() {
                return ROOT;
            }

        }
    }


}
