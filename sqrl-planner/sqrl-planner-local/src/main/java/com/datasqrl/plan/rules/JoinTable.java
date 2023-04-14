/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.util.AbstractPath;
import com.google.common.base.Preconditions;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.mapping.IntPair;

@AllArgsConstructor
@Getter
@ToString
public class JoinTable implements Comparable<JoinTable> {

  /**
   * The type of normalization used by a join table
   */
  public enum NormType {
    NORMALIZED, DENORMALIZED;
  }

  final VirtualRelationalTable table;
  final JoinTable parent;
  final JoinRelType joinType;
  final int offset;
  final NormType normType;

  public static JoinTable ofRoot(VirtualRelationalTable.Root root, NormType normType) {
    return new JoinTable(root, null, JoinRelType.INNER, 0, normType);
  }

  public int numColumns() {
    return normType==NormType.DENORMALIZED?table.getNumQueryColumns():table.getNumColumns();
  }

  public JoinTable withOffset(int newOffset) {
    return new JoinTable(table, parent, joinType, newOffset, normType);
  }

  public boolean isRoot() {
    return parent == null;
  }

  public boolean containsIndex(int globalIndex) {
    return getLocalIndex(globalIndex) >= 0;
  }

  public boolean isJoinCompatible(JoinTable right) {
    //join type can only be inner or left
    return joinType == JoinRelType.INNER || right.joinType == JoinRelType.LEFT;
  }

  public int getLocalIndex(int globalIndex) {
    int localIndex = globalIndex - offset;
    if (localIndex < 0 || localIndex >= numColumns()) {
      return -1;
    }
    return localIndex;
  }

  public int getGlobalIndex(int localIndex) {
    Preconditions.checkArgument(localIndex < numColumns());
    return offset + localIndex;
  }

  public int getNumLocalPk() {
    return table.getNumLocalPks();
  }

  public int getNumPkNormalized() { return table.getNumPrimaryKeys(); }

  @Override
  public int compareTo(JoinTable o) {
    return Integer.compare(this.offset, o.offset);
  }

  public static List<JoinTable> getRoots(List<JoinTable> joinTables) {
    return joinTables.stream().filter(JoinTable::isRoot).collect(Collectors.toList());
  }

  public static Set<JoinTable> getLeafs(List<JoinTable> joinTables) {
    Set<JoinTable> leafs = new HashSet<>(joinTables);
    joinTables.stream().filter(jt -> jt.parent != null).forEach(jt -> leafs.remove(jt.parent));
    return leafs;
  }

  public static Optional<JoinTable> find(List<JoinTable> joinTables, int index) {
    return joinTables.stream()
        .filter(jt -> jt.offset <= index && jt.offset + jt.numColumns() > index).findFirst();
  }

  public static boolean valid(List<JoinTable> tables) {
    return tables!=null && !tables.isEmpty();
  }

  public static boolean compatible(List<JoinTable> left, List<JoinTable> right) {
    if (!valid(left) || !valid(right)) return false;
    Set<VirtualRelationalTable.Root> rightRoots = right.stream().map(jt -> jt.table.getRoot()).collect(
        Collectors.toSet());
    return left.stream().map(jt -> jt.table.getRoot()).anyMatch(rightRoots::contains);
  }

  public static Map<JoinTable, JoinTable> joinListMap(List<JoinTable> left, int rightOffset,
      List<JoinTable> right, List<IntPair> equalities) {
    Preconditions.checkArgument(!equalities.isEmpty() && valid(left) && valid(right));
    Preconditions.checkArgument(Stream.concat(left.stream(),right.stream())
        .allMatch(jt -> jt.normType == NormType.NORMALIZED));
    Preconditions.checkArgument(right.size()==1); //Assumption checked prior
    JoinTable rightTbl = Iterables.getOnlyElement(right);
    //Check if equality condition are on primary key prefix for rightTbl
    if (!isPKPrefixConstraint(rightTbl,equalities, p -> p.target, rightOffset)) {
      return Collections.EMPTY_MAP;
    }
    //Find join table from left matches the constraints on pk prefix
    for (JoinTable leftTbl : left) {
      if (isPKPrefixConstraint(leftTbl, equalities, p -> p.source, 0)
          && leftTbl.isJoinCompatible(rightTbl)) {
        Map<JoinTable, JoinTable> result = new HashMap<>(1);
        result.put(rightTbl, leftTbl);
        return result;
      }
    }
    return Collections.EMPTY_MAP;
  }

  private static boolean isPKPrefixConstraint(JoinTable tbl, List<IntPair> equalities,
      Function<IntPair,Integer> getIdx, int offset) {
    List<Integer> pkList = ContiguousSet.closedOpen(0,
            Math.min(tbl.getNumPkNormalized(),equalities.size())).stream()
        .map(idx -> idx + tbl.offset + offset) //offset to actual index in relnode
        .collect(Collectors.toList());
    List<Integer> equalityList = equalities.stream().map(getIdx).sorted().collect(Collectors.toList());
    return pkList.equals(equalityList);
  }

  /**
   * This method tries to map the join tables from two join trees onto each other
   * if those trees are rooted in the same table.
   *
   * It assumes that nested tables are denormalized.
   *
   * @param left
   * @param rightOffset
   * @param right
   * @param equalities
   * @return
   */
  public static Map<JoinTable, JoinTable> joinTreeMap(List<JoinTable> left, int rightOffset,
      List<JoinTable> right, List<IntPair> equalities) {
    Preconditions.checkArgument(!equalities.isEmpty() && valid(left) && valid(right));
    Preconditions.checkArgument(Stream.concat(left.stream(),right.stream())
        .allMatch(jt -> jt.normType == NormType.DENORMALIZED));
    equalities = new ArrayList<>(equalities);
    LinkedHashMap<JoinTable, JoinTable> right2Left = new LinkedHashMap<>();
    //By sorting we make sure we can map from root to leaf and therefore that parents are mapped first
    Collections.sort(equalities, (a, b) -> Integer.compare(a.target, b.target));
    Iterator<IntPair> eqIter = equalities.listIterator();
    while (eqIter.hasNext()) {
      IntPair current = eqIter.next();
      JoinTable rt = find(right, current.target - rightOffset).get();
      JoinTable lt = find(left, current.source).get();
      if (rt.table.equals(lt.table) //Make sure tables map onto the same underlying table...
          && rt.getLocalIndex(current.target - rightOffset) == 0
          // and index is first local primary key column
          && (rt.parent == null || right2Left.containsKey(rt.parent)) //and parent was mapped
          && lt.isJoinCompatible(rt) //and the join types are compatible
      ) {
        //See if all the primary keys match
        for (int i = 0; i < rt.getNumLocalPk(); i++) {
          if (current == null) {
            if (eqIter.hasNext()) {
              current = eqIter.next();
            } else {
              return Collections.EMPTY_MAP;
            }
          }
          //Make sure the equality constrains the same local primary key columns in order
          if (rt.getLocalIndex(current.target - rightOffset) != i
              || lt.getLocalIndex(current.source) != i) {
            return Collections.EMPTY_MAP;
          }
          current = null;
        }
        //Found a match
        right2Left.put(rt, lt);
      } else {
        return Collections.EMPTY_MAP;
      }
    }
    //Map all nested tables with 0 local primary keys (i.e. singleton child tables)
    for (JoinTable rt : right) {
      if (rt.getNumLocalPk() == 0 && right2Left.containsKey(rt.parent)) {
        //Find corresponding left side table if exists
        JoinTable leftParent = right2Left.get(rt.parent);
        Optional<JoinTable> lt = left.stream()
            .filter(jt -> jt.table.equals(rt.table) && jt.parent.equals(leftParent)).findFirst();
        if (lt.isPresent()) {
          right2Left.put(rt, lt.get());
        }
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
      while (parent != null) {
        ancestors.add(parent);
        parent = parent.parent;
      }
      Collections.reverse(ancestors);
      return Path.CONSTRUCTOR.of(ancestors);
    }

    public IndexMap mapLeafTable() {
      return index -> {
        Preconditions.checkArgument(index >= 0);
        int delta = index;
        for (int i = 0; i < size(); i++) {
          JoinTable t = get(i);
          int localColLength;
          if (i < size() - 1) { //It's a parent table, only account for local primary keys
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
        throw new IllegalArgumentException(
            String.format("Index %d out of range of child path", index));
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
