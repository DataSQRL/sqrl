/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.tree;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Select
    extends Node {

  private boolean distinct;
//  private Optional<DistinctOn> distinctOn;
  private List<SelectItem> selectItems;

  public Select(boolean distinct, List<SelectItem> selectItems) {
    this(Optional.empty(), distinct, selectItems);
  }

  public Select(List<SelectItem> selectItems) {
    this(Optional.empty(), false, selectItems);
  }

  public Select(NodeLocation location, boolean distinct, List<SelectItem> selectItems) {
    this(Optional.of(location), distinct, selectItems);
  }

  public Select(Optional<NodeLocation> location, boolean distinct,
      List<SelectItem> selectItems) {
    super(location);
    this.distinct = distinct;
//    this.distinctOn = distinctOn;
    this.selectItems = ImmutableList.copyOf(requireNonNull(selectItems, "selectItems"));
  }

  public boolean isDistinct() {
    return distinct;
  }

  public List<SelectItem> getSelectItems() {
    return selectItems;
  }

//  public Optional<DistinctOn> getDistinctOn() {
//    return distinctOn;
//  }

  public void setDistinct(boolean distinct) {
    this.distinct = distinct;
  }

//  public void setDistinctOn(Optional<DistinctOn> distinctOn) {
//    this.distinctOn = distinctOn;
//  }

  public void setSelectItems(List<SelectItem> selectItems) {
    this.selectItems = selectItems;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSelect(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return selectItems;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("distinct", distinct)
        .add("selectItems", selectItems)
        .omitNullValues()
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Select select = (Select) o;
    return (distinct == select.distinct) &&
        Objects.equals(selectItems, select.selectItems);
  }

  @Override
  public int hashCode() {
    return Objects.hash(distinct, selectItems);
  }
}
