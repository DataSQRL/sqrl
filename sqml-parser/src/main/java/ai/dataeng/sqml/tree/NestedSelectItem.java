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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * SELECT venue { id, * } FROM @;
 */
public class NestedSelectItem
    extends SelectItem {

  private String name;
  private List<SelectItem> columns = new ArrayList<>();

  protected NestedSelectItem(Optional<NodeLocation> location, String name) {
    super(location);
  }
  public NestedSelectItem(String name) {
    super(Optional.empty());
    this.name = name;
  }

  @Override
  public List<? extends Node> getChildren() {
    return List.of();
  }

  public String getName() {
    return name;
  }

  public List<SelectItem> getColumns() {
    return columns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NestedSelectItem that = (NestedSelectItem) o;
    return Objects.equals(name, that.name) && Objects
        .equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, columns);
  }
}
