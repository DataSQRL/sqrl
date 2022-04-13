package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.SneakyThrows;

public class AliasUtil {

  @SneakyThrows
  public static Name getTableAlias(TableNode tableNode, int i, Supplier<Name> aliasGenerator) {
    if (tableNode.getNamePath().getLength() == 1 && i == 0) {
      return tableNode.getAlias().orElse(tableNode.getNamePath().getFirst());
    }

    if (i == tableNode.getNamePath().getLength() - 1) {
      return tableNode.getAlias().orElseGet(aliasGenerator);
    }

    return aliasGenerator.get();
  }

  public static SelectItem primaryKeySelect(NamePath name, NamePath alias, Column column) {
    Identifier identifier = new Identifier(Optional.empty(), name);
    Identifier aliasIdentifier = new Identifier(Optional.empty(), alias);
    Column ppk = column.copy();
    ppk.setParentPrimaryKey(true);
    ppk.setSource(column);
    identifier.setResolved(ppk);
    aliasIdentifier.setResolved(ppk);
    return new SingleColumn(identifier, aliasIdentifier);
  }
}
