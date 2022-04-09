package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.sqrl.node.PrimaryKeySelectItem;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AliasUtil {

  public static Identifier toIdentifier(Column c, Name alias) {
    return new Identifier(Optional.empty(), alias.toNamePath().concat(c.getId().toNamePath()));
  }

  public static List<Expression> aliasMany(List<Column> list, Name baseTableAlias) {
    return list.stream()
        .map(e->new Identifier(Optional.empty(), baseTableAlias.toNamePath().concat(e.getId())))
        .collect(Collectors.toList());
  }

  public static List<SelectItem> selectAliasItem(List<Column> list,
      Name baseTableAlias, AliasGenerator gen) {
    return toIdentifier(list, baseTableAlias).stream()
        .map(e->new SingleColumn(e, new Identifier(Optional.empty(), gen.nextAliasName().toNamePath())))
        .collect(Collectors.toList());
  }
  public static List<SelectItem> selectAliasItem(List<Column> list,
      Name baseTableAlias) {
    return toIdentifier(list, baseTableAlias).stream()
        .map(e->new SingleColumn(e))
        .collect(Collectors.toList());
  }

  private static List<Expression> toIdentifier(List<Column> list, Name baseTableAlias) {
    return list.stream()
        .map(c->new Identifier(Optional.empty(), baseTableAlias.toNamePath().concat(c.getId())))
        .collect(Collectors.toList());
  }

  public static Name getTableAlias(TableNode tableNode, int i) {
    if (tableNode.getNamePath().getLength() == 1 && i == 0) {
      return tableNode.getAlias().orElse(tableNode.getNamePath().getFirst());
    }

    if (i == tableNode.getNamePath().getLength() - 1) {
      return tableNode.getAlias().orElse(new AliasGenerator().nextTableAliasName());
    }

    return new AliasGenerator().nextTableAliasName();
  }

  public static PrimaryKeySelectItem primaryKeySelect(NamePath name, NamePath alias, Column column) {
    Identifier identifier = new Identifier(Optional.empty(), name);
    Identifier aliasIdentifier = new Identifier(Optional.empty(), alias);
    return new PrimaryKeySelectItem(identifier, aliasIdentifier, column);
  }
}
