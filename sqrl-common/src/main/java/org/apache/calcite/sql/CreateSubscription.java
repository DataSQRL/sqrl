package org.apache.calcite.sql;

import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class CreateSubscription extends Assignment {

  private final SubscriptionType subscriptionType;
  private final SqlIdentifier name;
  private final SqlNode query;

  public CreateSubscription(SqlParserPos location, SubscriptionType subscriptionType,
      SqlIdentifier name, SqlNode query) {
    super(location, name, Optional.empty());
    this.subscriptionType = subscriptionType;
    this.name = name;
    this.query = query;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateSubscription that = (CreateSubscription) o;
    return subscriptionType == that.subscriptionType && Objects.equals(name, that.name)
        && Objects.equals(query, that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionType, name, query);
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public String toString() {
    return "CreateSubscription{" +
        "subscriptionType=" + subscriptionType +
        ", name=" + name +
        ", query=" + query +
        '}';
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {

  }

  @Override
  public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {

  }

  @Override
  public <R> R accept(SqlVisitor<R> sqlVisitor) {
    return null;
  }

  @Override
  public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
    return false;
  }
}
