/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
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
package com.datasqrl.planner.hint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DocStringParserTest {

  @Test
  void givenNullDocString_whenParse_thenReturnsEmptyDocumentation() {
    Documentation result = DocStringParser.parse(null);

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs()).isEmpty();
    assertThat(result.argumentDocs()).isEmpty();
  }

  @Test
  void givenEmptyDocString_whenParse_thenReturnsEmptyDocumentation() {
    Documentation result = DocStringParser.parse("");

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs()).isEmpty();
    assertThat(result.argumentDocs()).isEmpty();
  }

  @Test
  void givenBlankDocString_whenParse_thenReturnsEmptyDocumentation() {
    Documentation result = DocStringParser.parse("   \n\t  ");

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs()).isEmpty();
    assertThat(result.argumentDocs()).isEmpty();
  }

  @Test
  void givenSimpleDescription_whenParse_thenReturnsDescriptionOnly() {
    String docString = "This is a simple table description.";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isEqualTo("This is a simple table description.");
    assertThat(result.columnDocs()).isEmpty();
    assertThat(result.argumentDocs()).isEmpty();
  }

  @Test
  void givenMultiLineDescription_whenParse_thenPreservesNewlines() {
    String docString =
        """
        This is line one.
        This is line two.

        This is after a blank line.""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).contains("This is line one.");
    assertThat(result.docString()).contains("This is line two.");
    assertThat(result.docString()).contains("This is after a blank line.");
  }

  @Test
  void givenColumnsWithHashHeader_whenParse_thenExtractsColumns() {
    String docString =
        """
        # Columns
        - id: The unique identifier
        - name: The entity name""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs()).hasSize(2);
    assertThat(result.columnDocs().get(NamePath.of("id"))).isEqualTo("The unique identifier");
    assertThat(result.columnDocs().get(NamePath.of("name"))).isEqualTo("The entity name");
    assertThat(result.argumentDocs()).isEmpty();
  }

  @Test
  void givenColumnsWithColonHeader_whenParse_thenExtractsColumns() {
    String docString =
        """
        Columns:
        - id: The unique identifier
        - name: The entity name""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(2);
    assertThat(result.columnDocs().get(NamePath.of("id"))).isEqualTo("The unique identifier");
    assertThat(result.columnDocs().get(NamePath.of("name"))).isEqualTo("The entity name");
  }

  @Test
  void givenSingularColumnHeader_whenParse_thenExtractsColumns() {
    String docString =
        """
        Column
        - id: The unique identifier""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(NamePath.of("id"))).isEqualTo("The unique identifier");
  }

  @Test
  void givenArgumentsWithHashHeader_whenParse_thenExtractsArguments() {
    String docString =
        """
        # Arguments
        - limit: Maximum number of results
        - offset: Starting position""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs()).isEmpty();
    assertThat(result.argumentDocs()).hasSize(2);
    assertThat(result.argumentDocs().get(NamePath.of("limit")))
        .isEqualTo("Maximum number of results");
    assertThat(result.argumentDocs().get(NamePath.of("offset"))).isEqualTo("Starting position");
  }

  @Test
  void givenSingularArgumentHeader_whenParse_thenExtractsArguments() {
    String docString =
        """
        Argument:
        - limit: Maximum number of results""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.argumentDocs()).hasSize(1);
    assertThat(result.argumentDocs().get(NamePath.of("limit")))
        .isEqualTo("Maximum number of results");
  }

  @Test
  void givenMixedCaseHeaders_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        # COLUMNS
        - id: The ID

        # ARGUMENTS
        - limit: The limit""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.argumentDocs()).hasSize(1);
  }

  @Test
  void givenDescriptionWithColumnsAndArguments_whenParse_thenExtractsAll() {
    String docString =
        """
        This table stores customer orders.

        ## Columns:
        - orderId: The unique order identifier
        - customerId: Reference to the customer
        - total: Order total amount

        ### Arguments
        - status: Filter by order status
        - limit: Maximum results to return""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isEqualTo("This table stores customer orders.");
    assertThat(result.columnDocs()).hasSize(3);
    assertThat(result.columnDocs().get(NamePath.of("orderId")))
        .isEqualTo("The unique order identifier");
    assertThat(result.columnDocs().get(NamePath.of("customerId")))
        .isEqualTo("Reference to the customer");
    assertThat(result.columnDocs().get(NamePath.of("total"))).isEqualTo("Order total amount");
    assertThat(result.argumentDocs()).hasSize(2);
    assertThat(result.argumentDocs().get(NamePath.of("status")))
        .isEqualTo("Filter by order status");
    assertThat(result.argumentDocs().get(NamePath.of("limit")))
        .isEqualTo("Maximum results to return");
  }

  @Test
  void givenColumnsBeforeDescription_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        # Columns
        - id: The identifier

        This is the table description that comes after.""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isEqualTo("This is the table description that comes after.");
    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(NamePath.of("id"))).isEqualTo("The identifier");
  }

  @Test
  void givenDescriptionBetweenSections_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        Initial description.

        # Columns
        - id: The ID

        Middle description.

        # Arguments
        - limit: The limit

        Final description.""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).contains("Initial description.");
    assertThat(result.docString()).contains("Middle description.");
    assertThat(result.docString()).contains("Final description.");
    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.argumentDocs()).hasSize(1);
  }

  @Test
  void givenAsteriskListItems_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        # Columns
        * id: The identifier
        * name: The name""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(2);
    assertThat(result.columnDocs().get(NamePath.of("id"))).isEqualTo("The identifier");
    assertThat(result.columnDocs().get(NamePath.of("name"))).isEqualTo("The name");
  }

  @Test
  void givenHeaderWithDash_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        # Columns -
        - id: The identifier""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(NamePath.of("id"))).isEqualTo("The identifier");
  }

  @Test
  void givenHeaderWithoutHashSymbol_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        Columns:
        - id: The identifier

        Arguments:
        - limit: The limit""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.argumentDocs()).hasSize(1);
  }

  @Test
  void givenExtraWhitespaceInListItems_whenParse_thenTrimsCorrectly() {
    String docString =
        """
        # Columns
        -   id  :   The identifier with extra spaces  """;

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(NamePath.of("id")))
        .isEqualTo("The identifier with extra spaces");
  }

  @Test
  void givenDescriptionWithColonButNotListItem_whenParse_thenPreservesAsDescription() {
    String docString = "This table has a colon: but it's not a column definition.";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString())
        .isEqualTo("This table has a colon: but it's not a column definition.");
    assertThat(result.columnDocs()).isEmpty();
  }

  @Test
  void givenOnlyColumnsSectionNoDescription_whenParse_thenDocStringIsNull() {
    String docString =
        """
        # Columns
        - id: The identifier
        - name: The name""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs()).hasSize(2);
  }

  @Test
  void givenUnderscoreInColumnName_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        # Columns
        - customer_id: The customer identifier
        - order_date: When the order was placed""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(2);
    assertThat(result.columnDocs().get(NamePath.of("customer_id")))
        .isEqualTo("The customer identifier");
    assertThat(result.columnDocs().get(NamePath.of("order_date")))
        .isEqualTo("When the order was placed");
  }

  @Test
  void givenColonInDescription_whenParse_thenPreservesFullDescription() {
    String docString =
        """
        # Columns
        - id: The identifier: this includes colons in the description""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(NamePath.of("id")))
        .isEqualTo("The identifier: this includes colons in the description");
  }

  @Test
  void givenIndentedHeader_whenParse_thenExtractsCorrectly() {
    String docString =
        """
          # Columns
          - id: The identifier""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
  }

  @Test
  void givenRealWorldExample_whenParse_thenExtractsAllParts() {
    String docString =
        """
        Retrieves order details for a customer with optional filtering.

        This function returns all orders associated with the given customer,
        optionally filtered by status and date range.

        # Columns:
        - orderId: Unique identifier for the order
        - status: Current order status (pending, shipped, delivered)
        - createdAt: Timestamp when order was created
        - totalAmount: Total order value in cents

        # Arguments:
        - customerId: The customer to retrieve orders for
        - status: Optional status filter
        - limit: Maximum number of orders to return (default: 100)""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString())
        .contains("Retrieves order details for a customer with optional filtering.");
    assertThat(result.docString())
        .contains("This function returns all orders associated with the given customer");

    assertThat(result.columnDocs()).hasSize(4);
    assertThat(result.columnDocs().get(NamePath.of("orderId")))
        .isEqualTo("Unique identifier for the order");
    assertThat(result.columnDocs().get(NamePath.of("status")))
        .isEqualTo("Current order status (pending, shipped, delivered)");
    assertThat(result.columnDocs().get(NamePath.of("createdAt")))
        .isEqualTo("Timestamp when order was created");
    assertThat(result.columnDocs().get(NamePath.of("totalAmount")))
        .isEqualTo("Total order value in cents");

    assertThat(result.argumentDocs()).hasSize(3);
    assertThat(result.argumentDocs().get(NamePath.of("customerId")))
        .isEqualTo("The customer to retrieve orders for");
    assertThat(result.argumentDocs().get(NamePath.of("status")))
        .isEqualTo("Optional status filter");
    assertThat(result.argumentDocs().get(NamePath.of("limit")))
        .isEqualTo("Maximum number of orders to return (default: 100)");
  }

  @Test
  void givenNestedColumnList_whenParse_thenDocumentsNestedFieldsToAnyDepth() {
    var docString =
        """
        Customer profiles

        # Columns:
        - id: The customer id
        - address: The postal address
          - street: The street and house number
          - geo: The location
            - lat: The latitude
            - lon: The longitude
          - city: The city
        - orders: The orders
          - total: The order total""";

    var result = DocStringParser.parse(docString);

    assertThat(result.docString()).isEqualTo("Customer profiles");
    assertThat(result.columnDocs())
        .containsExactly(
            entry(NamePath.of("id"), "The customer id"),
            entry(NamePath.of("address"), "The postal address"),
            entry(NamePath.of("address", "street"), "The street and house number"),
            entry(NamePath.of("address", "geo"), "The location"),
            entry(NamePath.of("address", "geo", "lat"), "The latitude"),
            entry(NamePath.of("address", "geo", "lon"), "The longitude"),
            entry(NamePath.of("address", "city"), "The city"),
            entry(NamePath.of("orders"), "The orders"),
            entry(NamePath.of("orders", "total"), "The order total"));
  }

  @Test
  void givenNestedArgumentList_whenParse_thenDocumentsNestedArguments() {
    var docString =
        """
        # Arguments:
        - filter: The filter
          - minTotal: The minimum total
        - limit: The maximum number of results""";

    var result = DocStringParser.parse(docString);

    assertThat(result.argumentDocs())
        .containsExactly(
            entry(NamePath.of("filter"), "The filter"),
            entry(NamePath.of("filter", "minTotal"), "The minimum total"),
            entry(NamePath.of("limit"), "The maximum number of results"));
  }

  @Test
  void givenNestedListIndentedWithTabs_whenParse_thenNestsByIndentation() {
    var docString = "# Columns:\n- address: The address\n\t- street: The street\n- id: The id";

    var result = DocStringParser.parse(docString);

    assertThat(result.columnDocs())
        .containsOnlyKeys(
            NamePath.of("address"), NamePath.of("address", "street"), NamePath.of("id"));
  }

  @Test
  void givenListWithoutBullets_whenParse_thenDocumentsColumns() {
    var docString =
        """
        # Columns:
        id: The customer id
        address: The address
          street: The street
        email: The contact email""";

    var result = DocStringParser.parse(docString);

    assertThat(result.docString()).isNull();
    assertThat(result.columnDocs())
        .containsExactly(
            entry(NamePath.of("id"), "The customer id"),
            entry(NamePath.of("address"), "The address"),
            entry(NamePath.of("address", "street"), "The street"),
            entry(NamePath.of("email"), "The contact email"));
  }

  @Test
  void givenLineWithoutBulletAfterBulletedList_whenParse_thenItIsDescription() {
    var docString = "Customers\n# Columns:\n- id: The customer id\nNote: ids are unique";

    var result = DocStringParser.parse(docString);

    assertThat(result.docString()).isEqualTo("Customers\nNote: ids are unique");
    assertThat(result.columnDocs()).containsOnlyKeys(NamePath.of("id"));
  }

  @Test
  void givenStarListItems_whenParse_thenKeepsThemAsItems() {
    var docString = "Customers\n# Columns:\n* id: The id\n* name: The name";

    var result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).containsOnlyKeys(NamePath.of("id"), NamePath.of("name"));
  }

  @Test
  void givenNestedColumnDocs_whenLookingUpNestedFields_thenFindsThem() {
    var result =
        DocStringParser.parse("# Columns:\n- address: The address\n  - street: The street");

    var lookup = result.getColumnLookup();

    assertThat(lookup.getDocsFor("address")).contains("The address");
    assertThat(lookup.nested("address").getDocsFor("street")).contains("The street");
    assertThat(lookup.getDocsFor("street")).isEmpty();
    assertThat(lookup.nested("address").nested("street").getDocsFor("any")).isEmpty();
    assertThat(result.getColumnOpt("address")).contains("The address");
  }

  @Test
  void givenColumnComments_whenAddedIfAbsent_thenDocStringTakesPrecedence() {
    var result =
        DocStringParser.parse("# Columns:\n- email: The contact email")
            .withColumnDocsIfAbsent(Map.of("email", "Overridden", "phone", "The phone number"));

    assertThat(result.getColumnOpt("email")).contains("The contact email");
    assertThat(result.getColumnOpt("phone")).contains("The phone number");
  }

  @Test
  void givenNoDocString_whenColumnDocsAddedIfAbsent_thenTheyDocumentTheColumns() {
    var result = Documented.EMPTY.withColumnDocsIfAbsent(Map.of("phone", "The phone number"));

    assertThat(result.docString()).isNull();
    assertThat(result.getColumnOpt("phone")).contains("The phone number");
  }
}
