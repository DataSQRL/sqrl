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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.planner.util.Documented.Documentation;
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
    assertThat(result.columnDocs().get(Name.system("id"))).isEqualTo("The unique identifier");
    assertThat(result.columnDocs().get(Name.system("name"))).isEqualTo("The entity name");
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
    assertThat(result.columnDocs().get(Name.system("id"))).isEqualTo("The unique identifier");
    assertThat(result.columnDocs().get(Name.system("name"))).isEqualTo("The entity name");
  }

  @Test
  void givenSingularColumnHeader_whenParse_thenExtractsColumns() {
    String docString =
        """
        Column
        - id: The unique identifier""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(Name.system("id"))).isEqualTo("The unique identifier");
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
    assertThat(result.argumentDocs().get(Name.system("limit")))
        .isEqualTo("Maximum number of results");
    assertThat(result.argumentDocs().get(Name.system("offset"))).isEqualTo("Starting position");
  }

  @Test
  void givenSingularArgumentHeader_whenParse_thenExtractsArguments() {
    String docString =
        """
        Argument:
        - limit: Maximum number of results""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.argumentDocs()).hasSize(1);
    assertThat(result.argumentDocs().get(Name.system("limit")))
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

        # Columns:
        - orderId: The unique order identifier
        - customerId: Reference to the customer
        - total: Order total amount

        # Arguments
        - status: Filter by order status
        - limit: Maximum results to return""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.docString()).isEqualTo("This table stores customer orders.");
    assertThat(result.columnDocs()).hasSize(3);
    assertThat(result.columnDocs().get(Name.system("orderId")))
        .isEqualTo("The unique order identifier");
    assertThat(result.columnDocs().get(Name.system("customerId")))
        .isEqualTo("Reference to the customer");
    assertThat(result.columnDocs().get(Name.system("total"))).isEqualTo("Order total amount");
    assertThat(result.argumentDocs()).hasSize(2);
    assertThat(result.argumentDocs().get(Name.system("status")))
        .isEqualTo("Filter by order status");
    assertThat(result.argumentDocs().get(Name.system("limit")))
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
    assertThat(result.columnDocs().get(Name.system("id"))).isEqualTo("The identifier");
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
    assertThat(result.columnDocs().get(Name.system("id"))).isEqualTo("The identifier");
    assertThat(result.columnDocs().get(Name.system("name"))).isEqualTo("The name");
  }

  @Test
  void givenHeaderWithDash_whenParse_thenExtractsCorrectly() {
    String docString =
        """
        # Columns -
        - id: The identifier""";

    Documentation result = DocStringParser.parse(docString);

    assertThat(result.columnDocs()).hasSize(1);
    assertThat(result.columnDocs().get(Name.system("id"))).isEqualTo("The identifier");
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
    assertThat(result.columnDocs().get(Name.system("id")))
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
    assertThat(result.columnDocs().get(Name.system("customer_id")))
        .isEqualTo("The customer identifier");
    assertThat(result.columnDocs().get(Name.system("order_date")))
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
    assertThat(result.columnDocs().get(Name.system("id")))
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
    assertThat(result.columnDocs().get(Name.system("orderId")))
        .isEqualTo("Unique identifier for the order");
    assertThat(result.columnDocs().get(Name.system("status")))
        .isEqualTo("Current order status (pending, shipped, delivered)");
    assertThat(result.columnDocs().get(Name.system("createdAt")))
        .isEqualTo("Timestamp when order was created");
    assertThat(result.columnDocs().get(Name.system("totalAmount")))
        .isEqualTo("Total order value in cents");

    assertThat(result.argumentDocs()).hasSize(3);
    assertThat(result.argumentDocs().get(Name.system("customerId")))
        .isEqualTo("The customer to retrieve orders for");
    assertThat(result.argumentDocs().get(Name.system("status")))
        .isEqualTo("Optional status filter");
    assertThat(result.argumentDocs().get(Name.system("limit")))
        .isEqualTo("Maximum number of orders to return (default: 100)");
  }
}
