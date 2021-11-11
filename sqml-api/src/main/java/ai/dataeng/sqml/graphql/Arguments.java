package ai.dataeng.sqml.graphql;


import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.StringType;
import java.util.List;

interface ArgumentNode {

}

class Arguments {
  List<ArgumentNode> arguments;
}

class VariableArgumentNode implements ArgumentNode {
  String name;
  TypedField field;
}

class LiteralArgumentNode implements ArgumentNode {
  String name;
  TypedField field;
}

class PageSizeArgumentNode implements ArgumentNode {
  String name = "page-size";
  Type type = new IntegerType();
}

class PageArgumentNode implements ArgumentNode {
  String name = "page";
  Type type = new StringType();
}

class OrderByArgumentNode implements ArgumentNode {
  String name = "order-by";
  List<OrderByItem> orders;
}

class OrderByItem {
  TypedField field;
  OrderByDirection direction;
}

class OrderByDirection {
  String DESC = "DESC";
  String ASC = "ASC";
}

interface PagingResponse {

}

// Sqml Paging

class SqmlPagingResponse implements PagingResponse {
  DataContainer data;
  Paging page;
}

class DataContainer {
  String name = "data";
  //List of data columns
}

class Paging {
  String name = "page-state";
  PagingObject pagingObject;
}

class PagingObject {
  HasNextPage hasNextPage;
  NextPage nextPage;
}

class HasNextPage {
  String name = "has-next";
}

class NextPage {
  String name = "next-page";
}
