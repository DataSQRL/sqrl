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

grammar SqlBase;

tokens {
    DELIMITER
}

script
    : ((statement) SEMICOLON?)* EOF
    ;

singleStatement
    : statement EOF
    ;

statement
    : qualifiedName ':=' assignment                                    #assign
    | CREATE SUBSCRIPTION qualifiedName ON subscriptionType AS query   #createSubscription
    | IMPORT importType=(FUNCTION | SOURCE | PUBLIC)? qualifiedName importAlias?     #importStatement
    | query                                                            #statementDefault
    ;

importAlias
    : AS qualifiedName
    ;

assignment
    :  expression                                                   # expressionAssign
    |  query                                                        # queryAssign
    | DISTINCT table=identifier ON (identifier (',' identifier)*)
      (ORDER BY sortItem (',' sortItem)*)?                          # distinctAssignment
    ;

subscriptionType
    : ADD //todo: add other types
    ;

inlineJoin
    : JOIN table=qualifiedName (AS? alias=identifier)?
      (ON expression)?
      (IN qualifiedName)? //todo IN statement
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
      (INVERSE inv=identifier)?
      (inlineJoin)?
    ;

query
    : queryNoWith
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                   #queryPrimaryDefault
    | '(' queryNoWith  ')'                 #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      (FROM relation (',' relation)*)?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    ;

groupingSet
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName '.' ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=relation (joinCriteria)?
      | NATURAL joinType JOIN right=aliasedRelation
      )                                           #joinRelation
    | aliasedRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : qualifiedName                                                   #tableName
    | '(' query ')'                                                   #subqueryRelation
    | '(' relation ')'                                                #parenthesizedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?     #predicated
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN qualifiedName                                               #inRelation
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? IN '(' query ')'                                               #inSubquery
    | NOT? LIKE pattern=valueExpression                                   #like
    | IS NOT? EMPTY                                                       #isEmpty
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression ('?' primaryExpression)?                                        #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | inlineJoin                                                                          #inlineJoinExpr
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | '?'                                                                                 #parameter
    | '(' expression (',' expression)+ ')'                                                #rowConstructor
    | qualifiedName '(' ASTERISK ')'                                                      #functionCall
    | qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')'                #functionCall
    | '(' query ')'                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS '(' query ')'                                                                #exists
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #simpleCase
    | CAST '(' expression AS type ')'                                                     #cast
    | qualifiedName                                                                       #columnReference
    | base=primaryExpression '.' fieldName=identifier                                     #dereference
    | '(' expression ')'                                                                 #parenthesizedExpression
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? expression intervalField
    ;

intervalField
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND
    ;

type
    : type ARRAY
    | ARRAY '<' type '>'
    | MAP '<' type ',' type '>'
    | ROW '(' identifier type (',' identifier type)* ')'
    | baseType ('(' typeParameter (',' typeParameter)* ')')?
    | INTERVAL from=intervalField TO to=intervalField
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : qualifiedName
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

qualifiedName
    : identifier ('.' identifier)* ('.*')?
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | DOUBLE_VALUE   #doubleLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ADD | ADMIN | ALL | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | CALL | CALLED | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT | CURRENT_ROLE
    | DATE | DAY | DEFINER | DESC | DETERMINISTIC | DISTRIBUTED
    | EXCLUDING | EXTERNAL
    | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTION | FUNCTIONS
    | GRANT | GRANTED | GRANTS | GRAPHVIZ
    | HOUR
    | IF | IGNORE | INCLUDING | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LANGUAGE | LAST | LATERAL | LEVEL | LIMIT | LOGICAL
    | MAP | MATERIALIZED | MINUTE | MONTH
    | NAME | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | POSITION | PRECEDING | PRIVILEGES | PROPERTIES
    | RANGE | READ | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNS | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SECURITY | SERIALIZABLE | SESSION | SET | SETS | SQL
    | SHOW | SOME | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEXT | TIMESTAMP | TO | TRANSACTION | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE
    | VALIDATE | VIEW
    | WEEK | WORK | WRITE
    | YEAR
    | ZONE
    ;

ADD: 'ADD';
ADMIN: 'ADMIN';
ALL: 'ALL';
ALTER: 'ALTER';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
BERNOULLI: 'BERNOULLI';
BETWEEN: 'BETWEEN';
BY: 'BY';
CALL: 'CALL';
CALLED: 'CALLED';
CASCADE: 'CASCADE';
CASE: 'CASE';
CAST: 'CAST';
CATALOGS: 'CATALOGS';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
COMMENT: 'COMMENT';
COMMIT: 'COMMIT';
COMMITTED: 'COMMITTED';
CONSTRAINT: 'CONSTRAINT';
CREATE: 'CREATE';
CROSS: 'CROSS';
CUBE: 'CUBE';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_ROLE: 'CURRENT_ROLE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DATE: 'DATE';
DAY: 'DAY';
DEALLOCATE: 'DEALLOCATE';
DEFINER: 'DEFINER';
DELETE: 'DELETE';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DETERMINISTIC: 'DETERMINISTIC';
DISTINCT: 'DISTINCT';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
END: 'END';
ESCAPE: 'ESCAPE';
EXCEPT: 'EXCEPT';
EXCLUDING: 'EXCLUDING';
EXECUTE: 'EXECUTE';
EXISTS: 'EXISTS';
EXTRACT: 'EXTRACT';
EXTERNAL: 'EXTERNAL';
FALSE: 'FALSE';
FILTER: 'FILTER';
FIRST: 'FIRST';
FOLLOWING: 'FOLLOWING';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTION: 'FUNCTION';
FUNCTIONS: 'FUNCTIONS';
GRANT: 'GRANT';
GRANTED: 'GRANTED';
GRANTS: 'GRANTS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
HOUR: 'HOUR';
IF: 'IF';
IGNORE: 'IGNORE';
IN: 'IN';
INCLUDING: 'INCLUDING';
INNER: 'INNER';
INPUT: 'INPUT';
INSERT: 'INSERT';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
INTO: 'INTO';
INVOKER: 'INVOKER';
IO: 'IO';
IS: 'IS';
ISOLATION: 'ISOLATION';
JSON: 'JSON';
JOIN: 'JOIN';
LANGUAGE: 'LANGUAGE';
LAST: 'LAST';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LEVEL: 'LEVEL';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LOCALTIME: 'LOCALTIME';
LOCALTIMESTAMP: 'LOCALTIMESTAMP';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MATERIALIZED: 'MATERIALIZED';
MINUTE: 'MINUTE';
MONTH: 'MONTH';
NAME: 'NAME';
NATURAL: 'NATURAL';
NFC : 'NFC';
NFD : 'NFD';
NFKC : 'NFKC';
NFKD : 'NFKD';
NO: 'NO';
NONE: 'NONE';
NORMALIZE: 'NORMALIZE';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
ON: 'ON';
ONLY: 'ONLY';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
ORDINALITY: 'ORDINALITY';
OUTER: 'OUTER';
OUTPUT: 'OUTPUT';
OVER: 'OVER';
PARTITION: 'PARTITION';
PARTITIONS: 'PARTITIONS';
POSITION: 'POSITION';
PRECEDING: 'PRECEDING';
PREPARE: 'PREPARE';
PRIVILEGES: 'PRIVILEGES';
PROPERTIES: 'PROPERTIES';
PUBLIC: 'PUBLIC';
RANGE: 'RANGE';
READ: 'READ';
RECURSIVE: 'RECURSIVE';
RELATIONSHIP: 'RELATIONSHIP';
RENAME: 'RENAME';
REPEATABLE: 'REPEATABLE';
REPLACE: 'REPLACE';
RESET: 'RESET';
RESPECT: 'RESPECT';
RESTRICT: 'RESTRICT';
RETURN: 'RETURN';
RETURNS: 'RETURNS';
REVOKE: 'REVOKE';
RIGHT: 'RIGHT';
ROLE: 'ROLE';
ROLES: 'ROLES';
ROLLBACK: 'ROLLBACK';
ROLLUP: 'ROLLUP';
ROW: 'ROW';
ROWS: 'ROWS';
SOURCE: 'SOURCE';
SCHEMA: 'SCHEMA';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SECURITY: 'SECURITY';
SELECT: 'SELECT';
SERIALIZABLE: 'SERIALIZABLE';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
SOME: 'SOME';
SQL: 'SQL';
SUBSCRIPTION: 'SUBSCRIPTION';
SUBSTRING: 'SUBSTRING';
SYSTEM: 'SYSTEM';
TABLE: 'TABLE';
TABLES: 'TABLES';
TABLESAMPLE: 'TABLESAMPLE';
TEXT: 'TEXT';
THEN: 'THEN';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRANSACTION: 'TRANSACTION';
TRUE: 'TRUE';
TRY_CAST: 'TRY_CAST';
TYPE: 'TYPE';
UESCAPE: 'UESCAPE';
UNBOUNDED: 'UNBOUNDED';
UNCOMMITTED: 'UNCOMMITTED';
UNION: 'UNION';
USE: 'USE';
USING: 'USING';
VALIDATE: 'VALIDATE';
VALUES: 'VALUES';
VIEW: 'VIEW';
WEEK: 'WEEK';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
WORK: 'WORK';
WRITE: 'WRITE';
YEAR: 'YEAR';
ZONE: 'ZONE';
LEARN: 'LEARN';
PREDICT: 'PREDICT';
IMPORT: 'IMPORT';
SEMICOLON: ';';
INVERSE: 'INVERSE';
FORECAST: 'FORECAST';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

UNICODE_STRING
    : 'U&\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

DOUBLE_VALUE
    : DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_' | '@') (LETTER | DIGIT | '_' | '@' | '-')*
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
