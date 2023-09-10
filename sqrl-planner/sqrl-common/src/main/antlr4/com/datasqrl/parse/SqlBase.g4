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
    : importDefinition                               #importStatement
    | exportDefinition                               #exportStatement
    | assignmentPath ':=' FROM fromDeclaration       #fromQuery
    | assignmentPath ':=' JOIN joinDeclaration       #joinQuery
    | assignmentPath ':=' STREAM streamQuerySpec     #streamQuery
    | assignmentPath ':=' DISTINCT distinctQuerySpec #distinctQuery
    | assignmentPath ':=' query                      #sqlQuery
    | assignmentPath ':=' expression                 #expressionQuery
    ;

assignmentPath
   : hint? qualifiedName tableFunctionDef?
   ;

importDefinition
   : IMPORT qualifiedName (AS? alias=identifier)? (TIMESTAMP expression (AS timestampAlias=identifier)?)?
   ;

exportDefinition
   : EXPORT qualifiedName TO qualifiedName
   ;

joinDeclaration
    : first=aliasedRelation (firstCondition=joinCondition)?
      (remainingJoins)*
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=INTEGER_VALUE)?
    ;

remainingJoins
    : joinType JOIN aliasedRelation (joinCondition)?
    ;

fromDeclaration
    : relation
      (WHERE where=booleanExpression)?
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=INTEGER_VALUE)?
    ;

distinctQuerySpec
   : identifier
     ON onExpr
     (ORDER BY orderExpr=expression ordering=DESC?)?
   ;

onExpr
   : '('? selectItem (',' selectItem)* ')'?;

streamQuerySpec
    : ON subscriptionType AS query;

subscriptionType
    : ADD
    | DELETE
    | UPDATE
    ;

query
    : queryTerm
      (ORDER BY sortItem (',' sortItem)*)?
      (LIMIT limit=INTEGER_VALUE)?
    ;

queryTerm
    : queryPrimary                                                                        #queryTermDefault
    | left=queryTerm operator=(UNION | INTERSECT | EXCEPT) setQuantifier right=queryTerm  #setOperation
    ;

queryPrimary
    : (querySpecification | subquery)
    ;

sortItem
    : expression ordering=(ASC | DESC)?
    ;

querySpecification
    : SELECT hint? setQuantifier? selectItem (',' selectItem)*
      FROM relation (',' relation)*
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

groupBy
    : expression (',' expression)*
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=aliasedRelation
      | joinType JOIN rightRelation=aliasedRelation (joinCondition)?
      )                                           #joinRelation
    | aliasedRelation                             #relationDefault
    ;

joinType
    : INNER?
    | TEMPORAL?
    | INTERVAL?
    | LEFT  (OUTER|TEMPORAL|INTERVAL)?
    | RIGHT (OUTER|TEMPORAL|INTERVAL)?
    ;

joinCondition
    : ON booleanExpression
    ;

aliasedRelation
    : relationPrimary (AS? identifier)?
    ;

relationPrimary
    : relationItem ('.' relationItem)*
    ;

relationItem
    : subquery
    | identifier
    | tableFunction
    ;

tableFunction
    : identifier tableFunctionExpression?
    ;

subquery
    : '(' query ')';

tableFunctionExpression
   : '(' expression (',' expression)* ')'
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
    | NOT? IN '(' expression (',' expression)* ')'                        #inList
    | NOT? LIKE pattern=valueExpression                                   #like
    | NOT? IN '(' query ')'                                               #inSubquery
    | IS NOT? NULL                                                        #nullPredicate
    ;

valueExpression
    : primaryExpression ('?' primaryExpression)?                                        #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    ;

primaryExpression
    : literal                                                                                #literalExpression
    | qualifiedName                                                                          #columnReference
    | identifier '(' (setQuantifier? expression (',' expression)*)? ')'                      #functionCall
    | identifier '(' ASTERISK ')'                                                            #functionCall
    | identifier '(' query ')'                                                               #functionCall
    | EXISTS '(' query ')'                                                                   #existsCall
    | '(' query ')'                                                                          #subqueryExpression
    | CASE whenClause+ (ELSE elseExpression=expression)? END                                 #simpleCase
    | CAST '(' expression AS type ')'                                                        #cast
    | '(' expression ')'                                                                     #parenthesizedExpression
    ;

literal
    : NULL                                                                                   #nullLiteral
    | interval                                                                               #intervalLiteral
    | number                                                                                 #numericLiteral
    | booleanValue                                                                           #booleanLiteral
    | string                                                                                 #stringLiteral
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
    : INTERVAL sign=(PLUS | MINUS)? number intervalField
    ;

intervalField
    : YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND
    | YEARS | MONTHS | WEEKS | DAYS | HOURS | MINUTES | SECONDS
    ;

type
    : baseType ('(' typeParameter (',' typeParameter)* ')')?
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : identifier
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

tableFunctionDef
   : '(' functionArgumentDef? (',' functionArgumentDef)* ')'
   ;

functionArgumentDef
   : name=identifier ':' typeName=type ('=' literal)?
   ;

hint
   : '/*+' hintItem (',' hintItem)* '*/'
   ;

// NO_HASH_JOIN, RESOURCE(mem, parallelism)
hintItem
   : identifier ('(' keyValue (',' keyValue)* ')')?
   ;

keyValue
   : identifier
   ;

qualifiedName
    : identifier ('.' identifier)* (all='.*')?
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
    : ADD | ALL | ANY | ARRAY | ASC | AT | DATE | DAY | DESC | HOUR | IF | IGNORE | INTERVAL
    | LATERAL | LIMIT | LOGICAL | MAP | MINUTE | MONTH | NULLIF | NULLS | ORDER | SECOND | TIMESTAMP
    | TO | WEEK | YEAR
    ;

ADD: 'ADD';
ALL: 'ALL';
AND: 'AND';
ANY: 'ANY';
ARRAY: 'ARRAY';
AS: 'AS';
ASC: 'ASC';
AT: 'AT';
BETWEEN: 'BETWEEN';
BY: 'BY';
CASE: 'CASE';
CAST: 'CAST';
CROSS: 'CROSS';
DATE: 'DATE';
DAY: 'DAY';
DAYS: 'DAYS';
DELETE: 'DELETE';
DESC: 'DESC';
DISTINCT: 'DISTINCT';
ELSE: 'ELSE';
EMPTY: 'EMPTY';
END: 'END';
EXCEPT: 'EXCEPT';
EXISTS: 'EXISTS';
EXPORT: 'EXPORT';
FALSE: 'FALSE';
FROM: 'FROM';
FULL: 'FULL';
GROUP: 'GROUP';
HAVING: 'HAVING';
HOUR: 'HOUR';
HOURS: 'HOURS';
IF: 'IF';
IGNORE: 'IGNORE';
IN: 'IN';
INNER: 'INNER';
INTERSECT: 'INTERSECT';
INTERVAL: 'INTERVAL';
IS: 'IS';
JOIN: 'JOIN';
LATERAL: 'LATERAL';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LOGICAL: 'LOGICAL';
MAP: 'MAP';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
NOT: 'NOT';
NULL: 'NULL';
NULLIF: 'NULLIF';
NULLS: 'NULLS';
ON: 'ON';
OR: 'OR';
ORDER: 'ORDER';
OUTER: 'OUTER';
RIGHT: 'RIGHT';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
SELECT: 'SELECT';
STREAM: 'STREAM';
TEMPORAL: 'TEMPORAL';
THEN: 'THEN';
TIMESTAMP: 'TIMESTAMP';
TO: 'TO';
TRUE: 'TRUE';
UESCAPE: 'UESCAPE';
UNION: 'UNION';
UPDATE: 'UPDATE';
USING: 'USING';
WEEK: 'WEEK';
WEEKS: 'WEEKS';
WHEN: 'WHEN';
WHERE: 'WHERE';
YEAR: 'YEAR';
YEARS: 'YEARS';
IMPORT: 'IMPORT';
SEMICOLON: ';';
INVERSE: 'INVERSE';

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
    : (LETTER | '_' | '@') (LETTER | DIGIT | '_' | '-')*
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

// TODO: empty comment block (/**/) conflicts with hint syntax
BRACKETED_COMMENT
    : '/*' ANY_EXCEPT_PLUS .*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

ANY_EXCEPT_PLUS : ~'+' ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
