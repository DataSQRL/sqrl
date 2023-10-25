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
    | assignmentPath ':=' SELECT query2              #sqlQuery
    | assignmentPath ':=' expression                 #expressionQuery
    ;

query2
    : ( . )+?
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
    : query2
    ;

fromDeclaration
    : query2
    ;

distinctQuerySpec
   : identifier
     ON onExpr
     (ORDER BY orderExpr=expression ordering=DESC?)?
   ;

onExpr
   : '('? selectItem (',' selectItem)* ')'?;

streamQuerySpec
    : ON subscriptionType AS SELECT query2;

subscriptionType
    : ADD
    | DELETE
    | UPDATE
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | ASTERISK                      #selectAll
    ;

expression
    : ( . )+?
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


tableFunctionDef
   : '(' functionArgumentDef? (',' functionArgumentDef)* ')'
   ;

functionArgumentDef
   : name=identifier ':' typeName=type ('=' expression)?
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
