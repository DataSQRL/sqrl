" Syntax of DataSQRL scripts, as in the TextMate grammar of the IntelliJ plugin. The language server
" adds semantic tokens on top, e.g. for tables and columns.
if exists('b:current_syntax')
  finish
endif

syn case ignore

syn keyword sqrlStatement import export to as returns subscribe next_batch
syn keyword sqrlKeyword select from where group by having order limit offset fetch first next rows
syn keyword sqrlKeyword only union intersect except minus all distinct on using join inner left
syn keyword sqrlKeyword right full outer cross natural lateral and or not in is like ilike similar
syn keyword sqrlKeyword between exists case when then else end with recursive values insert into
syn keyword sqrlKeyword overwrite update set delete create temporary table view function catalog
syn keyword sqrlKeyword database if drop alter rename primary key enforced watermark for system_time
syn keyword sqrlKeyword of interval partition partitioned over window range unbounded preceding
syn keyword sqrlKeyword following current asc desc nulls last cast try_cast escape descriptor unnest
syn keyword sqrlKeyword ordinality match_recognize measures pattern define within comment metadata
syn keyword sqrlKeyword virtual including excluding generated language show describe explain use
syn keyword sqrlKeyword load module filter any some at local zone default unique constraint year
syn keyword sqrlKeyword month day hour minute second week quarter millisecond microsecond
syn keyword sqrlType tinyint smallint int integer bigint float double precision real decimal dec
syn keyword sqrlType numeric boolean char character varchar string binary varbinary bytes date time
syn keyword sqrlType timestamp timestamp_ltz array map multiset row raw variant
syn keyword sqrlConstant true false null unknown

syn match sqrlFunction /\<\h[[:alnum:]_$]*\ze\s*(/
syn match sqrlNumber /\%([[:alnum:]_$]\)\@<!\%(\d\+\%(\.\d*\)\=\|\.\d\+\)\%([eE][-+]\=\d\+\)\=\>/
syn match sqrlOperator #<>\|!=\|<=\|>=\|||\|[=<>+\-*/%]#
syn match sqrlParameter /\%([[:alnum:]_:]\)\@<!:\h\w*/
syn match sqrlThis /\<this\ze\s*\./
syn match sqrlAssign /:=/
syn match sqrlTemplate /{{[^}]*}}/

" The name of a definition: a table or column, e.g. `Orders :=`, or a table function or
" relationship, e.g. `Orders.items(limit INT) :=`
syn match sqrlDefinition /^\s*\zs\%(\h[[:alnum:]_$]*\|`[^`]*`\)\%(\.\%(\h[[:alnum:]_$]*\|`[^`]*`\)\)*\ze\s*:=/
syn match sqrlFunctionDefinition /^\s*\zs\%(\h[[:alnum:]_$]*\|`[^`]*`\)\%(\.\%(\h[[:alnum:]_$]*\|`[^`]*`\)\)*\ze\s*(.*)\s*\%(returns\>.*\)\=:=/

" The path of an IMPORT or EXPORT, e.g. `connectors.police_{{variant}}`
syn match sqrlPath /\%(^\s*\%(import\|export\s\+\S\+\s\+to\)\s\+\)\@<=\%([[:alnum:]_.`*$-]\|{{[^}]*}}\)\+/ contains=sqrlTemplate

syn region sqrlString start=/'/ skip=/''/ end=/'/ contains=sqrlTemplate
syn region sqrlQuotedName start=/`/ end=/`/ oneline

syn match sqrlLineComment /--.*$/ contains=@Spell
syn region sqrlComment start=#/\*# end=#\*/# contains=@Spell
syn region sqrlDocComment start=#/\*\*\%(/\)\@!# end=#\*/# contains=@Spell
" A hint, e.g. `/*+ primary_key(id) */`
syn region sqrlHint start=#/\*+# end=#\*/# contains=sqrlHintName,sqrlHintArguments
syn match sqrlHintName /\h\w*/ contained
syn region sqrlHintArguments matchgroup=sqrlHint start=/(/ end=/)/ contained contains=sqrlHintArgument
syn match sqrlHintArgument /[^,)[:space:]]\+/ contained

hi def link sqrlStatement Include
hi def link sqrlKeyword Keyword
hi def link sqrlType Type
hi def link sqrlConstant Constant
hi def link sqrlFunction Function
hi def link sqrlNumber Number
hi def link sqrlOperator Operator
hi def link sqrlParameter Identifier
hi def link sqrlThis Special
hi def link sqrlAssign Operator
hi def link sqrlTemplate Special
hi def link sqrlDefinition Structure
hi def link sqrlFunctionDefinition Function
hi def link sqrlPath Identifier
hi def link sqrlString String
hi def link sqrlQuotedName Identifier
hi def link sqrlLineComment Comment
hi def link sqrlComment Comment
hi def link sqrlDocComment SpecialComment
hi def link sqrlHint PreProc
hi def link sqrlHintName PreProc
hi def link sqrlHintArgument Identifier

let b:current_syntax = 'sqrl'
