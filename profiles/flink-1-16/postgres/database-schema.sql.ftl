<#list postgres["ddl"] as statement>
${statement["sql"]}
</#list>