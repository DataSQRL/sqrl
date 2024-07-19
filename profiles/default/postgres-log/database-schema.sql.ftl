<#if postgres-log??>
<#list postgres-log["ddl"] as statement>
${statement["sql"]}
</#list>
</#if>