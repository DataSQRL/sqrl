<#if postgres_log??>
<#list postgres_log["ddl"] as statement>
${statement["sql"]}
</#list>
</#if>