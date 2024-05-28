<#if snowflake?? && snowflake["ddl"]??>
<#list snowflake["ddl"] as statement>
${statement["sql"]}
</#list>
</#if>