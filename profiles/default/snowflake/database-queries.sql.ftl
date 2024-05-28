<#if snowflake?? && snowflake["queries"]??>
<#list snowflake["queries"] as statement>
${statement["sql"]}

</#list>
</#if>