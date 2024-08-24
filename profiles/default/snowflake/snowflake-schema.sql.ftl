<#if iceberg?? && iceberg["engines"]["snowflake"]??>
<#list iceberg["engines"]["snowflake"]["ddl"] as statement>
${statement["sql"]};

</#list>
</#if>