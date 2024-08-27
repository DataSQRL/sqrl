<#if iceberg?? && iceberg["engines"]["snowflake"]??>
<#list iceberg["engines"]["snowflake"]["views"] as statement>
${statement["sql"]};

</#list>
</#if>