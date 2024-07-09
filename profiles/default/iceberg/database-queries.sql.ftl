<#if iceberg?? && iceberg["plan"]["queries"]??>
<#list iceberg["plan"]["queries"] as statement>
${statement["sql"]};

</#list>
</#if>