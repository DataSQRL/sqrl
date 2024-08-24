<#if iceberg?? && iceberg["plan"]["queries"]??>
<#list iceberg["plan"]["views"] as statement>
${statement["sql"]};

</#list>
</#if>