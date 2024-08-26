<#if iceberg?? && iceberg["plan"]["ddl"]??>
<#list iceberg["plan"]["ddl"] as statement>
${statement["sql"]};

</#list>
</#if>