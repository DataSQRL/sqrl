<#list config["values"]["flinkConfig"] as key, value>
<#if key?contains(".")>
SET '${key}' = '${value}';
</#if>

</#list>
<#list flink["flinkSql"] as sql>
${sql}

</#list>