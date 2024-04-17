<#list config["engines"]["streams"] as key, value>
<#if key?contains(".")>
SET '${key}' = '${value}';
</#if>

</#list>
<#list plan.get("streams").getFlinkSql() as sql>
${sql}

</#list>