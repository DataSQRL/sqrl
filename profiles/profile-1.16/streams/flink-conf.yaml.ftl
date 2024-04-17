<#list config["engines"]["streams"] as key, value>
<#if key?contains(".")>
${key}=${value}
</#if>
</#list>