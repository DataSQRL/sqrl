<#if config["values"]?? && config["values"]["flink-config"]??>
<#list config["values"]["flink-config"] as key, value>
<#if key?contains(".")>
"${key}": "${value}"
</#if>
</#list>
</#if>