<#if postgres??>
<#list postgres["ddl"] as statement>
${statement["sql"]}
</#list>
</#if>

<#if postgres_log??>
<#list postgres_log["ddl"] as statement>
${statement["sql"]}
</#list>
</#if>

<#if postgres??>
<#if !config["enabled-engines"]?seq_contains("vertx")>
<#if postgres["views"]??>
<#list postgres["views"] as statement>
${statement["sql"]};
</#list>
</#if>
</#if>
</#if>
