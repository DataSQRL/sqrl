<#list plan.get("database").getDdlStatements() as statement>
${statement.toSql()}
</#list>