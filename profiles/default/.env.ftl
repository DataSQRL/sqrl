<#assign composeFiles = "">
<#list config["enabled-engines"] as engine>
  <#if engine != "postgres_log">
    <#assign composeFiles = composeFiles + engine + ".compose.yml:">
  </#if>
</#list>
COMPOSE_FILE=${composeFiles?remove_ending(":")}
AWS_REGION=${r"${AWS_REGION:-us-east-1}"}
AWS_ACCESS_KEY_ID=${r"${AWS_ACCESS_KEY_ID:-myaccesskey}"}
AWS_SECRET_ACCESS_KEY=${r"${AWS_SECRET_ACCESS_KEY:-mysecretkey}"}
SNOWFLAKE_ID=${r"${SNOWFLAKE_ID:-mysnowflakeid}"}
SNOWFLAKE_PASSWORD=${r"${SNOWFLAKE_PASSWORD:-mysnowflakepassword}"}