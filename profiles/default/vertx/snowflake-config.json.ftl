{
  <#if config["engines"]["snowflake"]?? && config["engines"]["snowflake"]["url"]??>
  "url": "${config["engines"]["snowflake"]["url"]}"
  </#if>
}