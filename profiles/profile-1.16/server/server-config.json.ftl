<#if plan.get("server")??>
${plan.get("server").getConfig().toJson().encodePrettily()}
</#if>