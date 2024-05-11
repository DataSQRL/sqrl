<?xml version="1.0" encoding="UTF-8"?>
<jmeterTestPlan version="1.2" properties="5.0" jmeter="5.6.3">
  <hashTree>
    <TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="Test Plan">
      <elementProp name="TestPlan.user_defined_variables" elementType="Arguments">
        <collectionProp name="Arguments.arguments"/>
      </elementProp>
      <boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
      <boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
    </TestPlan>
    <hashTree>
    <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group">
            <intProp name="ThreadGroup.num_threads">1</intProp>
            <intProp name="ThreadGroup.ramp_time">1</intProp>
            <boolProp name="ThreadGroup.same_user_on_next_iteration">true</boolProp>
            <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
            <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller">
              <stringProp name="LoopController.loops">1</stringProp>
              <boolProp name="LoopController.continue_forever">false</boolProp>
            </elementProp>
          </ThreadGroup>
          <hashTree>

       <#if test["mutations"]??>
       <#list test["mutations"] as planItem>
       <HTTPSamplerProxy guiclass="HttpTestSampleGui" testclass="HTTPSamplerProxy" testname="${planItem.name}">
         <elementProp name="HTTPsampler.Arguments" elementType="Arguments">
           <collectionProp name="Arguments.arguments">
             <elementProp name="" elementType="HTTPArgument">
               <boolProp name="HTTPArgument.always_encode">false</boolProp>
               <stringProp name="Argument.value">${planItem.query}</stringProp>
               <stringProp name="Argument.metadata">=</stringProp>
             </elementProp>
           </collectionProp>
         </elementProp>
         <stringProp name="HTTPSampler.protocol">http</stringProp>
         <stringProp name="HTTPSampler.domain">server</stringProp>
         <stringProp name="HTTPSampler.port">8888</stringProp>
         <stringProp name="HTTPSampler.path">/graphql</stringProp>
         <stringProp name="HTTPSampler.method">POST</stringProp>
         <boolProp name="HTTPSampler.use_keepalive">true</boolProp>
         <boolProp name="HTTPSampler.follow_redirects">true</boolProp>
       </HTTPSamplerProxy>
       <hashTree>
         <JSR223PostProcessor guiclass="TestBeanGUI" testclass="JSR223PostProcessor" testname="JSR223 PostProcessor">
           <stringProp name="cacheKey"></stringProp>
           <stringProp name="filename"></stringProp>
           <stringProp name="parameters"></stringProp>
           <stringProp name="script">import java.nio.file.*;

       // Define the snapshot directory path
       Path snapshotDir = Paths.get('snapshots');

       // Check if the directory exists, create it if it doesn’t
       if (!Files.exists(snapshotDir)) {
           Files.createDirectories(snapshotDir);
       }

       Path snapshotPath = snapshotDir.resolve('${planItem.name}.snapshot');
       String currentResponse = prev.getResponseDataAsString();
       if (Files.exists(snapshotPath)) {
           String existingSnapshot = new String(Files.readAllBytes(snapshotPath), "UTF-8");
           if (!existingSnapshot.equals(currentResponse)) {
               log.error('Snapshot mismatch detected for ' + sampler.getName());
               log.error('Expected:' + existingSnapshot);
               log.error('Found   :' + currentResponse);
           } else {
               log.info('Snapshot OK for ' + sampler.getName());
           }
       } else {
           Files.write(snapshotPath, currentResponse.getBytes("UTF-8"));
           log.error('Snapshot saved for ' + sampler.getName());
       }
       </stringProp>
           <stringProp name="scriptLanguage">groovy</stringProp>
         </JSR223PostProcessor>
         <hashTree/>
         <HeaderManager guiclass="HeaderPanel" testclass="HeaderManager" testname="HTTP Header Manager">
           <collectionProp name="HeaderManager.headers">
             <elementProp name="Content-Type" elementType="Header">
               <stringProp name="Header.name">Content-Type</stringProp>
               <stringProp name="Header.value">application/graphql</stringProp>
             </elementProp>
           </collectionProp>
         </HeaderManager>
         <hashTree/>
       </hashTree>
       </#list>
       </#if>
      </hashTree>
      <ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group">
        <intProp name="ThreadGroup.num_threads">1</intProp>
        <intProp name="ThreadGroup.ramp_time">0</intProp>
        <longProp name="ThreadGroup.duration">60</longProp>
        <longProp name="ThreadGroup.delay">${environment["SQRL_TEST_DELAY"]!config["test-runner"]["delay-sec"]!60}</longProp>
        <boolProp name="ThreadGroup.same_user_on_next_iteration">false</boolProp>
        <boolProp name="ThreadGroup.scheduler">true</boolProp>
        <stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
        <elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController">
          <stringProp name="LoopController.loops">1</stringProp>
          <boolProp name="LoopController.continue_forever">false</boolProp>
        </elementProp>
      </ThreadGroup>
      <hashTree>

       <#list test["queries"] as planItem>
       <HTTPSamplerProxy guiclass="HttpTestSampleGui" testclass="HTTPSamplerProxy" testname="${planItem.name}">
         <elementProp name="HTTPsampler.Arguments" elementType="Arguments">
           <collectionProp name="Arguments.arguments">
             <elementProp name="" elementType="HTTPArgument">
               <boolProp name="HTTPArgument.always_encode">false</boolProp>
               <stringProp name="Argument.value">${planItem.query}</stringProp>
               <stringProp name="Argument.metadata">=</stringProp>
             </elementProp>
           </collectionProp>
         </elementProp>
         <stringProp name="HTTPSampler.protocol">http</stringProp>
         <stringProp name="HTTPSampler.domain">server</stringProp>
         <stringProp name="HTTPSampler.port">8888</stringProp>
         <stringProp name="HTTPSampler.path">/graphql</stringProp>
         <stringProp name="HTTPSampler.method">POST</stringProp>
         <boolProp name="HTTPSampler.use_keepalive">true</boolProp>
         <boolProp name="HTTPSampler.follow_redirects">true</boolProp>
       </HTTPSamplerProxy>
       <hashTree>
         <JSR223PostProcessor guiclass="TestBeanGUI" testclass="JSR223PostProcessor" testname="JSR223 PostProcessor">
           <stringProp name="cacheKey"></stringProp>
           <stringProp name="filename"></stringProp>
           <stringProp name="parameters"></stringProp>
           <stringProp name="script">import java.nio.file.*;

       // Define the snapshot directory path
       Path snapshotDir = Paths.get('snapshots');

       // Check if the directory exists, create it if it doesn’t
       if (!Files.exists(snapshotDir)) {
           Files.createDirectories(snapshotDir);
       }

       Path snapshotPath = snapshotDir.resolve('${planItem.name}.snapshot');
       String currentResponse = prev.getResponseDataAsString();
       if (Files.exists(snapshotPath)) {
           String existingSnapshot = new String(Files.readAllBytes(snapshotPath), "UTF-8");
           if (!existingSnapshot.equals(currentResponse)) {
               log.error('Snapshot mismatch detected for ' + sampler.getName());
               log.error('Expected:' + existingSnapshot);
               log.error('Found   :' + currentResponse);
           } else {
               log.info('Snapshot OK for ' + sampler.getName());
           }
       } else {
           Files.write(snapshotPath, currentResponse.getBytes("UTF-8"));
           log.error('Snapshot saved for ' + sampler.getName());
       }
       </stringProp>
           <stringProp name="scriptLanguage">groovy</stringProp>
         </JSR223PostProcessor>
         <hashTree/>
         <HeaderManager guiclass="HeaderPanel" testclass="HeaderManager" testname="HTTP Header Manager">
           <collectionProp name="HeaderManager.headers">
             <elementProp name="Content-Type" elementType="Header">
               <stringProp name="Header.name">Content-Type</stringProp>
               <stringProp name="Header.value">application/graphql</stringProp>
             </elementProp>
           </collectionProp>
         </HeaderManager>
         <hashTree/>
       </hashTree>
       </#list>

      </hashTree>
      <ResultCollector guiclass="SimpleDataWriter" testclass="ResultCollector" testname="Simple Data Writer">
        <boolProp name="ResultCollector.error_logging">false</boolProp>
        <objProp>
          <name>saveConfig</name>
          <value class="SampleSaveConfiguration">
            <time>true</time>
            <latency>true</latency>
            <timestamp>true</timestamp>
            <success>true</success>
            <label>true</label>
            <code>true</code>
            <message>true</message>
            <threadName>true</threadName>
            <dataType>true</dataType>
            <encoding>false</encoding>
            <assertions>true</assertions>
            <subresults>true</subresults>
            <responseData>false</responseData>
            <samplerData>false</samplerData>
            <xml>false</xml>
            <fieldNames>true</fieldNames>
            <responseHeaders>false</responseHeaders>
            <requestHeaders>false</requestHeaders>
            <responseDataOnError>false</responseDataOnError>
            <saveAssertionResultsFailureMessage>true</saveAssertionResultsFailureMessage>
            <assertionsResultsToSave>0</assertionsResultsToSave>
            <bytes>true</bytes>
            <sentBytes>true</sentBytes>
            <url>true</url>
            <threadCounts>true</threadCounts>
            <idleTime>true</idleTime>
            <connectTime>true</connectTime>
          </value>
        </objProp>
        <stringProp name="filename">target/jtls/result.jtl</stringProp>
      </ResultCollector>
      <hashTree/>
      <CookieManager guiclass="CookiePanel" testclass="CookieManager" testname="HTTP Cookie Manager">
        <collectionProp name="CookieManager.cookies"/>
        <boolProp name="CookieManager.clearEachIteration">true</boolProp>
      </CookieManager>
      <hashTree/>
      <CacheManager guiclass="CacheManagerGui" testclass="CacheManager" testname="HTTP Cache Manager">
        <boolProp name="clearEachIteration">true</boolProp>
        <boolProp name="useExpires">true</boolProp>
      </CacheManager>
      <hashTree/>
    </hashTree>
  </hashTree>
</jmeterTestPlan>
