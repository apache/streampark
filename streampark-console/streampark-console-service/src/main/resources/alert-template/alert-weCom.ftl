> Apache StreamPark , Make stream processing easier!

# ${subject}
<#if  type == 1 || type == 2 || type == 3>
### **Dear StreamPark user:**

`Oops! I'm sorry to inform you that something wrong with your app`
</#if>
<#if  type == 4>
### **Dear StreamPark user: ${user}**

`This is the latest auto probe result`
</#if>
<#if  type == 1 || type == 2 >
-   **Job Name：${jobName}**
</#if>
<#if  type == 3 >
-   **Cluster Name：${jobName}**
</#if>
<#if  type == 1 >
-   **Job Status：${status}**
-   **Start Time：${startTime}**
-   **End Time：${endTime}**
-   **Duration：${duration}**
<#if  restart >
-   **Restart：${restartIndex}/${totalRestart}**
</#if>
</#if>
<#if  type == 2 >
-   **CheckPoint Status：FAILED**
-   **Checkpoint Failure Rate Interval：${cpFailureRateInterval}**
-   **Max Failures Per Interval：${cpMaxFailureInterval}**
-   **Start Time：${startTime}**
-   **Duration：${duration}**
</#if>
<#if  type == 3 >
-   **Cluster Status：${status}**
-   **Start Time：${startTime}**
-   **End Time：${endTime}**
-   **Duration：${duration}**
-   **All Jobs：${allJobs}**
-   **About Affected Jobs：${affectedJobs}**
</#if>
<#if  type == 4 >
-   **Probe Jobs：${probeJobs}**
-   **Failed Jobs：${failedJobs}**
-   **Lost Jobs：${lostJobs}**
-   **Cancelled Jobs：${cancelledJobs}**
</#if>

> Best Wishes!
> Apache StreamPark

<#if link??>[Details](${link})    </#if>[Website](https://streampark.apache.org)    [GitHub](https://github.com/apache/streampark)

