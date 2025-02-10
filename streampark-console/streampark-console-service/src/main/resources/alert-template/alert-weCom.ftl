> Apache StreamPark , Make stream processing easier!

# ${subject}

### **Dear StreamPark user:**

`Oops! I'm sorry to inform you that something wrong with your app`

-   **Job Name：${jobName}**
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

> Best Wishes!
> Apache StreamPark

<#if link??>[Details](${link})    </#if>[Website](https://streampark.apache.org)    [GitHub](https://github.com/apache/streampark)

