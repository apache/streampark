{
  "config": {
    "wide_screen_mode": true
  },
  "elements": [
    {
      "tag": "note",
      "elements": [
        {
          "tag": "plain_text",
          "content": "Apache StreamPark , Make stream processing easier!"
        }
      ]
    },
    {
      "tag": "markdown",
      "content": "**Dear StreamPark user:**"
    },
    {
      "tag": "note",
      "elements": [
        {
          "tag": "plain_text",
          "content": "Oops! I'm sorry to inform you that something wrong with your app"
        }
      ]
    },
    {
      "fields": [
        {
          "is_short": false,
          "text": {
            "content": "**Job Name：${jobName}**",
            "tag": "lark_md"
          }
        },
<#if  type == 1 >
        {
          "is_short": false,
          "text": {
            "content": "**Job Status：${status}**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": true,
          "text": {
            "content": "**Start Time：${startTime}**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": false,
          "text": {
            "content": "**End Time：${endTime}**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": true,
          "text": {
            "content": "**Duration：${duration}**",
            "tag": "lark_md"
          }
        }
<#if  restart >
        ,{
          "is_short": false,
          "text": {
            "content": "**Restart：${restartIndex}/${totalRestart}**",
            "tag": "lark_md"
          }
        }
</#if>
</#if>
<#if  type == 2 >
        {
          "is_short": false,
          "text": {
            "content": "**CheckPoint Status：FAILED**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": false,
          "text": {
            "content": "**Checkpoint Failure Rate Interval：${cpFailureRateInterval}**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": false,
          "text": {
            "content": "**Max Failures Per Interval：${cpMaxFailureInterval}**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": false,
          "text": {
            "content": "**Start Time：${startTime}**",
            "tag": "lark_md"
          }
        },
        {
          "is_short": false,
          "text": {
            "content": "**Duration：${duration}**",
            "tag": "lark_md"
          }
        }
</#if>
      ],
      "tag": "div"
    },
<#if  atAll >
    {
      "tag": "markdown",
      "content": "<at id=all></at>"
    },
</#if>
    {
      "tag": "note",
      "elements": [
        {
          "tag": "plain_text",
          "content": "Best Wishes!\nApache StreamPark"
        }
      ]
    },
    {
      "tag": "action",
      "actions": [
<#if link??>{
          "tag": "button",
          "text": {
            "tag": "plain_text",
            "content": "Details"
          },
          "type": "primary",
          "url": "${link}"
        },</#if>
        {
          "tag": "button",
          "text": {
            "tag": "plain_text",
            "content": "Website"
          },
          "type": "primary",
          "url": "https://streampark.apache.org"
        },
        {
          "tag": "button",
          "text": {
            "tag": "plain_text",
            "content": "GitHub"
          },
          "type": "primary",
          "url": "https://github.com/apache/streampark"
        }
      ]
    }
  ],
  "header": {
    "template": "red",
    "title": {
      "content": "${subject}",
      "tag": "plain_text"
    }
  }
}
