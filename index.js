const express = require('express');
const { Redis } = require('@upstash/redis');
const fetch = require('node-fetch');

const app = express();
app.use(express.json({ limit: '5mb' }));

// 初始化 Redis 客户端
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// 添加任务到队列
app.post('/enqueue', async (req, res) => {
  const task = req.body;
  if (!task || !task.type) {
    return res.status(400).json({ error: 'Missing task type or data' });
  }
  await redis.lpush('task_queue', JSON.stringify(task));
  console.log('✅ 入队任务:', task.type);
  res.json({ status: 'Task enqueued', task });
});

// 处理队列中的任务（带锁 + while）
app.post('/process', async (req, res) => {
  const lockKey = 'processing_lock';

  const locked = await redis.set(lockKey, '1', { nx: true, ex: 60 });
  if (!locked) {
    return res.json({ status: '⏳ 正在处理任务中，跳过本次触发' });
  }

  let processed = 0;

  try {
    while (true) {
      const taskData = await redis.rpop('task_queue');
      if (!taskData) break;

      let task;
      try {
        task = typeof taskData === 'string' ? JSON.parse(taskData) : taskData;
      } catch (err) {
        console.error('❌ JSON 解析失败:', taskData);
        continue;
      }

      console.log('🟡 正在处理任务:', task);

      try {
        if (task.type === 'wechat') {
          if (!task.webhookUrl || !task.text) {
            throw new Error('Missing webhookUrl or text in wechat task');
          }
          await sendWechatNotice(task.webhookUrl, task.text);

        } else if (task.type === 'notion_insert') {
          if (!task.name || !task.message || !Array.isArray(task.array) ||
              !task.notionApiKey || !task.databaseId || !task.wechatWebhookUrl) {
            throw new Error('Missing required fields for notion_insert task');
          }
          await insertToNotion(task);

        } else if (task.type === 'supabase_insert_job') {
          if (!task.apiKey || !task.supabaseUrl || !task.payload || !task.wechatWebhookUrl) {
            throw new Error('Missing required fields for supabase_insert_job task');
          }
          await insertToSupabase(task);

        } else if (task.type === 'supabase_insert_job_batch') {
          await insertSupabaseBatch(task);
        } else {
          console.warn('⚠️ 未知任务类型:', task.type);
        }

        processed++;
      } catch (err) {
        console.error('❌ 任务处理失败:', err.message);
      }
    }
  } finally {
    await redis.del(lockKey);
  }

  res.json({ status: `✅ 已处理 ${processed} 个任务` });
});

// 微信通知
async function sendWechatNotice(webhookUrl, text) {
  try {
    const response = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        msgtype: 'text',
        text: { content: text },
      }),
    });
    const result = await response.json();
    console.log('📨 微信通知返回:', result);
  } catch (err) {
    console.error('❌ 微信通知失败:', err.message);
  }
}

// 插入 Notion
async function insertToNotion({ notionApiKey, databaseId, array, wechatWebhookUrl, message, name }) {
  const results = [];
  for (const item of array) {
    let attempt = 0;
    let inserted = false;
    while (attempt < 3 && !inserted) {
      try {
        const res = await fetch("https://api.notion.com/v1/pages", {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${notionApiKey}`,
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
          },
          body: JSON.stringify({
            parent: { database_id: databaseId },
            properties: item.properties
          })
        });

        const json = await res.json();

        if (res.status === 429) {
          const retryAfter = parseInt(res.headers.get('Retry-After') || '2', 10);
          console.warn(`⚠️ 第 ${attempt + 1} 次请求限流，等待 ${retryAfter} 秒重试...`);
          await delay(retryAfter * 1000);
          attempt++;
          continue;
        }

        if (res.ok) {
          results.push({ status: "success", pageId: json.id, url: json.url });
          inserted = true;
        } else {
          const errorMsg = json.message || "Unknown error";
          await sendWechatNotice(wechatWebhookUrl, `❗插入失败：${getTitle(item)}\n原因：${errorMsg}`);
          results.push({ status: "error", message: errorMsg });
          break;
        }
      } catch (err) {
        await sendWechatNotice(wechatWebhookUrl, `❗插入失败：${getTitle(item)}\n系统异常：${err.message}`);
        results.push({ status: "error", message: err.message });
        break;
      }
    }
  }

  const successCount = results.filter(r => r.status === "success").length;
  const errorCount = results.length - successCount;
  const finalNotice = errorCount > 0
    ? `⚠️ 插入完成：${successCount} 成功 / ${errorCount} 失败。\n📨 城市岗位：${name} ${message}`
    : `✅ 全部插入成功，共 ${successCount} 条。\n📨 城市岗位：${name} ${message}`;

  await sendWechatNotice(wechatWebhookUrl, finalNotice);
  return { ret: results };
}

//批量插入
async function insertSupabaseBatch({ apiKey, supabaseUrl, payload, wechatWebhookUrl }) {
  try {
    const res = await fetch(`${supabaseUrl}/rest/v1/job`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": apiKey,
        "Authorization": `Bearer ${apiKey}`,
        "Prefer": "return=representation"
      },
      body: JSON.stringify(payload)
    });

    const json = await res.json();

    if (!res.ok) {
      await sendWechatNotice(wechatWebhookUrl, `❌ 批量插入失败：${json.message || '未知错误'}`);
      return;
    }

    await sendWechatNotice(wechatWebhookUrl, `✅ 批量插入成功，共 ${json.length} 条`);
  } catch (err) {
    await sendWechatNotice(wechatWebhookUrl, `❌ Supabase 异常：${err.message}`);
  }
}


// 插入 Supabase job 表
async function insertToSupabase({ apiKey, supabaseUrl, payload, wechatWebhookUrl }) {
  try {
    const res = await fetch(`${supabaseUrl}/rest/v1/job`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": apiKey,
        "Authorization": `Bearer ${apiKey}`,
        "Prefer": "return=representation"
      },
      body: JSON.stringify(payload)
    });

    const data = await res.json();

    if (!res.ok) {
      const msg = data.message || "未知错误";
      await sendWechatNotice(wechatWebhookUrl, `❌ Supabase 插入失败：${msg}`);
      return;
    }

    await sendWechatNotice(wechatWebhookUrl, `✅ Supabase 插入成功，ID: ${data[0]?.id}`);
  } catch (err) {
    await sendWechatNotice(wechatWebhookUrl, `❌ Supabase 插入异常：${err.message}`);
  }
}

// 工具函数
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getTitle(item) {
  try {
    return item.properties?.Company?.title?.[0]?.text?.content || '未命名';
  } catch {
    return '未知项';
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 队列服务运行中，监听端口 ${PORT}`);
});
