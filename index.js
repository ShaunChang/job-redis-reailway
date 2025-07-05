const express = require('express');
const { Redis } = require('@upstash/redis');
const fetch = require('node-fetch'); // 注意 Railway 环境中默认支持 fetch，也可移除此行

const app = express();
app.use(express.json());

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
  console.log('✅ 已入队任务:', task.type);
  res.json({ status: 'Task enqueued', task });
});

// 从队列中取出任务并处理
app.post('/process', async (req, res) => {
  const taskData = await redis.rpop('task_queue');

  if (!taskData) {
    return res.json({ status: '⏳ No tasks in queue' });
  }

  const task = JSON.parse(taskData);
  console.log('🟡 处理任务:', task);

  try {
    if (task.type === 'wechat') {
      if (!task.webhookUrl || !task.text) {
        throw new Error('Missing webhookUrl or text in wechat task');
      }
      await sendWechatNotice(task.webhookUrl, task.text);
      return res.json({ status: '✅ 微信任务已完成', task });
    }

    // 可扩展更多类型
    return res.json({ status: '⚠️ 未知任务类型', task });

  } catch (err) {
    console.error('❌ 任务处理失败:', err.message);
    return res.status(500).json({ error: err.message });
  }
});

// 发送企业微信通知
async function sendWechatNotice(webhookUrl, text) {
  try {
    const res = await fetch(webhookUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        msgtype: "text",
        text: { content: text }
      })
    });
    const result = await res.json();
    console.log("✅ 企业微信发送结果:", result);
  } catch (e) {
    console.error("❌ 企业微信发送失败：", e.message);
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 队列服务已启动，监听端口 ${PORT}`);
});
