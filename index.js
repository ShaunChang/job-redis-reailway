const express = require('express');
const { Redis } = require('@upstash/redis');

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
  if (!task) {
    return res.status(400).json({ error: 'Missing task data' });
  }
  await redis.lpush('task_queue', JSON.stringify(task));
  res.json({ status: 'Task enqueued' });
});

// 处理队列中的任务
app.post('/process', async (req, res) => {
  const taskData = await redis.rpop('task_queue');
  if (!taskData) {
    return res.json({ status: 'No tasks in queue' });
  }
  const task = JSON.parse(taskData);
  // 在此处处理任务
  console.log('Processing task:', task);
  res.json({ status: 'Task processed', task });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Queue service running on port ${PORT}`);
});
