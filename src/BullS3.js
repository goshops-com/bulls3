const EventEmitter = require('events');
const schedule = require('node-schedule');

class BullS3 extends EventEmitter {
  constructor(queueName, storage, config = {}) {
    super();
    this.queueName = queueName;
    this.storage = storage;
    this.isProcessing = false;
    this.pollingInterval = config.pollingInterval || 5000;
    this.heartbeatInterval = config.heartbeatInterval || 30000;
    this.jobTimeout = config.jobTimeout || 300000;
    this.cronLockDuration = config.cronLockDuration || 60000; // 1 minute lock by default
    this.processorFn = null;
    this.pollingTimeout = null;
    this.heartbeatTimeout = null;
    this.cronJobs = new Map();
    this.podId = `pod_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
  }

  async initialize() {
    await this.storage.initialize();
    await this.storage.ensureMetadataExists(this.queueName);
    await this.loadCronJobs();
  }

  async add(jobData, opts = {}) {
    console.log('add', jobData, opts);
    if (opts.repeat && opts.repeat.cron) {
      return this.addCronJob(jobData, opts.repeat.cron, opts.repeat.timezone);
    }
    const jobId = opts.jobId || `job_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;

    // Check if job already exists
    const existingJob = await this.storage.getJobData(this.queueName, jobId);
    if (existingJob) {
      console.log(`Job already exists with ID: ${jobId}`);
      return jobId;
    }

    await this.storage.addJob(this.queueName, jobId, jobData);
    this.emit('added', { id: jobId, data: jobData });
    return jobId;
  }

  async addCronJob(jobData, cronExpression, timezone, jobId) {
    const cronJobId = jobId || `cron_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;

    // Check if cron job already exists
    const existingCronJob = await this.storage.getCronJobById(this.queueName, cronJobId);
    if (existingCronJob) {
      console.log(`Cron job already exists with ID: ${cronJobId}`);
      return cronJobId;
    }

    const cronJobInfo = {
      id: cronJobId,
      cron: cronExpression,
      timezone: timezone,
      jobData: jobData
    };

    await this.storage.addCronJob(this.queueName, cronJobId, cronJobInfo);
    this.scheduleCronJob(cronJobInfo);
    this.emit('cronJobAdded', cronJobInfo);
    return cronJobId;
  }

  async loadCronJobs() {
    const cronJobs = await this.storage.getCronJobs(this.queueName);
    for (const cronJob of cronJobs) {
      this.scheduleCronJob(cronJob);
    }
  }

  scheduleCronJob(cronJobInfo) {
    const job = schedule.scheduleJob(cronJobInfo.cron, async () => {
      const lockAcquired = await this.acquireCronLock(cronJobInfo.id);
      if (lockAcquired) {
        try {
          await this.add(cronJobInfo.jobData);
        } finally {
          await this.releaseCronLock(cronJobInfo.id);
        }
      }
    });
    this.cronJobs.set(cronJobInfo.id, job);
  }

  async acquireCronLock(cronJobId) {
    try {
      await this.storage.acquireLock(this.queueName, cronJobId, this.podId, this.cronLockDuration);
      return true;
    } catch (error) {
      if (error.code === 'LockAlreadyExists') {
        return false;
      }
      throw error;
    }
  }

  async releaseCronLock(cronJobId) {
    await this.storage.releaseLock(this.queueName, cronJobId, this.podId);
  }

  async removeCronJob(cronJobId) {
    const job = this.cronJobs.get(cronJobId);
    if (job) {
      job.cancel();
      this.cronJobs.delete(cronJobId);
    }
    await this.storage.removeCronJob(this.queueName, cronJobId);
    this.emit('cronJobRemoved', cronJobId);
  }

  process(processorFn) {
    this.processorFn = processorFn;
    this.startProcessing();
  }

  startProcessing() {
    if (this.pollingTimeout) {
      clearTimeout(this.pollingTimeout);
    }
    this.processNextJob();
  }

  async processNextJob() {
    if (this.isProcessing || !this.processorFn) {
      this.scheduleNextProcess();
      return;
    }

    this.isProcessing = true;

    try {
      const job = await this.storage.getNextJob(this.queueName);
      if (job) {
        this.emit('active', job);
        job.data.startTime = Date.now();
        job.data.lastHeartbeat = Date.now();
        await this.storage.updateJob(this.queueName, job.id, job.data);

        this.startHeartbeat(job);

        try {
          const result = await this.processorFn(job);
          job.data.result = result;
          job.data.status = 'completed';
          await this.storage.updateJob(this.queueName, job.id, job.data);
          this.emit('completed', job);
        } catch (error) {
          job.data.status = 'failed';
          job.data.error = error.message;
          await this.storage.updateJob(this.queueName, job.id, job.data);
          this.emit('failed', job, error);
        } finally {
          this.stopHeartbeat();
        }
      } else {
        await this.checkAbandonedJobs();
      }
    } catch (error) {
      console.error('Error processing job:', error);
    } finally {
      this.isProcessing = false;
      this.scheduleNextProcess();
    }
  }

  scheduleNextProcess() {
    this.pollingTimeout = setTimeout(() => this.processNextJob(), this.pollingInterval);
  }

  startHeartbeat(job) {
    this.heartbeatTimeout = setInterval(async () => {
      job.data.lastHeartbeat = Date.now();
      await this.storage.updateJob(this.queueName, job.id, job.data);
    }, this.heartbeatInterval);
  }

  stopHeartbeat() {
    if (this.heartbeatTimeout) {
      clearInterval(this.heartbeatTimeout);
      this.heartbeatTimeout = null;
    }
  }

  async checkAbandonedJobs() {
    const abandonedJobs = await this.storage.getAbandonedJobs(this.queueName, this.jobTimeout);
    for (const job of abandonedJobs) {
      job.data.status = 'waiting';
      job.data.lastHeartbeat = null;
      await this.storage.updateJob(this.queueName, job.id, job.data);
      this.emit('abandoned', job);
    }
  }

  async stop() {
    if (this.pollingTimeout) {
      clearTimeout(this.pollingTimeout);
      this.pollingTimeout = null;
    }
    this.stopHeartbeat();

    for (const [cronJobId, job] of this.cronJobs) {
      job.cancel();
    }
    this.cronJobs.clear();
  }
}

module.exports = BullS3;