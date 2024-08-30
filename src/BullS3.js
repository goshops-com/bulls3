const EventEmitter = require('events');

class BullS3 extends EventEmitter {
  constructor(queueName, storage, config = {}) {
    super();
    this.queueName = queueName;
    this.storage = storage;
    this.isProcessing = false;
    this.pollingInterval = config.pollingInterval || 5000; // Default to 5 seconds if not specified
    this.processorFn = null;
    this.pollingTimeout = null;
  }

  async initialize() {
    await this.storage.initialize();
    await this.storage.ensureMetadataExists(this.queueName);
  }

  async add(jobData) {
    const jobId = await this.storage.addJob(this.queueName, jobData);
    this.emit('added', { id: jobId, data: jobData });
    return jobId;
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
        }
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

  stop() {
    if (this.pollingTimeout) {
      clearTimeout(this.pollingTimeout);
      this.pollingTimeout = null;
    }
  }
}

module.exports = BullS3;