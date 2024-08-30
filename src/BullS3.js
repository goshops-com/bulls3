const EventEmitter = require('events');

class BullS3 extends EventEmitter {
  constructor(queueName, storage) {
    super();
    this.queueName = queueName;
    this.storage = storage;
    this.isProcessing = false;
  }

  async initialize() {
    await this.storage.initialize();
    await this.storage.ensureMetadataExists(this.queueName);
    this.startProcessing();
  }

  async add(jobData) {
    const jobId = await this.storage.addJob(this.queueName, jobData);
    this.emit('added', { id: jobId, data: jobData });
    return jobId;
  }

  process(processorFn) {
    this.processorFn = processorFn;
  }

  startProcessing() {
    setInterval(() => this.processNextJob(), 1000);
  }

  async processNextJob() {
    if (this.isProcessing || !this.processorFn) return;
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
    }
  }
}

module.exports = BullS3;