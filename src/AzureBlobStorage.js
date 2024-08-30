const { BlobServiceClient } = require('@azure/storage-blob');

class AzureBlobStorage {
  constructor(config) {
    this.blobServiceClient = BlobServiceClient.fromConnectionString(config.connectionString);
    this.containerName = config.containerName;
    this.containerClient = this.blobServiceClient.getContainerClient(this.containerName);
  }

  async initialize() {
    await this.ensureContainerExists();
  }

  async ensureContainerExists() {
    await this.containerClient.createIfNotExists();
  }

  async ensureMetadataExists(queueName) {
    const metadataKey = this._getMetadataKey(queueName);
    const blobClient = this.containerClient.getBlockBlobClient(metadataKey);

    try {
      await blobClient.getProperties();
    } catch (error) {
      if (error.statusCode === 404) {
        const initialMetadata = {
          waitingJobs: [],
          inProgressJobs: [],
          completedJobs: [],
          failedJobs: []
        };
        await blobClient.upload(JSON.stringify(initialMetadata), JSON.stringify(initialMetadata).length);
      } else {
        throw error;
      }
    }
  }

  async addJob(queueName, jobData) {
    await this.ensureMetadataExists(queueName);
    
    const jobId = `job_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/${jobId}.json`);
    await blobClient.upload(JSON.stringify({ ...jobData, status: 'waiting' }), JSON.stringify(jobData).length);

    await this._updateQueueMetadata(queueName, metadata => {
      metadata.waitingJobs.push(jobId);
      return metadata;
    });

    return jobId;
  }

  async getNextJob(queueName) {
    await this.ensureMetadataExists(queueName);
    const metadata = await this._getQueueMetadata(queueName);

    for (const jobId of metadata.waitingJobs) {
      const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/${jobId}.json`);

      try {
        const jobData = await this.getJobData(queueName, jobId);
        if (jobData.status === 'waiting') {
          jobData.status = 'active';
          await blobClient.upload(JSON.stringify(jobData), JSON.stringify(jobData).length, { overwrite: true });

          await this._updateQueueMetadata(queueName, metadata => {
            metadata.waitingJobs = metadata.waitingJobs.filter(id => id !== jobId);
            metadata.inProgressJobs.push(jobId);
            return metadata;
          });

          return { id: jobId, data: jobData };
        }
      } catch (error) {
        // If there's an error processing this job, move to the next one
        continue;
      }
    }
    return null;
  }

  async updateJob(queueName, jobId, jobData) {
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/${jobId}.json`);
    await blobClient.upload(JSON.stringify(jobData), JSON.stringify(jobData).length, { overwrite: true });

    await this._updateQueueMetadata(queueName, metadata => {
      if (jobData.status === 'completed') {
        metadata.inProgressJobs = metadata.inProgressJobs.filter(id => id !== jobId);
        metadata.completedJobs.push({ id: jobId, completedAt: Date.now() });
        this._pruneCompletedJobs(metadata);
      } else if (jobData.status === 'failed') {
        metadata.inProgressJobs = metadata.inProgressJobs.filter(id => id !== jobId);
        metadata.failedJobs.push(jobId);
      }
      return metadata;
    });
  }

  _pruneCompletedJobs(metadata) {
    const now = Date.now();
    metadata.completedJobs = metadata.completedJobs
      .filter(job => (now - job.completedAt) < this.completedJobRetentionPeriod)
      .slice(-this.maxCompletedJobs);
  }

  async getJobData(queueName, jobId) {
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/${jobId}.json`);
    const downloadResponse = await blobClient.download();
    const downloaded = await this._streamToBuffer(downloadResponse.readableStreamBody);
    return JSON.parse(downloaded.toString());
  }

  async _getQueueMetadata(queueName) {
    const metadataKey = this._getMetadataKey(queueName);
    const blobClient = this.containerClient.getBlockBlobClient(metadataKey);
    const downloadResponse = await blobClient.download();
    const downloadedContent = await this._streamToBuffer(downloadResponse.readableStreamBody);
    return JSON.parse(downloadedContent.toString());
  }

  async _updateQueueMetadata(queueName, updateFn) {
    const metadataKey = this._getMetadataKey(queueName);
    const blobClient = this.containerClient.getBlockBlobClient(metadataKey);

    let attempts = 0;
    const maxAttempts = 5;

    while (attempts < maxAttempts) {
      try {
        const metadata = await this._getQueueMetadata(queueName);
        const updatedMetadata = updateFn(metadata);
        await blobClient.upload(JSON.stringify(updatedMetadata), JSON.stringify(updatedMetadata).length, { overwrite: true });
        return;
      } catch (error) {
        attempts += 1;
        if (attempts === maxAttempts) {
          throw new Error('Failed to update queue metadata after max attempts');
        }
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempts))); // Exponential backoff
      }
    }
  }

  _getMetadataKey(queueName) {
    return `${queueName}-metadata.json`;
  }

  async _streamToBuffer(readableStream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      readableStream.on('data', (data) => {
        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
      });
      readableStream.on('end', () => {
        resolve(Buffer.concat(chunks));
      });
      readableStream.on('error', reject);
    });
  }
}

module.exports = AzureBlobStorage;