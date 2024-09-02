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
          failedJobs: [],
          cronJobs: []
        };
        await blobClient.upload(JSON.stringify(initialMetadata), JSON.stringify(initialMetadata).length);
      } else {
        throw error;
      }
    }
  }

  async addJob(queueName, jobId, jobData) {
    await this.ensureMetadataExists(queueName);
    
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
        metadata.completedJobs.push(jobId);
      } else if (jobData.status === 'failed') {
        metadata.inProgressJobs = metadata.inProgressJobs.filter(id => id !== jobId);
        metadata.failedJobs.push(jobId);
      }
      return metadata;
    });
  }

  async getJobData(queueName, jobId) {
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/${jobId}.json`);
    try {
      const downloadResponse = await blobClient.download();
      const downloaded = await this._streamToBuffer(downloadResponse.readableStreamBody);
      return JSON.parse(downloaded.toString());
    } catch (error) {
      if (error.statusCode === 404) { // Not Found
        return null;
      }
      throw error;
    }
  }

  async getCronJobById(queueName, cronJobId) {
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/cron/${cronJobId}.json`);
    try {
      const downloadResponse = await blobClient.download();
      const downloadedContent = await this._streamToBuffer(downloadResponse.readableStreamBody);
      return JSON.parse(downloadedContent.toString());
    } catch (error) {
      if (error.statusCode === 404) { // Not Found
        return null;
      }
      throw error;
    }
  }
  
  async getAbandonedJobs(queueName, timeout) {
    const metadata = await this._getQueueMetadata(queueName);
    const now = Date.now();
    const abandonedJobs = [];

    for (const jobId of metadata.inProgressJobs) {
      const jobData = await this.getJobData(queueName, jobId);
      if (now - jobData.lastHeartbeat > timeout) {
        abandonedJobs.push({ id: jobId, data: jobData });
      }
    }

    return abandonedJobs;
  }

  async addCronJob(queueName, cronJobId, cronJobInfo) {
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/cron/${cronJobId}.json`);
    await blobClient.upload(JSON.stringify(cronJobInfo), JSON.stringify(cronJobInfo).length);

    await this._updateQueueMetadata(queueName, metadata => {
      if (!metadata.cronJobs) {
        metadata.cronJobs = [];
      }
      metadata.cronJobs.push(cronJobId);
      return metadata;
    });
  }

  async getCronJobs(queueName) {
    const metadata = await this._getQueueMetadata(queueName);
    const cronJobs = [];

    if (metadata.cronJobs) {
      for (const cronJobId of metadata.cronJobs) {
        const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/cron/${cronJobId}.json`);
        try {
          const downloadResponse = await blobClient.download();
          const downloadedContent = await this._streamToBuffer(downloadResponse.readableStreamBody);
          cronJobs.push(JSON.parse(downloadedContent.toString()));
        } catch (error) {
          console.error(`Error loading cron job ${cronJobId}:`, error);
        }
      }
    }

    return cronJobs;
  }

  async removeCronJob(queueName, cronJobId) {
    const blobClient = this.containerClient.getBlockBlobClient(`${queueName}/cron/${cronJobId}.json`);
    await blobClient.delete();

    await this._updateQueueMetadata(queueName, metadata => {
      if (metadata.cronJobs) {
        metadata.cronJobs = metadata.cronJobs.filter(id => id !== cronJobId);
      }
      return metadata;
    });
  }

  async acquireLock(queueName, cronJobId, podId, duration) {
    const lockBlobClient = this.containerClient.getBlockBlobClient(`${queueName}/locks/${cronJobId}.lock`);
    const lockContent = JSON.stringify({ podId, expiresAt: Date.now() + duration });
  
    try {
      await lockBlobClient.upload(lockContent, lockContent.length, { conditions: { ifNoneMatch: "*" } });
      return true;
    } catch (error) {
      if (error.statusCode === 412) { // Precondition Failed, blob already exists
        const existingLock = await this._getLockInfo(lockBlobClient);
        if (existingLock && existingLock.expiresAt > Date.now()) {
          const error = new Error('Lock already exists');
          error.code = 'LockAlreadyExists';
          throw error;
        }
        // Lock has expired, overwrite it
        await lockBlobClient.upload(lockContent, lockContent.length, { overwrite: true });
        return true;
      } else {
        throw error;
      }
    }
  }

  async releaseLock(queueName, cronJobId, podId) {
    const lockBlobClient = this.containerClient.getBlockBlobClient(`${queueName}/locks/${cronJobId}.lock`);
    try {
      const existingLock = await this._getLockInfo(lockBlobClient);
      if (existingLock && existingLock.podId === podId) {
        await lockBlobClient.delete();
      }
    } catch (error) {
      if (error.statusCode !== 404) { // Not Found is okay, lock might have expired
        throw error;
      }
    }
  }

  async _getLockInfo(lockBlobClient) {
    try {
      const downloadResponse = await lockBlobClient.download();
      const downloadedContent = await this._streamToBuffer(downloadResponse.readableStreamBody);
      return JSON.parse(downloadedContent.toString());
    } catch (error) {
      if (error.statusCode === 404) { // Not Found
        return null;
      }
      throw error;
    }
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