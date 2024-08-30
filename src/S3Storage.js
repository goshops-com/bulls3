const AWS = require('aws-sdk');

class S3Storage {
  constructor(config) {
    this.bucketName = config.bucketName;
    this.s3 = new AWS.S3({
      endpoint: config.endpoint,
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
      s3ForcePathStyle: true,
      signatureVersion: 'v4'
    });
  }

  async addJob(queueName, jobId, jobData) {
    await this.s3.putObject({
      Bucket: this.bucketName,
      Key: `${queueName}/${jobId}.json`,
      Body: JSON.stringify(jobData),
      ContentType: 'application/json'
    }).promise();
  }

  async getNextJob(queueName) {
    const response = await this.s3.listObjectsV2({
      Bucket: this.bucketName,
      Prefix: `${queueName}/`
    }).promise();

    for (const item of response.Contents || []) {
      const jobId = item.Key.split('/').pop().split('.')[0];
      const job = await this.getJob(queueName, jobId);
      if (job && job.data.status === 'waiting') {
        job.data.status = 'active';
        await this.updateJob(queueName, jobId, job.data);
        return job;
      }
    }

    return null;
  }

  async updateJob(queueName, jobId, jobData) {
    await this.s3.putObject({
      Bucket: this.bucketName,
      Key: `${queueName}/${jobId}.json`,
      Body: JSON.stringify(jobData),
      ContentType: 'application/json'
    }).promise();
  }

  async removeJob(queueName, jobId) {
    await this.s3.deleteObject({
      Bucket: this.bucketName,
      Key: `${queueName}/${jobId}.json`
    }).promise();
  }

  async listJobs(queueName) {
    const response = await this.s3.listObjectsV2({
      Bucket: this.bucketName,
      Prefix: `${queueName}/`
    }).promise();

    const jobs = await Promise.all((response.Contents || []).map(async (item) => {
      const jobId = item.Key.split('/').pop().split('.')[0];
      return await this.getJob(queueName, jobId);
    }));

    return jobs.filter(job => job !== null);
  }

  async getJob(queueName, jobId) {
    try {
      const response = await this.s3.getObject({
        Bucket: this.bucketName,
        Key: `${queueName}/${jobId}.json`
      }).promise();

      const data = JSON.parse(response.Body.toString());
      return { id: jobId, data };
    } catch (error) {
      if (error.code === 'NoSuchKey') {
        return null;
      }
      throw error;
    }
  }

  async clearQueue(queueName) {
    const response = await this.s3.listObjectsV2({
      Bucket: this.bucketName,
      Prefix: `${queueName}/`
    }).promise();

    if (response.Contents && response.Contents.length > 0) {
      await this.s3.deleteObjects({
        Bucket: this.bucketName,
        Delete: {
          Objects: response.Contents.map(item => ({ Key: item.Key }))
        }
      }).promise();
    }
  }
}

module.exports = S3Storage;