# BullS3: Distributed Job Queue for S3 and Azure Blob Storage

BullS3 is a distributed job queue system that uses either Amazon S3 or Azure Blob Storage as its backend. It provides a simple and efficient way to manage background jobs in a distributed environment, offering flexibility in cloud storage choices.

## Features

- Distributed job queue using Amazon S3 or Azure Blob Storage
- Simple API similar to Bull
- Job addition, processing, and status management
- Event-based job lifecycle
- Automatic retries and error handling
- Seamless switching between S3 and Azure Blob Storage

## Installation

```bash
npm install bulls3
```

## Configuration

Before using BullS3, you need to set up your cloud storage connection. You'll need:

For Amazon S3:
1. AWS Access Key ID
2. AWS Secret Access Key
3. S3 Bucket Name

For Azure Blob Storage:
1. Azure Storage Account
2. Connection String
3. Container Name

## Usage

Here's a basic example of how to use BullS3 with Azure Blob Storage:

```javascript
const { BullS3, AzureBlobStorage } = require('bulls3');

const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;

const azureStorage = new AzureBlobStorage({
  connectionString,
  containerName: 'my-job-queue'
});

const queue = new BullS3('myQueue', azureStorage);

async function run() {
  await queue.initialize();

  // Add a job
  const jobId = await queue.add({ x: 1, y: 2 });
  console.log(`Added job with ID: ${jobId}`);

  // Process jobs
  queue.process(async (job) => {
    console.log('Processing job:', job.id);
    return job.data.x + job.data.y;
  });

  // Event listeners
  queue.on('added', (job) => console.log('Job added:', job.id));
  queue.on('active', (job) => console.log('Job started:', job.id));
  queue.on('completed', (job) => console.log('Job completed:', job.id, 'Result:', job.data.result));
  queue.on('failed', (job, err) => console.log('Job failed:', job.id, 'Error:', err.message));
}

run().catch(console.error);
```

To use with Amazon S3, simply replace the storage initialization:

```javascript
const { BullS3, S3Storage } = require('bulls3');

const s3Storage = new S3Storage({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  bucketName: 'my-s3-bucket'
});

const queue = new BullS3('myQueue', s3Storage);

// ... rest of the code remains the same
```

## API

### BullS3

#### `constructor(queueName, storage)`

Creates a new BullS3 instance.

- `queueName`: Name of the queue
- `storage`: Instance of S3Storage or AzureBlobStorage

#### `async initialize()`

Initializes the queue. Must be called before adding or processing jobs.

#### `async add(data)`

Adds a new job to the queue.

- `data`: Job data (object)

Returns: Job ID (string)

#### `process(handler)`

Sets up the job processing function.

- `handler`: Async function that processes a job

#### Events

- `'added'`: Emitted when a job is added
- `'active'`: Emitted when a job starts processing
- `'completed'`: Emitted when a job is completed successfully
- `'failed'`: Emitted when a job fails

### S3Storage

#### `constructor(config)`

Creates a new S3Storage instance.

- `config.accessKeyId`: AWS Access Key ID
- `config.secretAccessKey`: AWS Secret Access Key
- `config.bucketName`: Name of the S3 bucket to use

### AzureBlobStorage

#### `constructor(config)`

Creates a new AzureBlobStorage instance.

- `config.connectionString`: Azure Storage connection string
- `config.containerName`: Name of the Azure Blob container to use

## Switching Between S3 and Azure Blob Storage

To switch between S3 and Azure Blob Storage, simply initialize the appropriate storage class and pass it to BullS3. The rest of your code remains unchanged, making it easy to migrate between cloud providers or use different storage solutions in different environments.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.