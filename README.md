### Overview

For downloading files from cloud storage to local disk.

#### Work Flow

1. Listen to new file upload message via rabbit
2. Download file from cloud
3. Do post processing (e.g. image resize, image stats, etc)
4. Save file in local drive
5. Remove file from cloud
6. Ack back file is done via rabbit 

#### Supported Clouds

* AWS S3 International
* AWS S3 China
* Aliyun OSS
* Private Minio Server
