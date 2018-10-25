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

#### Sample docker run
```bash
#!/bin/bash
set -e
docker run --restart unless-stopped --env-file .env -v /etc/group:/etc/group:ro -v /etc/passwd:/etc/passwd:ro -u $( id -u $USER ):$( id -g $USER ) -v /mnt/data/user_drive:/mnt/data/user_drive --name alti-downloader -d jackytck/cloud-downloader-docker:v1.0.1
docker ps
docker logs cloud-downloader
set +e
```

#### .env
```bash
#!/bin/bash
# set timezone
TZ=Asia/Hong_Kong
```
