import AliOSS from 'ali-oss-extra'
import AwsCli from 'aws-cli-js-jt'
import { Client as MinioClient } from 'minio'
import amqp from 'amqplib'
import base64 from 'base-64'
import chalk from 'chalk'
import crypto from 'crypto'
import fileExtension from 'file-extension'
import fs from 'fs-promise'
import im from 'altiimagemagick'
import moment from 'moment'
import pTimeout from 'p-timeout'
import { pick } from 'lodash'
import sharp from 'sharp'

/**
 * Data directories.
 */
fs.ensureDir('./data/downloading')

/**
 * Global constants.
 */
const {
  RABBIT_USER,
  RABBIT_PASSWORD,
  RABBIT_HOST,
  RABBIT_PORT,
  RABBIT_QUEUE,
  RABBIT_QUEUE_FAIL,
  RABBIT_QUEUE_CHANGE_STATE,
  RABBIT_QUEUE_SUCCESS,
  RABBIT_QUEUE_MODEL,
  RABBIT_QUEUE_MODEL_RESULT,
  RABBIT_EXCHANGE_ALTI_HEART_PING,
  RABBIT_QUEUE_ALTI_HEART_PONG,
  BUCKETS,
  S3_CHINA_BUCKETS,
  ALTI_VERBOSE,
  ALTI_HOST_NAME,
  ALTI_HOST_TYPE,
  ALTI_NICK_NAME,
  CONCURRENT_DOWNLOAD,
  CONCURRENT_DOWNLOAD_MODEL,
  OSS_GET_TIMEOUT,
  TRANSFER_TIMEOUT
} = process.env

/**
 * Bucket regions.
 */
const bucketRegionList = BUCKETS.split(';').map(x => x.split('@'))
const S3BucketToRegion = new Map(bucketRegionList)
const S3ChinaBuckets = S3_CHINA_BUCKETS.split(';')

const log = ALTI_VERBOSE ? console.log : () => {}

/**
 * RabbitMQ.
 */
let rabbitChannel = null

async function connectRabbit () {
  try {
    const uri = `amqp://${RABBIT_USER}:${RABBIT_PASSWORD}@${RABBIT_HOST}:${RABBIT_PORT}`
    const connection = await amqp.connect(uri)

    rabbitChannel = await connection.createChannel()

    // ImageChannelFail = await connection.createChannel()
    await rabbitChannel.assertQueue(RABBIT_QUEUE_FAIL, { durable: true })

    // ImageChannelChangeState = await connection.createChannel()
    await rabbitChannel.assertQueue(RABBIT_QUEUE_CHANGE_STATE, { durable: true })

    // ImageChannelSuccess = await connection.createChannel()
    await rabbitChannel.assertQueue(RABBIT_QUEUE_SUCCESS, { durable: true })

    // ImageChannel = await connection.createChannel()
    await rabbitChannel.assertQueue(RABBIT_QUEUE, { durable: true })
    rabbitChannel.prefetch(+CONCURRENT_DOWNLOAD)
    rabbitChannel.consume(RABBIT_QUEUE, work)

    // for model upload
    await rabbitChannel.assertQueue(RABBIT_QUEUE_MODEL, { durable: true })
    await rabbitChannel.assertQueue(RABBIT_QUEUE_MODEL_RESULT, { durable: true })
    rabbitChannel.prefetch(+CONCURRENT_DOWNLOAD_MODEL)
    rabbitChannel.consume(RABBIT_QUEUE_MODEL, workModel)

    // for heartbeat ping pong
    await rabbitChannel.assertExchange(RABBIT_EXCHANGE_ALTI_HEART_PING, 'fanout', { durable: true })
    const tmpQueue = await rabbitChannel.assertQueue('', { exclusive: true })
    await rabbitChannel.bindQueue(tmpQueue.queue, RABBIT_EXCHANGE_ALTI_HEART_PING, '')
    rabbitChannel.consume(tmpQueue.queue, handlePing.bind(this, ALTI_VERBOSE), { noAck: true })
    await rabbitChannel.assertQueue(RABBIT_QUEUE_ALTI_HEART_PONG, { durable: true })

    console.log(chalk.inverse(`Connected ${RABBIT_HOST}:${RABBIT_PORT}`))
  } catch (err) {
    console.error(err)
  }
}

/**
 * Pretty print the machine in console.
 */
function logMachine (machine) {
  console.log(chalk.magenta(JSON.stringify(machine, null, 2)))
}

/**
 * Pretty print the file in console.
 */
function logFile (file) {
  console.log(chalk.magenta(JSON.stringify(file, null, 2)))
}

/**
 * Handle ping message from alti-heartbeater.
 */
async function handlePing (verbose, message) {
  const msg = message.content.toString()
  log(chalk.cyan('Received a Ping:'))
  log(msg)
  try {
    const mach = JSON.parse(msg)
    mach.name = ALTI_HOST_NAME
    mach.nickname = ALTI_NICK_NAME
    mach.type = ALTI_HOST_TYPE
    mach.pong = moment().format('YYYY-MM-DD hh:mm:ss.SSSS A')
    const pong = pick(mach, [
      'name',
      'nickname',
      'type',
      'ping',
      'pong',
      'extra'
    ])
    await rabbitChannel.sendToQueue(RABBIT_QUEUE_ALTI_HEART_PONG, Buffer.from(JSON.stringify(pong)))
    if (verbose) {
      logMachine(pong)
      console.log(chalk.cyan('Done Pong'))
    }
  } catch (err) {
    console.error(err)
  }
}

/**
 * Cloud clients.
 */
const awsCli = new AwsCli({
  aws_access_key_id: process.env.AWS_ACCESS_KEY_ID,
  aws_secret_access_key: process.env.AWS_SECRET_ACCESS_KEY
})
const awsCliChina = new AwsCli({
  aws_access_key_id: process.env.AWS_CHINA_ACCESS_KEY_ID,
  aws_secret_access_key: process.env.AWS_CHINA_SECRET_ACCESS_KEY
})
const aliBuckets = process.env.ALI_BUCKETS.split(',')
const aliRegions = process.env.ALI_REGIONS.split(',')
const ossCli = new Map()
aliBuckets.forEach((bucket, i) => {
  const region = aliRegions[i]
  const client = new AliOSS({
    accessKeyId: process.env.ALI_SDK_OSS_ID,
    accessKeySecret: process.env.ALI_SDK_OSS_SECRET,
    bucket,
    region
  })
  ossCli.set(bucket, client)
})

const minioClient = new MinioClient({
  endPoint: process.env.MINIO_ENDPOINT,
  port: +process.env.MINIO_PORT,
  secure: process.env.MINIO_SECURE === 'true',
  accessKey: process.env.MINIO_ACCESS,
  secretKey: process.env.MINIO_SECRET
})

/**
 * Download single file from S3 to local downloading directory.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function downloadFromS3 (file) {
  return new Promise(async (resolve, reject) => {
    try {
      const { bucket, pid, filename } = file
      log(`Downloading from s3://${bucket}`)
      const localPath = `./data/downloading/${pid}/${filename}`
      const cmd = `s3 cp s3://${bucket}/${pid}/${filename} ${localPath} --region ${S3BucketToRegion.get(bucket)}`
      await awsCli.command(cmd)
      file.localPath = localPath
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

/**
 * Download single file from China S3 to local downloading directory.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function downloadFromS3China (file) {
  return new Promise(async (resolve, reject) => {
    try {
      const { bucket, pid, filename } = file
      log(`Downloading from s3://${bucket}`)
      const localPath = `./data/downloading/${pid}/${filename}`
      const cmd = `s3 cp s3://${bucket}/${pid}/${filename} ${localPath} --region ${S3BucketToRegion.get(bucket)}`
      await awsCliChina.command(cmd)
      file.localPath = localPath
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

/**
 * Download single file from OSS to local downloading directory.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function downloadFromOSS (file) {
  return new Promise(async (resolve, reject) => {
    try {
      const { bucket, pid, filename } = file
      log(`Downloading from oss://${bucket}`)
      const localDir = `./data/downloading/${pid}`
      const localPath = `${localDir}/${filename}`
      const client = ossCli.get(bucket)
      await fs.ensureDir(localDir)
      await client.get(`${pid}/${filename}`, localPath, { timeout: +OSS_GET_TIMEOUT })
      file.localPath = localPath
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

function downloadFromMinio (file) {
  return new Promise(async (resolve, reject) => {
    const { bucket, pid, filename } = file
    log(`Downloading from minio://${bucket}`)
    const remotePath = `${pid}/${filename}`
    const localPath = `./data/downloading/${pid}/${filename}`
    file.localPath = localPath
    minioClient.fGetObject(bucket, remotePath, localPath, err => {
      if (err) {
        reject(err)
      }
      resolve()
    })
  })
}

/**
 * Download single file from S3 or OSS.
 * Expects file to have:
 * a. cloud
 * b. bucket
 * c. pid
 * d. filename
 * Will set:
 * a. localPath
 */
function download (file) {
  return new Promise(async (resolve, reject) => {
    try {
      file.state = 'Downloading'
      switch (file.cloud) {
        case 'S3':
          if (S3ChinaBuckets.includes(file.bucket)) {
            await downloadFromS3China(file)
          } else {
            await downloadFromS3(file)
          }
          break
        case 'OSS':
          await downloadFromOSS(file)
          break
        case 'MINIO':
          await downloadFromMinio(file)
          break
        default:
          file.state = 'Failed'
          file.error.push(`Unknown Cloud: ${file.cloud}`)
      }
      file.state = 'Downloaded'
      log(`Downloaded ${file.iid}`)
      resolve()
    } catch (err) {
      file.error.push('Download Error')
      reject(err)
    }
  })
}

/**
 * Get the dominant color of an image in GIF formatted base64 string.
 * e.g. R0lGODlhAwACAPIFAFlbVF1cW15eWG5xcXxya3BycwAAAAAAACH5BAAAAAAALAAAAAADAAIAAAMEOCUEkgA7
 * Usage:
 * <img  src="data:gif;base64,R0lGODlhAwACAPIFAFlbVF1cW15eWG5xcXxya3BycwAAAAAAACH5BAAAAAAALAAAAAADAAIAAAMEOCUEkgA7">
 */
function dominantColor (srcPath) {
  return new Promise((resolve, reject) => {
    const opts = {
      srcPath,
      width: 3,
      height: 3,
      format: 'gif'
    }
    im.resize(opts, (error, data) => {
      if (error) {
        return reject(error)
      }
      resolve(base64.encode(data))
    })
  })
}

/**
 * Extract width, height, giga-pixel, file size and sha1 checksum of an image.
 * Expects file to have:
 * a. localPath
 * b. pid
 * c. filename
 * Will set:
 * a. width
 * b. height
 * c. gigaPixel
 * d. fileSize
 * e. contentHash
 * f. dominantColor
 */
function extractInfo (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Extracting info: ${file.iid} ${file.filename}`)
      // width and height
      const image = sharp(file.localPath)
      const [meta, dominant] = await Promise.all([
        image.metadata(),
        dominantColor(file.localPath)
        // vibrantColor(file.localPath)
      ])
      const { width, height } = meta
      file.width = width
      file.height = height
      file.dominantColor = dominant
      // file.vibrantColor = palette

      // giga-pixel
      file.gigaPixel = Math.max(7990272, width * height) / 1000000000
      const stats = await fs.stat(file.localPath)

      // file size
      file.fileSize = stats.size

      // content hash
      const sha1 = crypto.createHash('sha1')
      sha1.setEncoding('hex')
      fs.createReadStream(file.localPath).pipe(sha1)
      sha1.on('finish', () => {
        file.contentHash = sha1.read()
        log(`Extracted ${file.iid}`)
        resolve()
      })
    } catch (err) {
      file.error.push('Extract Info Error')
      reject(err)
    }
  })
}

/**
 * Resize images into various thumb sizes.
 * Expects file to have:
 * a. localPath
 * b. pid
 * c. filename
 * Will set:
 * a. localThumbPath
 * b. localThumbName
 * c. thumbSize
 * d. resized
 */
function resize (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Resizing: ${file.iid} ${file.filename}`)
      // assume there is an extension
      const extension = fileExtension(file.filename)
      const image = sharp(file.localPath)
      const thumbSizes = process.env.THUMB_SIZE.split(',')
      const filePathWithoutExt = file.localPath.substring(0, file.localPath.length - extension.length - 1)
      const thumbs = thumbSizes.map(size => ({
        width: +size,
        path: `${filePathWithoutExt}_${size}.${extension}`,
        filename: `${file.filename.substring(0, file.filename.length - extension.length - 1)}_${size}.${extension}`
      }))
      const jobs = thumbs.map(j => image.resize(j.width).toFile(j.path))
      await Promise.all(jobs)
      file.localThumbPath = thumbs.map(t => t.path)
      file.localThumbName = thumbs.map(t => t.filename)
      file.thumbSize = thumbSizes.map(t => +t)
      file.resized = true
      log('Resized')
      resolve()
    } catch (err) {
      file.error.push('Resize Error')
      reject(err)
    }
  })
}

/**
 * Transfer file to master.
 * Expects file to have:
 * a. localPath
 * b. pid
 * c. filename
 * e. localThumbPath
 * f. localThumbName
 * g. thumbSize
 * Will set:
 * a. remotePath
 * b. remoteThumbPath
 * Transfer original file and its resized thumbs to master.
 */
function transfer (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Transferring: ${file.iid} ${file.filename}`)
      // assume there is an extension
      const extension = fileExtension(file.filename)
      const remotePath = `${process.env.MASTER_DRIVE_MOUNT}/${file.pid}`
      const remotePathSmall = `${remotePath}/.small`
      await fs.ensureDir(remotePath)
      await fs.ensureDir(remotePathSmall)
      await fs.copy(file.localPath, `${remotePath}/${file.filename}`)
      for (let i = 0; i < file.localThumbPath.length; i++) {
        await fs.copy(file.localThumbPath[i], `${remotePathSmall}/${file.localThumbName[i]}`)
      }

      file.remotePath = `${remotePath}/${file.filename}`
      const filenameWithoutExt = file.filename.substring(0, file.filename.length - extension.length - 1)
      file.remoteThumbPath = file.thumbSize.map(s => `${remotePath}/.small/${filenameWithoutExt}_${s}.${extension}`)
      file.state = 'Transfered'
      log('Transferred')
      resolve()
    } catch (err) {
      file.state = 'Failed'
      file.error.push('Transfer Error')
      reject(err)
    }
  })
}

/**
 * Remove original file and its resized thumbs.
 * Expects file to have:
 * a. localPath
 * b. pid
 * c. filename
 * d. localThumbPath
 */
function cleanLocal (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Cleaning local files: ${file.iid} ${file.filename}`)
      let jobs = [file.localPath]
      if (file.localThumbPath && file.localThumbPath.length) {
        jobs = jobs.concat(file.localThumbPath)
      }
      jobs = jobs.map(j => fs.remove(j))
      await Promise.all(jobs)
      file.state = 'Cleaned Local'
      log('Cleaned Local')
      resolve()
    } catch (err) {
      file.state = 'Failed'
      file.error.push('Clean Local Error')
      reject(err)
    }
  })
}

/**
 * Remove file from S3.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function cleanS3 (file) {
  return new Promise(async (resolve, reject) => {
    try {
      const { bucket, pid, filename } = file
      const cmd = `s3 rm s3://${bucket}/${pid}/${filename} --region ${S3BucketToRegion.get(bucket)}`
      await awsCli.command(cmd)
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

/**
 * Remove file from S3 China.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function cleanS3China (file) {
  return new Promise(async (resolve, reject) => {
    try {
      const { bucket, pid, filename } = file
      const cmd = `s3 rm s3://${bucket}/${pid}/${filename} --region ${S3BucketToRegion.get(bucket)}`
      await awsCliChina.command(cmd)
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

/**
 * Remove file from OSS.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function cleanOSS (file) {
  return new Promise(async (resolve, reject) => {
    try {
      const { bucket, pid, filename } = file
      const client = ossCli.get(bucket)
      await client.delete(`${pid}/${filename}`)
      resolve()
    } catch (err) {
      reject(err)
    }
  })
}

/**
 * Remove file from Minio.
 * Expects file to have:
 * a. bucket
 * b. pid
 * c. filename
 */
function cleanMinio (file) {
  return new Promise(async (resolve, reject) => {
    const { bucket, pid, filename } = file
    const objName = `${pid}/${filename}`
    minioClient.removeObject(bucket, objName, err => {
      if (err) {
        reject(err)
      }
      resolve()
    })
  })
}

/**
 * Remove original file from cloud.
 * Expects file to have:
 * a. cloud
 * b. bucket
 * c. pid
 * d. filename
 */
function cleanCloud (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Cleaning cloud files: ${file.iid} ${file.filename}`)
      switch (file.cloud) {
        case 'S3':
          if (S3ChinaBuckets.includes(file.bucket)) {
            await cleanS3China(file)
          } else {
            await cleanS3(file)
          }
          break
        case 'OSS':
          await cleanOSS(file)
          break
        case 'MINIO':
          await cleanMinio(file)
          break
        default:
          file.state = 'Failed'
          file.error.push(`Unknown Cloud: ${file.cloud}`)
      }
      log('Cleaned Cloud')
      resolve()
    } catch (err) {
      console.error(err)
      file.state = 'Failed'
      file.error.push('Clean Cloud Error')
      reject(err)
    }
  })
}

/**
 * Change the file state and publish to rabbit.
 */
function changeState (file, state) {
  file.state = state
  rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE_CHANGE_STATE, Buffer.from(JSON.stringify(file)), { persistent: true })
  return file
}

/**
 * Main work:
 * a. download
 * b. extract image size and compute giga pixel and sha1 checksum
 * c. resize into various thumb sizes
 * d. trasfer to master
 * e. clean up downloaded local files in downloading directory
 * f. clean up uploaded files in cloud
 * g. produce a success rabbit message
 *
 * Expects file to have:
 * a. cloud
 * b. bucket
 * c. pid
 * d. filename
 *
 * Sample Rabbit message:
 * {
 *   "iid":"59116462690e235bafa4b6eb",
 *   "retry":0,
 *   "error":[
 *   ],
 *   "cloud":"S3",
 *   "bucket":"nat-image-test-s3",
 *   "pid":"59116462690e235bafa4b6e6",
 *   "filename":"853dadd6-edb6-4c7e-a01c-2907d6d5baa9.jpg"
 * }
 */
async function work (message) {
  let file = {}
  try {
    console.log(chalk.cyan('Received a download message:'))
    file = JSON.parse(message.content.toString())
    logFile(file)
    file = changeState(file, 'Verifying')
    await download(file)
    await extractInfo(file)
    await resize(file)
    file = changeState(file, 'Copying')
    await pTimeout(transfer(file), TRANSFER_TIMEOUT * 1000, `transfer timed out after ${TRANSFER_TIMEOUT} seconds!`)
    await cleanLocal(file)
    await pTimeout(cleanCloud(file), 120 * 1000, 'cleanCloud timed out after 120 seconds!')
    file.state = 'Ready'
    console.log(chalk.cyan('Ready'))
    rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE_SUCCESS, Buffer.from(JSON.stringify(file)), { persistent: true })
  } catch (err) {
    console.error('work error', err)
    file.error.push(err.toString())
    console.log(chalk.red('Failed', file.iid))

    if (file.retry < process.env.RETRY_LIMIT) {
      console.log(chalk.yellow('Retrying...'))
      file.retry++
      file.error = []
      setTimeout(_ => {
        rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE, Buffer.from(JSON.stringify(file)), { persistent: true })
      }, 2000)
    } else {
      try {
        // send to fail queue first, in case error is thrown in cleaning
        rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE_FAIL, Buffer.from(JSON.stringify(file)), { persistent: true })

        // file may not exist locally
        await cleanLocal(file)
        await cleanCloud(file)
      } catch (err) {
        console.error(err)
      }
    }
  }
  rabbitChannel.ack(message)
}

/**
 * Get filesize and compute sha1 hash.
 */
function extractInfoModel (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Extracting info: ${file.iid} ${file.filename}`)

      // file size
      const stats = await fs.stat(file.localPath)
      file.fileSize = stats.size

      // content hash
      const sha1 = crypto.createHash('sha1')
      sha1.setEncoding('hex')
      fs.createReadStream(file.localPath).pipe(sha1)
      sha1.on('finish', () => {
        file.contentHash = sha1.read()
        log(`Extracted ${file.iid}`)
        resolve()
      })
    } catch (err) {
      file.error.push('Extract Info Error')
      reject(err)
    }
  })
}

/**
 * Transfer model to master.
 */
function transferModel (file) {
  return new Promise(async (resolve, reject) => {
    try {
      log(`Transferring: ${file.iid} ${file.filename}`)
      // assume there is an extension
      const remotePath = `${process.env.MASTER_DRIVE_MOUNT}/${file.pid}`
      await fs.ensureDir(remotePath)
      await fs.copy(file.localPath, `${remotePath}/${file.filename}`)
      file.remotePath = `${remotePath}/${file.filename}`
      file.state = 'Transfered'
      log('Transferred')
      resolve()
    } catch (err) {
      file.state = 'Failed'
      file.error.push('Transfer Error')
      reject(err)
    }
  })
}

/**
 * Download zipped obj from cloud and copy to master.
 * Sample Rabbit message:
 * {
 *   "iid": "59805db81cfeea696b1e9a1b",
 *   "retry": 0,
 *   "error": [],
 *   "cloud": "S3",
 *   "bucket": "nat-model-singapore",
 *   "pid": "59229b5ee964657e1ff17fa7",
 *   "filename": "07cdb427-14fa-4143-95ff-3c52ab4d1668.zip"
 * }
 */
async function workModel (message) {
  let file = {}
  try {
    console.log(chalk.cyan('Received a download message:'))
    file = JSON.parse(message.content.toString())
    logFile(file)
    await download(file)
    await extractInfoModel(file)
    await transferModel(file)
    await cleanLocal(file)
    await cleanCloud(file)
    file.state = 'Ready'
    console.log(chalk.cyan('Ready'))
    rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE_MODEL_RESULT, Buffer.from(JSON.stringify(file)), { persistent: true })
  } catch (err) {
    console.error('workModel error', err)
    file.error.push(err.toString())
    console.log(chalk.red('Failed', file.iid))

    if (file.retry < process.env.RETRY_LIMIT) {
      console.log(chalk.yellow('Retrying...'))
      file.retry++
      file.error = []
      setTimeout(_ => {
        rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE_MODEL, Buffer.from(JSON.stringify(file)), { persistent: true })
      }, 2000)
    } else {
      try {
        // send to fail queue first, in case error is thrown in cleaning
        file.state = 'Failed'
        rabbitChannel.sendToQueue(process.env.RABBIT_QUEUE_MODEL_RESULT, Buffer.from(JSON.stringify(file)), { persistent: true })

        // file may not exist locally
        await cleanLocal(file)
        await cleanCloud(file)
      } catch (err) {
        console.error(err)
      }
    }
  }
  rabbitChannel.ack(message)
}

function main () {
  console.log('Starting cloud-downloader...')
  connectRabbit()
}

main()
