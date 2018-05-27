const {
  AWS_ACCESS,
  AWS_SECRET,
  CONCURRENCY,
  HOST_NAME,
  HOST_TYPE,
  RABBIT_HOST,
  RABBIT_PING,
  RABBIT_PONG,
  RABBIT_PORT,
  RABBIT_USER,
  RABBIT_PASSWORD,
  RABBIT_QUEUE,
  OSS_ACCESS,
  OSS_SECRET
} = process.env

const config = {
  host: {
    name: HOST_NAME,
    type: HOST_TYPE
  },
  rabbit: {
    host: RABBIT_HOST,
    port: RABBIT_PORT,
    user: RABBIT_USER,
    password: RABBIT_PASSWORD,
    queue: RABBIT_QUEUE,
    ping: RABBIT_PING,
    pong: RABBIT_PONG
  },
  s3: {
    access: AWS_ACCESS,
    secret: AWS_SECRET
  },
  oss: {
    access: OSS_ACCESS,
    secret: OSS_SECRET
  },
  concurrency: CONCURRENCY
}

module.exports = config
