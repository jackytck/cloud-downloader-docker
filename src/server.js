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

function main () {
  console.log('Starting cloud-downloader...')
}

main()
