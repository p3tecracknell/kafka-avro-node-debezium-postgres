'use strict'

require('dotenv').config()
const kafkaHandler = require('./index.js')
const {
    KAFKA_BOOTSTRAP_BROKER,
    KAFKA_KEY,
    KAFKA_SECRET,
    SCHEMA_REGISTRY_URL,
    KAFKA_CUSTOMERS_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_SOCKET_BLOCKING_MAX_MS
} = process.env

async function main() {
    const topics = [KAFKA_CUSTOMERS_TOPIC]
    const kafkaAvroConfig = {
      kafkaBroker: KAFKA_BOOTSTRAP_BROKER,
      schemaRegistry: SCHEMA_REGISTRY_URL,
      topics: topics,
      shouldFailWhenSchemaIsMissing: true
    }

    let kafkaConsumerSettings = {
      'group.id': KAFKA_GROUP_ID,
      'socket.keepalive.enable': true,
      'enable.auto.commit': false,
      'socket.blocking.max.ms': KAFKA_SOCKET_BLOCKING_MAX_MS || 100,
      //'debug': 'all',
      'auto.offset.reset': 'earliest',
      'sasl.mechanisms': 'PLAIN'
    }

    if (KAFKA_KEY) {
      kafkaConsumerSettings = { ...kafkaConsumerSettings, 
        'security.protocol': 'SASL_SSL',
        'sasl.username': KAFKA_KEY,
        'sasl.password': KAFKA_SECRET
      }
    }

    await kafkaHandler.setupStream(kafkaAvroConfig, kafkaConsumerSettings)
}

kafkaHandler.onCreate(KAFKA_CUSTOMERS_TOPIC, async function(record) {
    console.log('Customer created')
    console.log(record)
  })
  
kafkaHandler.onUpdate(KAFKA_CUSTOMERS_TOPIC, async function(record) {
    console.log('Customer updated')
    console.log(record)
  })
  
  kafkaHandler.onDelete(KAFKA_CUSTOMERS_TOPIC, async function(record) {
    console.log('Customer deleted')
    console.log(record)
  })

main()