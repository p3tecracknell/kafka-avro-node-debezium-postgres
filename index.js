'use strict'

const KafkaAvro = require('kafka-avro')
const { DEBUG_LEVEL } = process.env

const DEBEZIUM_OP_CREATE = 'c'
const DEBEZIUM_OP_UPDATE = 'u'
const DEBEZIUM_OP_DELETE = 'd'

let stream, topicFunctionGroups = {}

function setupHandler(topic, fn, event, parsedType) {
    if (!topicFunctionGroups[topic]) topicFunctionGroups[topic] = {}

    topicFunctionGroups[topic][event] = async (message) => {
        const record = message.parsed[parsedType][topic + '.Value']
        await fn(record)
    }
}

module.exports = {
    onCreate: async (topic, fn) => setupHandler(topic, fn, 'create', 'after'),
    onUpdate: async (topic, fn) => setupHandler(topic, fn, 'update', 'after'),
    onDelete: async (topic, fn) => setupHandler(topic, fn, 'delete', 'before'),

    setupStream: async function(kafkaAvroConfig, consumerSettings) {
        const kafkaAvro = new KafkaAvro(kafkaAvroConfig)
        const topics = kafkaAvroConfig.topics
        if (!topics) throw 'topics not provided in kafkaAvroConfig'
        
        await kafkaAvro.init()

        stream = await kafkaAvro.getConsumer(consumerSettings)
      
        await stream.connect({}, console.log)
      
        stream.on('ready', () => {
            console.log('Connected to consumer')
            stream.subscribe(topics)
            
            stream.consume()
        
            stream.on('data', async function (message) {
                if (DEBUG_LEVEL === 'verbose') console.log(JSON.stringify(message, null, 4))
                const { parsed, topic } = message
                if (!parsed) return // TODO investigate. I believe there are 2 delete records
                
                const operation = parsed.op
                const topicFunctionGroup = topicFunctionGroups[topic]
                if (operation === DEBEZIUM_OP_CREATE) await topicFunctionGroup.create(message)
                else if (operation === DEBEZIUM_OP_UPDATE) await topicFunctionGroup.update(message)
                else if (operation === DEBEZIUM_OP_DELETE) await topicFunctionGroup.delete(message)
                stream.commitMessage(message)
            })
        
            stream.on('error', console.log)
        })
    }      
}