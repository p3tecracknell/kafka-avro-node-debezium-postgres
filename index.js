'use strict'

const KafkaAvro = require('kafka-avro')
const { DEBUG_LEVEL } = process.env

const DEBEZIUM_OP_CREATE = 'c'
const DEBEZIUM_OP_UPDATE = 'u'
const DEBEZIUM_OP_DELETE = 'd'

module.exports = async function(kafkaAvroConfig, consumerSettings) {
    const topicFunctionGroups = {}
    const kafkaAvro = new KafkaAvro(kafkaAvroConfig)
    const topics = kafkaAvroConfig.topics
    if (!topics) throw 'topics not provided in kafkaAvroConfig'
    
    await kafkaAvro.init()

    const stream = await kafkaAvro.getConsumer(consumerSettings)
    
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
            if (operation === DEBEZIUM_OP_CREATE) await executeListeners('create', topic, message)
            else if (operation === DEBEZIUM_OP_UPDATE) await executeListeners('update', topic, message)
            else if (operation === DEBEZIUM_OP_DELETE) await executeListeners('delete', topic, message)
            stream.commitMessage(message)
        })
    
        stream.on('error', console.log)
    })

    async function executeListeners(event, topic, message) {
        // Look for handlers for this topic. Stop if there aren't any setup
        const topicFunctionGroup = topicFunctionGroups[topic]
        if (!topicFunctionGroup) return
    
        // Look for the specific listener
        const listeners = topicFunctionGroup[event]
        for (let listener of listeners) {
            await listener(message)
        }
    }

    return {
        addListener: async function(event, topic, fn) {
            let parsedType
            if (event === 'create' || event === 'update') {
                parsedType = 'after'
            } else if (event === 'delete') {
                parsedType = 'before'
            } else {
                throw new Error('Unexpected event type. Must be create, update or delete')
            }
    
            if (!topicFunctionGroups[topic]) topicFunctionGroups[topic] = {
                create: [],
                update: [],
                delete: []
            }
    
            const newListener = async (message) => {
                const record = message.parsed[parsedType][topic + '.Value']
                await fn(record)
            }
    
            topicFunctionGroups[topic][event].push(newListener)
        }
    }
}