"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const kafka = require("kafka-node");
const log4js = require("log4js");
const async = require("async");
const QUEUE_CONCURRENCY = 1;
const consumerGroups = {};
const processCallbacks = {};
const Logger = getLogger();
const Producer = kafka.Producer;
const ConsumerGroup = kafka.ConsumerGroup;
let client = null;
let producer = null;
exports.KafkaProducerService = {
    init: (options) => {
        Logger.info('[KAFKA]', 'Connecting Kafka producer', options.host, options.port);
        client = new kafka.Client(options.host + ':' + options.port);
        producer = new Producer(client);
        producer.on('ready', () => {
            Logger.info('[KAFKA]', 'Kafka producer connected');
        });
        producer.on('error', (err) => {
            Logger.error(err.message);
        });
    },
    send: (message, topic) => {
        return new Promise((resolve, reject) => {
            let _message = '{}';
            try {
                _message = JSON.stringify(message);
            }
            catch (err) {
                Logger.error(err.message);
            }
            producer.send([{
                    topic: topic,
                    messages: _message
                }], function (err, data) {
                if (err) {
                    return reject(err);
                }
                else {
                    Logger.trace('[KAFKA]', 'Producer message sent', topic, message);
                    return resolve(data);
                }
            });
        });
    }
};
exports.KafkaConsumerService = {
    addConsumerGroup: (consumerGroupOptions) => {
        Logger.info('[KAFKA]', 'Connecting Kafka consumer group', consumerGroupOptions.name, consumerGroupOptions.kafkaHost, consumerGroupOptions.topics);
        const consumerGroup = new ConsumerGroup({
            kafkaHost: consumerGroupOptions.kafkaHost,
            groupId: consumerGroupOptions.groupId
        }, consumerGroupOptions.topics);
        const q = async.queue((message, cb) => {
            if (!processCallbacks[message.event]) {
                cb();
            }
            else {
                const processCallback = processCallbacks[message.event];
                processCallback(message.data, (message) => {
                    if (consumerGroupOptions.processCallback) {
                        consumerGroupOptions.processCallback(message, cb);
                    }
                    else {
                        cb();
                    }
                });
            }
        }, QUEUE_CONCURRENCY);
        q.drain = () => {
            consumerGroup.resume();
        };
        consumerGroup.on('connect', () => {
            Logger.info('[KAFKA]', 'Kafka consumer group', consumerGroupOptions.name, 'connected');
        });
        consumerGroup.on('message', (message) => {
            try {
                const value = JSON.parse(message.value);
                if (value) {
                    q.push(value, (err, result) => {
                        if (err) {
                            Logger.error(err.message);
                        }
                    });
                    consumerGroup.pause();
                }
                else {
                    Logger.error('[KAFKA]', 'Empty kafka message');
                }
            }
            catch (err) {
                Logger.error(err.message);
            }
        });
        consumerGroup.on('error', function (err) {
            Logger.error('[KAFKA]', 'Kafka error', err.message);
        });
        consumerGroup.on('offsetOutOfRange', function (err) {
            Logger.error('[KAFKA]', 'Kafka offsetOutOfRange', err.message);
        });
        consumerGroups[consumerGroupOptions.name] = {
            consumer: consumerGroup,
            q: q
        };
    },
    addProcessCallback: (event, callback) => {
        processCallbacks[event] = callback;
    },
    removeConsumerGroup: (name) => {
        return new Promise((resolve, reject) => {
            if (consumerGroups[name]) {
                const consumerGroup = consumerGroups[name].consumer;
                const q = consumerGroups[name].q;
                consumerGroup.close((err) => {
                    delete consumerGroups[name];
                    if (err) {
                        return reject(err);
                    }
                    else {
                        return resolve();
                    }
                });
            }
            else {
                return resolve();
            }
        });
    }
};
function getLogger() {
    const loggerName = 'kafka';
    const appenders = {};
    appenders[loggerName] = { type: 'stdout' };
    const LOG_LEVEL = process.env['WP_KAFKA_LOG_LEVEL'] ? process.env['WP_KAFKA_LOG_LEVEL'] : 'info';
    log4js.configure({
        appenders: appenders,
        categories: { default: { appenders: [loggerName], level: LOG_LEVEL } }
    });
    return log4js.getLogger(loggerName);
}
