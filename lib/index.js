"use strict";
exports.__esModule = true;
var kafka = require("kafka-node");
var log4js = require("log4js");
var async = require("async");
var consumerGroups = {};
var processCallbacks = {};
var Logger = getLogger();
var Producer = kafka.Producer;
var ConsumerGroup = kafka.ConsumerGroup;
var client = null;
var producer = null;
exports.KafkaProducerService = {
    init: function (options) {
        Logger.info('[KAFKA]', 'Connecting Kafka producer', options.host, options.port);
        client = new kafka.Client(options.host + ':' + options.port);
        producer = new Producer(client);
        producer.on('ready', function () {
            Logger.info('[KAFKA]', 'Kafka producer connected');
        });
        producer.on('error', function (err) {
            Logger.error(err.message);
        });
    },
    send: function (message, topic) {
        return new Promise(function (resolve, reject) {
            var _message = '{}';
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
    addConsumerGroup: function (consumerGroupOptions) {
        Logger.info('[KAFKA]', 'Connecting Kafka consumer group', consumerGroupOptions.name, consumerGroupOptions.kafkaHost, consumerGroupOptions.topics);
        var consumerGroup = new ConsumerGroup({
            kafkaHost: consumerGroupOptions.kafkaHost,
            groupId: consumerGroupOptions.groupId
        }, consumerGroupOptions.topics);
        var queueConcurrency = consumerGroupOptions.queueConcurrency ? consumerGroupOptions.queueConcurrency : 1;
        var q = async.queue(function (message, cb) {
            if (!processCallbacks[message.event]) {
                if (consumerGroupOptions.processCallback) {
                    consumerGroupOptions.processCallback(message, cb);
                }
                else {
                    cb();
                }
            }
            else {
                var processCallback = processCallbacks[message.event];
                processCallback(message.data, function (message) {
                    if (consumerGroupOptions.processCallback) {
                        consumerGroupOptions.processCallback(message, cb);
                    }
                    else {
                        cb();
                    }
                });
            }
        }, queueConcurrency);
        q.saturated = function () {
            consumerGroup.pause();
        };
        q.unsaturated = function () {
            consumerGroup.resume();
        };
        q.drain = function () {
            consumerGroup.resume();
        };
        consumerGroup.on('connect', function () {
            Logger.info('[KAFKA]', 'Kafka consumer group', consumerGroupOptions.name, 'connected');
        });
        consumerGroup.on('message', function (message) {
            try {
                var value = JSON.parse(message.value);
                if (value) {
                    q.push(value, function (err, result) {
                        if (err) {
                            Logger.error(err.message);
                        }
                    });
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
    addProcessCallback: function (event, callback) {
        processCallbacks[event] = callback;
    },
    removeConsumerGroup: function (name) {
        return new Promise(function (resolve, reject) {
            if (consumerGroups[name]) {
                var consumerGroup = consumerGroups[name].consumer;
                var q = consumerGroups[name].q;
                consumerGroup.close(function (err) {
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
    var loggerName = 'kafka';
    var appenders = {};
    appenders[loggerName] = { type: 'stdout' };
    var LOG_LEVEL = process.env['WP_KAFKA_LOG_LEVEL'] ? process.env['WP_KAFKA_LOG_LEVEL'] : 'info';
    log4js.configure({
        appenders: appenders,
        categories: { "default": { appenders: [loggerName], level: LOG_LEVEL } }
    });
    return log4js.getLogger(loggerName);
}
