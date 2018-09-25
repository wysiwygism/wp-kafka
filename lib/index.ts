import * as kafka from "kafka-node";
import log4js = require('log4js');
import * as async from "async";

export interface IKafkaConsumerGroupOptions {
    name: string;
    kafkaHost: string;
    groupId: string;
    topics: string[];
    processCallback: (message: any, cb: (err?: Error) => void) => void;
}
export interface IKafkaMessage {
    event: string;
    data: any;
}
interface IConsumerGroup {
    consumer: any,
    q: any
}

const QUEUE_CONCURRENCY: number = 1;
const consumerGroups: any = {};
const processCallbacks: any = {};
const Logger = getLogger();
const Producer = kafka.Producer;
const ConsumerGroup = kafka.ConsumerGroup;

let client: any = null;
let producer: any = null;

export const KafkaProducerService: any = {
    init: (host: string, port: string): void => {
        Logger.info('[KAFKA]', 'Connecting Kafka producer', host, port);
        client = new kafka.Client(host + ':' + port);
        producer = new Producer(client);
        producer.on('ready', () => {
            Logger.info('[KAFKA]', 'Kafka producer connected');
        });
        producer.on('error', (err: Error) => {
            Logger.error(err.message);
        });
    },
    send: (message: any, topic: string): Promise<void> => {
        return new Promise((resolve, reject) => {
            let _message: string = '{}';
            try {
                _message = JSON.stringify(message);
            } catch (err) {
                Logger.error(err.message);
            }
            producer.send([{
                topic: topic,
                messages: _message
            }], function (err: Error, data: any) {
                if (err) {
                    return reject(err);
                } else {
                    Logger.trace('[KAFKA]', 'Producer message sent', topic, message);
                    return resolve(data);
                }
            });
        });
    }
};

export const KafkaConsumerService: any = {
    addConsumerGroup: (consumerGroupOptions: IKafkaConsumerGroupOptions): void => {
        Logger.info('[KAFKA]', 'Connecting Kafka consumer group', consumerGroupOptions.name, consumerGroupOptions.kafkaHost, consumerGroupOptions.topics);

        const consumerGroup = new ConsumerGroup({
            kafkaHost: consumerGroupOptions.kafkaHost,
            groupId: consumerGroupOptions.groupId
        }, consumerGroupOptions.topics);

        const q = async.queue((message: IKafkaMessage, cb) => {
            if (!processCallbacks[message.event]) {
                cb();
            } else {
                const processCallback: (data: any, callback: (message: any) => void) => void = processCallbacks[message.event];
                processCallback(message.data, (message: any) => {
                    if (consumerGroupOptions.processCallback) {
                        consumerGroupOptions.processCallback(message, cb);
                    } else {
                        cb();
                    }
                });
            }
        }, QUEUE_CONCURRENCY);
        q.drain = () => {
            consumerGroup.resume();
        };

        /*consumerGroup.on('connect', () => {
            Logger.info('[KAFKA]', 'Kafka consumer group', consumerGroupOptions.name, 'connected');
        });*/
        consumerGroup.on('message', (message: any) => {
            try {
                const value: IKafkaMessage = JSON.parse(message.value);
                if (value) {
                    q.push(value, (err?: Error, result?: any) => {
                        if (err) {
                            Logger.error(err.message);
                        }
                    });
                    consumerGroup.pause();
                } else {
                    Logger.error('[KAFKA]', 'Empty kafka message');
                }
            } catch (err) {
                Logger.error(err.message);
            }
        });
        consumerGroup.on('error', function (err) {
            Logger.error('[KAFKA]', 'Kafka error', err.message);
        });
        consumerGroup.on('offsetOutOfRange', function (err) {
            Logger.error('[KAFKA]', 'Kafka offsetOutOfRange', err.message);
        });
        consumerGroups[consumerGroupOptions.name] = <IConsumerGroup>{
            consumer: consumerGroup,
            q: q
        };
    },
    addProcessCallback: (event: string, callback: (data: any, callback: (message: any) => void) => void) => {
        processCallbacks[event] = callback;
    },
    removeConsumerGroup: (name: string): Promise<void> => {
        return new Promise<void>((resolve, reject) => {
            if (consumerGroups[name]) {
                const consumerGroup = consumerGroups[name].consumer;
                const q = consumerGroups[name].q;
                consumerGroup.close((err?: Error) => {
                    delete consumerGroups[name];
                    if (err) {
                        return reject(err);
                    } else {
                        return resolve();
                    }
                });
            } else {
                return resolve();
            }
        });
    }
};

function getLogger() {
    const loggerName: string = 'kafka';
    const appenders: any = {};
    appenders[loggerName] = { type: 'stdout' };
    const LOG_LEVEL: string = process.env['WP_KAFKA_LOG_LEVEL'] ? <string>process.env['WP_KAFKA_LOG_LEVEL'] : 'info';
    log4js.configure({
        appenders: appenders,
        categories: { default: { appenders: [loggerName], level: LOG_LEVEL } }
    });
    return log4js.getLogger(loggerName);
}