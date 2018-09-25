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
export declare const KafkaProducerService: any;
export declare const KafkaConsumerService: any;
