export interface IPaymentRequestCharge {
    paymentRequestId: string;
    response: string;
    isError: boolean;
    providerId?: string;
    createdAt?: Date;
}
