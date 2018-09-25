import { IPaymentRequestHistory } from "./payment-request-history";
import { IPaymentRequestItem } from "./payment-request-item";
export interface IPaymentRequest {
    userId: string;
    billingPeriod: string;
    amount: number;
    currencyCode: string;
    status: string;
    txnId?: string;
    history?: IPaymentRequestHistory[];
    items: IPaymentRequestItem[];
}
