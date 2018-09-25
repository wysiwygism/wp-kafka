import { IPaymentRequestActiveDurationDate } from "./payment-request-active-duration-date";
export interface IPaymentRequestItem {
    websiteId: string;
    websiteHost: string;
    websiteActivePeriodDuration: number;
    websiteActivePeriodDurationDates: IPaymentRequestActiveDurationDate[];
}
