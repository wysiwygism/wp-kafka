import { IPlan } from "./plan";
export interface IUserPlan {
    plan: IPlan;
    expDate: Date;
    subscriptionId?: string;
    autoRenew: boolean;
}
