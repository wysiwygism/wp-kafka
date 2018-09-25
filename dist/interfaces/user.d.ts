export interface IUser {
    firstName?: string;
    lastName?: string;
    email: string;
    pass?: string;
    phone?: string;
    company?: string;
    totalWebsitesLimit?: number;
    usedFreeTrial?: boolean;
    active: boolean;
    verificationToken?: string;
    verificationTokenExp?: Date;
    billingToken?: string;
    billingTokenExpDate?: string;
    isBillingComplete?: boolean;
    accountBalance?: number;
    countryCode?: string;
    createdAt?: Date;
    modifiedAt?: Date;
}
