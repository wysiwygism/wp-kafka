export interface IUserLicense {
    userId: string;
    licenseId: string;
    subscriptionId: string;
    expDate: Date;
    autoRenew: boolean;
    status: string;
    statusDate: Date;
}
