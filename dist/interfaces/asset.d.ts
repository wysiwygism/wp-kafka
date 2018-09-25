import { IAssetInclude } from "./asset-include";
export interface IAsset {
    userId: string;
    websiteId: string;
    cloneId: string;
    url: string;
    mimetype: string;
    encoding?: string;
    parseError?: boolean;
    parseErrorData?: any;
    isMobile?: boolean;
    is404?: boolean;
    isRemoved?: boolean;
    optimizationLevel?: number;
    data?: string;
    optimizedData?: string;
    md5Url?: string;
    crawlId?: string;
    include?: IAssetInclude;
    isOriginal: boolean;
    active: boolean;
    isNewAsset?: boolean;
    isDynamic?: boolean;
    createdAt?: Date;
    modifiedAt?: Date;
}
