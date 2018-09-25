import { IAssetIncludeBundles } from "./asset-include-bundles";
export interface IAssetInclude {
    js?: IAssetIncludeBundles;
    css?: IAssetIncludeBundles;
    images?: IAssetIncludeBundles;
}
