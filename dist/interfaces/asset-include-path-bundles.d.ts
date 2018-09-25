import { IAssetReference } from "./asset-reference";
export interface IAssetIncludePathBundles {
    assets: Array<IAssetReference>;
    md5: string;
    position: string;
    path: string;
}
