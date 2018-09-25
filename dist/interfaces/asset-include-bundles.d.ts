import { IAssetIncludePathBundles } from "./asset-include-path-bundles";
export interface IAssetIncludeBundles {
    bundles?: Array<string>;
    pathBundles: Array<IAssetIncludePathBundles>;
}
