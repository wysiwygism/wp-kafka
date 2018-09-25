import { IWebsiteTestResult } from "./website-test-result";
export interface IWebsiteTestResults {
    origin: IWebsiteTestResult;
    originPsi: IWebsiteTestResult;
    originMobilePsi: IWebsiteTestResult;
    _interface: IWebsiteTestResult;
    _interfacePsi: IWebsiteTestResult;
    _interfaceMobilePsi: IWebsiteTestResult;
}
