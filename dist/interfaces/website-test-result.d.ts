export interface IWebsiteTestResult {
    total: number;
    cache: number;
    keepalive: number;
    gzip: number;
    compress: number;
    progressive: number;
    cdn: number;
    size: number;
    docTime: number;
    loadTime: number;
}
