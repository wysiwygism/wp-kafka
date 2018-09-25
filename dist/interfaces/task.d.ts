export interface ITask {
    userId: string;
    name: string;
    startDate: Date;
    endDate: Date;
    duration: number;
    progress: number;
    state: string;
    stateText: string;
}
