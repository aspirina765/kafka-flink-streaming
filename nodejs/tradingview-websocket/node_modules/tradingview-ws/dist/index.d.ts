declare type Subscriber = (event: TradingviewEvent) => void;
declare type Unsubscriber = () => void;
export interface Candle {
    timestamp: number;
    high: number;
    low: number;
    open: number;
    close: number;
    volume: number;
}
interface TradingviewConnection {
    subscribe: (handler: Subscriber) => Unsubscriber;
    send: (name: string, params: any[]) => void;
    close: () => Promise<void>;
}
interface ConnectionOptions {
    sessionId?: string;
}
interface TradingviewEvent {
    name: string;
    params: any[];
}
declare type TradingviewTimeframe = number | '1D' | '1W' | '1M';
export declare function connect(options?: ConnectionOptions): Promise<TradingviewConnection>;
interface GetCandlesParams {
    connection: TradingviewConnection;
    symbols: string[];
    amount?: number;
    timeframe?: TradingviewTimeframe;
}
export declare function getCandles({ connection, symbols, amount, timeframe }: GetCandlesParams): Promise<Candle[][]>;
export {};
//# sourceMappingURL=index.d.ts.map