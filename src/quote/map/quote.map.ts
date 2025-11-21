import { MARKET } from '../enums/market-code.enum';
import { DnseQuote } from '../schemas/dnse-quote.schema';
import { MainQuote } from '../schemas/main-quote.schema';

export const fieldMap: Record<keyof MainQuote, keyof DnseQuote | null> = {
  StockCode: 'symbol',
  TradingDate: 'tradingTime',
  KLCPLH: 'listedShares',
  PriorClosePrice: 'referencePrice',
  CeilingPrice: 'highLimitPrice',
  FloorPrice: 'lowLimitPrice',
  TotalVol: 'totalVolumeTraded',
  TotalVal: 'grossTradeAmount',
  HighestPrice: 'highestPrice',
  LowestPrice: 'lowestPrice',
  OpenPrice: 'openPrice',
  LastPrice: 'matchPrice',
  AvrPrice: 'averagePrice',
  Change: 'changedValue',
  ChangeRatio: 'changedRatio',
  ClosePrice: 'matchPrice',
  BasicPrice: 'referencePrice',
  MarketID: 'marketId',

  // những key MainQuote không khớp
  KLCPNY: null,
  MarketCapital: null,
  TotalPutVol: null,
  TotalPutVal: null,
  Row: null,
  Rows: null,
};

export const marketMap: Record<string, MARKET> = {
  MARKET_ID_STO: MARKET.MARKET_ID_STO,
  MARKET_ID_STX: MARKET.MARKET_ID_STX,
  MARKET_ID_UPX: MARKET.MARKET_ID_UPX,
};
