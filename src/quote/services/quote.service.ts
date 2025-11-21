import { Logger } from '@nestjs/common';
import { fieldMap, marketMap } from '../map/quote.map';
import { DnseQuote } from '../schemas/dnse-quote.schema';
import { MainQuote } from '../schemas/main-quote.schema';
import { QuoteDnseCacheService } from './quote-cache.service';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';

export class QuoteService {
  private readonly logger = new Logger(QuoteService.name);

  constructor(
    @InjectModel(DnseQuote.name)
    private readonly dnseQuoteModel: Model<DnseQuote>,
    @InjectModel(MainQuote.name)
    private readonly mainQuoteModel: Model<MainQuote>,
    private readonly quoteCacheService: QuoteDnseCacheService,
  ) {}

  mapQuoteToInternalFormat(quote: Partial<DnseQuote>): Partial<MainQuote> {
    const result: MainQuote = {};

    for (const [mainKey, dnseKey] of Object.entries(fieldMap)) {
      // Nếu field không ánh xạ -> null
      if (!dnseKey) {
        result[mainKey] = null;
        continue;
      }

      const value = quote[dnseKey] as string;

      // Nếu không có dữ liệu → bỏ qua
      if (value === undefined || value === null) {
        result[mainKey] = null;
        continue;
      }

      if (mainKey === 'TradingDate') {
        const vnDate = new Date(value);
        vnDate.setHours(vnDate.getHours() + 7);
        result[mainKey] = vnDate;
        continue;
      }

      if (mainKey === 'MarketID') {
        const marketEnum = marketMap[value] ?? value;
        result[mainKey] = marketEnum;
        continue;
      }

      const num = Number(value);
      result[mainKey] = isNaN(num) ? value : num;
    }

    return result;
  }

  async saveQuoteIfChanged(data: Partial<DnseQuote>): Promise<void> {
    const symbol = data.symbol;
    if (!symbol) throw new Error('Symbol is required to save quote');

    const cached = this.quoteCacheService.get(symbol);

    const isChanged =
      !cached ||
      cached.matchPrice !== data.matchPrice ||
      cached.totalVolumeTraded !== data.totalVolumeTraded ||
      cached.matchQuantity !== data.matchQuantity ||
      cached.changedValue !== data.changedValue ||
      cached.changedRatio !== data.changedRatio;

    if (!isChanged) return;

    const mainQuote = this.mapQuoteToInternalFormat(data);

    const StockCode = mainQuote.StockCode;
    if (!StockCode) throw new Error('StockCode missing in mainQuote mapping');

    await Promise.all([
      this.dnseQuoteModel.updateOne(
        { symbol },
        { $set: data },
        { upsert: true },
      ),
      this.mainQuoteModel.updateOne(
        { StockCode },
        { $set: mainQuote },
        { upsert: true },
      ),
    ]);
  }
}
