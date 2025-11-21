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
      // If no mapping key, skip
      if (!dnseKey) {
        result[mainKey] = null;
        continue;
      }

      const value = quote[dnseKey] as string;

      // If no value, set null
      if (value === undefined || value === null) {
        result[mainKey] = null;
        continue;
      }

      // Special handling for specific fields
      if (mainKey === 'TradingDate') {
        const vnDate = new Date(value);
        vnDate.setHours(vnDate.getHours() + 7);
        result[mainKey] = vnDate;
        continue;
      }

      // Sepecial handling for MarketID field
      if (mainKey === 'MarketID') {
        result[mainKey] = marketMap[value] ?? value;
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
    const mainQuote = this.mapQuoteToInternalFormat(data);
    const StockCode = mainQuote.StockCode;
    if (!StockCode) throw new Error('StockCode missing in mainQuote mapping');

    // Kiểm tra DnseQuote thay đổi so với cache
    const isDnseChanged =
      !cached ||
      cached.matchPrice !== data.matchPrice ||
      cached.totalVolumeTraded !== data.totalVolumeTraded ||
      cached.matchQuantity !== data.matchQuantity ||
      cached.changedValue !== data.changedValue ||
      cached.changedRatio !== data.changedRatio;

    // Quyết định có cần lưu hay không
    const shouldSaveDnse = isDnseChanged;
    let shouldSaveMain = isDnseChanged;

    // Nếu DnseQuote chưa thay đổi, check MainQuote tồn tại không
    if (!isDnseChanged) {
      const oldMain = await this.mainQuoteModel.findOne({ StockCode }).lean();

      if (!oldMain) {
        shouldSaveMain = true; // Main chưa tồn tại → upsert
      } else {
        // So sánh field MainQuote với mainQuote mới
        const importantMainFields = [
          'LastPrice',
          'TotalVol',
          'Change',
          'ChangeRatio',
        ];

        for (const key of importantMainFields) {
          if (mainQuote[key] !== oldMain[key]) {
            console.log(
              `MainQuote field ${key} changed: ${oldMain[key]} -> ${mainQuote[key]}`,
            );
            shouldSaveMain = true;
            break;
          }
        }
      }
    }

    if (!shouldSaveDnse && !shouldSaveMain) return;

    const ops: Promise<any>[] = [];
    if (shouldSaveDnse) {
      ops.push(
        this.dnseQuoteModel.updateOne(
          { symbol },
          { $set: data },
          { upsert: true },
        ),
      );
    }

    // Upsert MainQuote nếu cần
    if (shouldSaveMain) {
      ops.push(
        this.mainQuoteModel.updateOne(
          { StockCode },
          { $set: mainQuote },
          { upsert: true },
        ),
      );
    }

    await Promise.all(ops);

    // Cập nhật lại cache DnseQuote
    this.quoteCacheService.set(symbol, data);

    this.logger.debug(
      `Saved quote for ${symbol} (Dnse changed: ${isDnseChanged}, MainQuote changed: ${shouldSaveMain})`,
    );
  }
}
