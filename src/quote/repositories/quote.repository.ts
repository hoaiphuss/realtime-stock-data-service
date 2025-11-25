import { Model } from 'mongoose';
import { MainQuote } from '../schemas/main-quote.schema';
import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';

@Injectable()
export class QuoteRepository {
  constructor(
    @InjectModel(MainQuote.name) private readonly model: Model<MainQuote>,
  ) {}

  async getByStockCode(code: string) {
    return this.model.findOne({ StockCode: code }).lean();
  }

  async getByMarketID(query: {
    Page: number;
    PageSize: number;
    Market_ID: string;
  }) {
    const { Page, PageSize, Market_ID } = query;

    const skip = (Page - 1) * PageSize;

    const [items, total] = await Promise.all([
      this.model
        .find({ MarketID: Market_ID })
        .sort({ StockCode: 1 })
        .skip(skip)
        .limit(PageSize)
        .lean(),
      this.model.countDocuments({ MarketID: Market_ID }),
    ]);

    return {
      Page,
      PageSize,
      Market_ID,
      TotalItems: total,
      TotalPages: Math.ceil(total / PageSize),
      Data: items,
    };
  }
}
