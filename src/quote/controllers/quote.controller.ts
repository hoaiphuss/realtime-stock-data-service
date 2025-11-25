import { Controller, Get, Query } from '@nestjs/common';
import { QuoteService } from '../services/quote.service';

@Controller()
export class QuoteController {
  constructor(private readonly quoteService: QuoteService) {}

  /**
   * GET /stocktradinginfo?Code=AAA
   */
  @Get('stocktradinginfo')
  async getStockTradingInfo(@Query('Code') code: string) {
    if (!code) {
      throw new Error('Code is required');
    }

    return this.quoteService.getByStockCode(code);
  }

  /**
   * GET /stocktradinginfobyMarket_ID?Page=1&PageSize=20&Market_ID=HOSE
   */
  @Get('stocktradinginfobyMarket_ID')
  async getByMarketID(
    @Query('Page') Page = 1,
    @Query('PageSize') PageSize = 20,
    @Query('Market_ID') Market_ID = 'HOSE',
  ) {
    return this.quoteService.getByMarketID({
      Page: Number(Page),
      PageSize: Number(PageSize),
      Market_ID,
    });
  }
}
