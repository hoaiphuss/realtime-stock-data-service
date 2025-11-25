import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DnseQuote, DnseQuoteSchema } from './schemas/dnse-quote.schema';
import { MainQuote, MainQuoteSchema } from './schemas/main-quote.schema';
import { QuoteService } from './services/quote.service';
import { QuoteDnseCacheService } from './services/quote-cache.service';
import { QuoteRepository } from './repositories/quote.repository';
import { QuoteController } from './controllers/quote.controller';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: DnseQuote.name, schema: DnseQuoteSchema },
      { name: MainQuote.name, schema: MainQuoteSchema },
    ]),
  ],
  providers: [QuoteService, QuoteDnseCacheService, QuoteRepository],
  exports: [QuoteService, QuoteRepository],
  controllers: [QuoteController],
})
export class QuoteModule {}
