import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { DnseQuote, DnseQuoteSchema } from './schemas/dnse-quote.schema';
import { MainQuote, MainQuoteSchema } from './schemas/main-quote.schema';
import { QuoteService } from './services/quote.service';

@Module({
  imports: [
    MongooseModule.forFeature([
      { name: DnseQuote.name, schema: DnseQuoteSchema },
      { name: MainQuote.name, schema: MainQuoteSchema },
    ]),
  ],
  providers: [QuoteService],
  exports: [QuoteService],
})
export class QuoteModule {}
