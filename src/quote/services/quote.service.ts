import { fieldMap } from '../map/quote.map';
import { DnseQuote } from '../schemas/dnse-quote.schema';
import { MainQuote } from '../schemas/main-quote.schema';

export class QuoteService {
  mapQuoteToInternalFormat(quote: DnseQuote): MainQuote {
    const result: MainQuote = {};

    for (const [mainKey, dnseKey] of Object.entries(fieldMap)) {
      if (!dnseKey) {
        result[mainKey] = null;
        continue;
      }

      const value = quote[dnseKey] as string;

      if (mainKey === 'TradingDate' && value) {
        const vnDate = new Date(value);
        vnDate.setHours(vnDate.getHours() + 7);
        result[mainKey] = vnDate;
      }

      result[mainKey] = value;
    }
    return result;
  }
}
