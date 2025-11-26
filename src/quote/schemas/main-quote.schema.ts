import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type MainQuoteDocument = HydratedDocument<MainQuote>;

@Schema({ timestamps: true, versionKey: false })
export class MainQuote {
  @Prop() StockCode?: string;
  @Prop() TradingDate?: Date;
  @Prop() KLCPLH?: number;
  @Prop() PriorClosePrice?: number;
  @Prop() CeilingPrice?: number;
  @Prop() FloorPrice?: number;
  @Prop() TotalVol?: number;
  @Prop() TotalVal?: number;
  @Prop() HighestPrice?: number;
  @Prop() LowestPrice?: number;
  @Prop() OpenPrice?: number;
  @Prop() LastPrice?: number;
  @Prop() AvrPrice?: number;
  @Prop() Change?: number;
  @Prop() ChangeRatio?: number;
  @Prop() ClosePrice?: number;
  @Prop() BasicPrice?: number;
  @Prop() KLCPNY?: number;
  @Prop() MarketCapital?: number;
  @Prop() TotalPutVol?: number;
  @Prop() TotalPutVal?: number;
  @Prop() Row?: number;
  @Prop() Rows?: number;
  @Prop() MarketID?: string;
}

export const MainQuoteSchema = SchemaFactory.createForClass(MainQuote);

MainQuoteSchema.index({ MarketID: 1, StockCode: 1 });
