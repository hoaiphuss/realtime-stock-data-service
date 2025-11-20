import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document, HydratedDocument } from 'mongoose';

export type DnseQuoteDocument = HydratedDocument<DnseQuote>;

@Schema({ timestamps: true, versionKey: false })
export class DnseQuote extends Document {
  @Prop() marketId?: string;
  @Prop() boardId?: string;
  @Prop() boardIdOriginal?: string;
  @Prop() productId?: string;
  @Prop() isin?: string;
  @Prop() symbol?: string;
  @Prop() symbolName?: string;
  @Prop() symbolEnglishName?: string;
  @Prop() tradingTime?: Date;
  @Prop() securityGroupId?: string;
  @Prop() productGrpId?: string;
  @Prop() referencePrice?: number;
  @Prop() highLimitPrice?: number;
  @Prop() lowLimitPrice?: number;
  @Prop() highestPrice?: number;
  @Prop() lowestPrice?: number;
  @Prop() openPrice?: number;
  @Prop() closePrice?: number;
  @Prop() averagePrice?: number;
  @Prop() buyForeignQuantity?: number;
  @Prop() sellForeignQuantity?: number;
  @Prop() buyForeignValue?: number;
  @Prop() sellForeignValue?: number;
  @Prop() foreignerBuyPossibleQuantity?: number;
  @Prop() foreignerOrderLimitQuantity?: number;
  @Prop() securityStatus?: string;
  @Prop() expectedTradePrice?: number;
  @Prop() expectedTradeQuantity?: number;
  @Prop() totalVolumeTraded?: number;
  @Prop() grossTradeAmount?: number;
  @Prop() sellTotalOrderQuantity?: number;
  @Prop() buyTotalOrderQuantity?: number;
  @Prop() matchPrice?: number;
  @Prop() matchQuantity?: number;
  @Prop() matchValue?: number;
  @Prop() changedValue?: number;
  @Prop() changedRatio?: number;
  @Prop() lastTradingDate?: string;
  @Prop() symbolType?: string;
  @Prop() openInterestQuantity?: number;
  @Prop() putOrCall?: string;
  @Prop() exerciseStyle?: string;
  @Prop() maturityDate?: string;
  @Prop() issuer?: string;
  @Prop() issueDate?: string;
  @Prop() contractMultiplier?: number;
  @Prop() listedShares?: number;
  @Prop() strikePrice?: number;
  @Prop() settlementMethod?: string;
  @Prop() firstTradingDate?: string;
  @Prop() finalSettlementDate?: string;
  @Prop() listingDate?: string;
  @Prop() couponRate?: number;
  @Prop() yield?: number;
  @Prop() symbolAdminStatusCode?: string;
  @Prop() symbolTradingMethodStatusCode?: string;
  @Prop() symbolTradingSantionStatusCode?: string;
  @Prop() tradingSessionId?: string;
}

export const DnseQuoteSchema = SchemaFactory.createForClass(DnseQuote);
