import { Module } from '@nestjs/common';
import { MqttService } from './mqtt.service';
import { AuthModule } from 'src/auth/auth.module';
import { QuoteModule } from 'src/quote/quote.module';

@Module({
  imports: [QuoteModule, AuthModule],
  providers: [MqttService],
})
export class MqttModule {}
