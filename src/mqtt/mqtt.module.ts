import { Module } from '@nestjs/common';
import { MqttService } from './mqtt.service';
import { AuthModule } from 'src/auth/auth.module';
import { QuoteModule } from 'src/quote/quote.module';
import { AppMailerModule } from 'src/mailer/mailer.module';

@Module({
  imports: [QuoteModule, AuthModule, AppMailerModule],
  providers: [MqttService],
})
export class MqttModule {}
