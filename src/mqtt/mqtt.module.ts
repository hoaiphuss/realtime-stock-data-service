import { Module } from '@nestjs/common';
import { AuthModule } from 'src/auth/auth.module';
import { QuoteModule } from 'src/quote/quote.module';
import { AppMailerModule } from 'src/mailer/mailer.module';
import { MqttService } from './services/mqtt.service';
import { MqttAlertService } from './services/mqtt-alert.service';
import { MqttConnectionManager } from './services/mqtt-connection.service';
import { MqttHealthService } from './services/mqtt-health.service';

@Module({
  imports: [QuoteModule, AuthModule, AppMailerModule],
  providers: [
    MqttService,
    MqttAlertService,
    MqttConnectionManager,
    MqttHealthService,
  ],
})
export class MqttModule {}
