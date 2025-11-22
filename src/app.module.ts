import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { MongooseModule } from '@nestjs/mongoose';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { AuthModule } from './auth/auth.module';
import { ScheduleModule } from '@nestjs/schedule';
import { QuoteModule } from './quote/quote.module';
import { MqttModule } from './mqtt/mqtt.module';
import { AppMailerModule } from './mailer/mailer.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => ({
        uri: configService.get<string>('MONGODB_URL'),
      }),
      inject: [ConfigService],
    }),
    AuthModule,
    ScheduleModule.forRoot(),
    QuoteModule,
    MqttModule,
    AppMailerModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
