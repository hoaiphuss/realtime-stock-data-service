import { MailerModule } from '@nestjs-modules/mailer';
import { Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AlertService } from './alert.service';
import { MailerController } from './mailer.controller';

@Module({
  imports: [
    MailerModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        transport: {
          host: config.get<string>('EMAIL_HOST'),
          secure: false,
          auth: {
            user: config.get<string>('EMAIL_AUTH_USER'),
            pass: config.get<string>('EMAIL_AUTH_PASS'),
          },
        },
        defaults: {
          from: `"DNSE Alert" <${config.get('SMTP_FROM')}>`,
        },
      }),
    }),
  ],
  providers: [AlertService],
  exports: [AlertService],
  controllers: [MailerController],
})
export class AppMailerModule {}
