import { Injectable, Logger } from '@nestjs/common';
import { MailerService } from '@nestjs-modules/mailer';

@Injectable()
export class AlertService {
  private readonly logger = new Logger(AlertService.name);

  constructor(private readonly mailerService: MailerService) {}

  async sendError(subject: string, message: string) {
    try {
      await this.mailerService.sendMail({
        to: process.env.ADMIN_EMAILS?.split(',') || [],
        subject: `[DNSE Alert] ${subject}`,
        text: message,
      });
      this.logger.log(`Sent alert email: ${subject}`);
    } catch (err) {
      this.logger.error('Failed to send alert email', err);
    }
  }
}
