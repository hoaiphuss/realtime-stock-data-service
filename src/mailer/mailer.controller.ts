import { Controller, Get, Query } from '@nestjs/common';
import { AlertService } from './alert.service';

@Controller('mailer')
export class MailerController {
  constructor(private readonly alertService: AlertService) {}

  /**
   * GET /alert/test?subject=Hello&message=Test123
   */
  @Get('test')
  async testAlert(
    @Query('subject') subject: string = 'Test Alert',
    @Query('message') message: string = 'This is a test alert message',
  ) {
    await this.alertService.sendError(subject, message);
    return {
      success: true,
      message: 'Alert email sent (if mailer configured correctly)',
    };
  }
}
