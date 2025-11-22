import { Injectable, Logger } from '@nestjs/common';
import { AlertService } from 'src/mailer/alert.service';

@Injectable()
export class MqttAlertService {
  private readonly logger = new Logger(MqttAlertService.name);

  private lastAlertSent: Record<string, number> = {};

  constructor(private readonly alertService: AlertService) {}

  async send(type: string, message: string, mins: number) {
    const alertCooldown = mins * 60 * 1000;
    const now = Date.now();
    const lastSent = this.lastAlertSent[type] ?? 0;

    const addMessage = `Alert "${type}" skipped (cooldown). Will send again after 5 minutes if issue persists.`;

    if (now - lastSent < alertCooldown) {
      this.logger.warn(addMessage);
      return;
    }

    this.lastAlertSent[type] = now;

    try {
      await this.alertService.sendError(type, message + `\n\n${addMessage}`);
    } catch (error) {
      this.logger.error(`Failed to send alert: ${type}`, error);
    }
  }
}
