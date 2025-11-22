import { Injectable, Logger } from '@nestjs/common';
import { isTradingTime } from '../utils/mqtt-session.util';
import { MqttAlertService } from './mqtt-alert.service';
import { MqttConnectionManager } from './mqtt-connection.service';
import { ALERT_TIME_GAP } from '../enums/alert-time-gap.enum';

@Injectable()
export class MqttHealthService {
  private readonly logger = new Logger(MqttHealthService.name);

  private interval: NodeJS.Timeout | null = null;
  private lastMessageTime = Date.now();

  constructor(
    private readonly alertService: MqttAlertService,
    private readonly conn: MqttConnectionManager,
  ) {}

  markMessageReceived() {
    this.lastMessageTime = Date.now();
  }

  start() {
    if (this.interval) return;

    this.interval = setInterval(
      () => {
        if (!isTradingTime()) return;

        const diff = Date.now() - this.lastMessageTime;
        if (diff > 15 * 60 * 1000) {
          void this.alertService
            .send(
              'MQTT Health',
              `No messages for ${diff / 60000} min`,
              ALERT_TIME_GAP.TEN_MINUTE,
            )
            .catch((err) => this.logger.error(err));

          void this.conn
            .connect()
            .catch((err) => this.logger.error('Reconnect failed', err));
        }
      },
      5 * 60 * 1000,
    );
  }

  stop() {
    if (this.interval) clearInterval(this.interval);
    this.interval = null;
  }
}
