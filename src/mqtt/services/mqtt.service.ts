import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { Cron } from '@nestjs/schedule';
import { MqttConnectionManager } from './mqtt-connection.service';
import { MqttHealthService } from './mqtt-health.service';
import { isTradingTime } from '../utils/mqtt-session.util';

@Injectable()
export class MqttService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MqttService.name);

  constructor(
    private readonly conn: MqttConnectionManager,
    private readonly health: MqttHealthService,
  ) {}

  async onModuleInit() {
    if (isTradingTime()) await this.conn.connect();
    this.health.start();
  }

  onModuleDestroy() {
    this.health.stop();
    this.conn.end();
  }

  @Cron('0 9 * * 1-5')
  @Cron('0 13 * * 1-5')
  async startSession() {
    await this.conn.connect();
  }

  @Cron('30 11 * * 1-5')
  @Cron('30 15 * * 1-5')
  endSession() {
    this.conn.end();
    this.health.stop();
  }
}
