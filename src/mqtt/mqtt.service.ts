import {
  Injectable,
  Logger,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { connect, MqttClient } from 'mqtt';
import { AuthService } from 'src/auth/services/auth.service';
import { DnseQuote } from 'src/quote/schemas/dnse-quote.schema';
import { QuoteService } from 'src/quote/services/quote.service';
import { isTradingTime } from './utils/mqtt-session.util';
import { Cron } from '@nestjs/schedule';
import { buildMqttConnectOptions } from './utils/mqtt-connection.util';
import { registerMqttEvents } from './utils/mqtt-events.util';
import { AlertService } from 'src/mailer/alert.service';

@Injectable()
export class MqttService implements OnModuleInit, OnModuleDestroy {
  // MQTT client instance now
  private client: MqttClient | null = null;
  private readonly logger = new Logger(MqttService.name);

  private lastMessageTime = Date.now();
  private healthInterval: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelay = 5000;

  constructor(
    private readonly quoteService: QuoteService,
    private readonly authService: AuthService,
    private readonly configService: ConfigService,
    private readonly alertService: AlertService,
  ) {}

  async onModuleInit() {
    if (isTradingTime()) {
      await this.connectToBroker();
    }

    this.startHealthCheck();
  }

  onModuleDestroy() {
    this.stopHealthCheck();
    if (this.client) this.client.end(true);
  }

  /**
   * ======================================
   * ==========  OPEN SESSION  ============
   * ======================================
   */
  @Cron('0 0 9 * * 1-5') // 9:00 các ngày thứ 2 → 6
  @Cron('0 0 13 * * 1-5') // 13:00
  async cronStartSession() {
    this.logger.log('🔔 Trading session started → connecting MQTT...');
    await this.connectToBroker();
  }

  /**
   * ======================================
   * ==========  CLOSE SESSION ============
   * ======================================
   */
  @Cron('30 11 * * 1-5') // 11:30
  @Cron('30 15 * * 1-5') // 15:30
  cronEndSession() {
    this.logger.log('🔕 Trading session ended → disconnecting MQTT...');
    this.client?.end(true);
    this.client = null;
  }

  /**
   * Health Check
   */
  private startHealthCheck() {
    this.healthInterval = setInterval(
      () => {
        const diff = Date.now() - this.lastMessageTime;
        if (diff > 15 * 60 * 1000) {
          // 15 phút
          void this.alertService.sendError(
            'MQTT Health Check',
            `No MQTT messages for ${Math.round(diff / 1000 / 60)} minutes. Reconnecting...`,
          );
          void this.forceReconnect();
        }
      },
      5 * 60 * 1000,
    ); // check every 5 phút
  }

  private async forceReconnect() {
    this.client?.end(true);
    await this.connectToBroker();
  }

  private scheduleReconnect() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    const jitter = Math.floor(Math.random() * (this.reconnectDelay / 2));
    const delay = this.reconnectDelay + jitter;

    this.reconnectTimer = setTimeout(() => void this.connectToBroker(), delay);

    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 15 * 60 * 1000);
  }

  private stopHealthCheck() {
    if (this.healthInterval) {
      clearInterval(this.healthInterval);
      this.healthInterval = null;
    }
  }

  /** Connect Broker */
  /**
   * ========================
   *   CONNECT
   * ========================
   */
  private async connectToBroker() {
    if (!isTradingTime()) return;

    try {
      const { token, investorId } = await this.authService.getValidToken();
      const brokerUrl = this.configService.get<string>('BROKEN_URL');
      const topic = this.configService.get<string>('TOPIC');

      if (!brokerUrl) throw new NotFoundException('No broker URL provided');

      const clientId = `${this.configService.get('CLIENT_ID')}-${Math.random()
        .toString(36)
        .substring(2, 7)}`;

      const options = buildMqttConnectOptions({
        clientId,
        username: investorId,
        password: token,
      });

      this.client = connect(brokerUrl, options);

      registerMqttEvents<Partial<DnseQuote>>(
        this.client,
        topic!,
        this.logger,
        (json) => {
          void this.quoteService.saveQuoteIfChanged(json as Partial<DnseQuote>);
        },
        () => {
          this.scheduleReconnect();
        },
      );
    } catch (err) {
      await this.alertService.sendError('MQTT Connect Error', String(err));
      this.scheduleReconnect();
    }
  }
}
