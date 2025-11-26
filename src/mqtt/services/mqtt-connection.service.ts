import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { connect, MqttClient } from 'mqtt';
import { AuthService } from 'src/auth/services/auth.service';
import { buildMqttConnectOptions } from '../utils/mqtt-connection.util';
import { registerMqttEvents } from '../utils/mqtt-events.util';
import { isTradingTime } from '../utils/mqtt-session.util';
import { MqttAlertService } from './mqtt-alert.service';
import { QuoteService } from 'src/quote/services/quote.service';
import { ALERT_TIME_GAP } from '../enums/alert-time-gap.enum';
import { DnseQuote } from 'src/quote/schemas/dnse-quote.schema';

@Injectable()
export class MqttConnectionManager {
  private readonly logger = new Logger(MqttConnectionManager.name);

  public client: MqttClient | null = null;
  private isConnecting = false;

  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelay = 5000;
  private readonly reconnectMax = 15 * 60 * 1000;

  // ---- Watchdog: detect no data ----
  private lastMessageTime = Date.now();
  private watchdogTimer: NodeJS.Timeout | null = null;
  private readonly MESSAGE_TIMEOUT = 5 * 1000; // 30s không có message sẽ cảnh báo

  constructor(
    private readonly authService: AuthService,
    private readonly configService: ConfigService,
    private readonly alertService: MqttAlertService,
    private readonly quoteService: QuoteService,
    private readonly mqttAllertService: MqttAlertService,
  ) {}

  async connect() {
    if (!isTradingTime()) {
      await this.mqttAllertService.send(
        'MQTT Connect Skipped',
        'Outside trading hours — skipping connect',
        ALERT_TIME_GAP.TEN_MINUTE,
      );
      return;
    }

    if (this.isConnecting) {
      this.logger.debug('Connect already in progress, skipping duplicate call');
      return;
    }

    this.isConnecting = true;

    try {
      const { token, investorId } = await this.authService.getValidToken();
      const brokerUrl = this.configService.get<string>('BROKER_URL');
      const topic = this.configService.get<string>('TOPIC');

      if (!brokerUrl) {
        await this.alertService.send(
          'Missing Broker Url',
          'Missing Broker Url',
          ALERT_TIME_GAP.TEN_MINUTE,
        );
        return;
      }

      const clientId = `${this.configService.get('CLIENT_ID')}-${Math.random()
        .toString(36)
        .substring(2, 7)}`;

      const options = buildMqttConnectOptions({
        clientId,
        username: investorId,
        password: token,
      });

      this.client = connect(brokerUrl, options);

      // register events with message timestamp update
      registerMqttEvents(
        this.client,
        topic ?? '',
        this.logger,
        (json) => {
          this.lastMessageTime = Date.now(); // update watchdog
          void this.quoteService.saveQuoteIfChanged(json as Partial<DnseQuote>);
        },
        () => this.scheduleReconnect(),
      );

      // start watchdog timer
      this.startWatchdog();

      this.reconnectDelay = 5000;
      this.logger.log('MQTT connected');
    } catch (err) {
      const errMessage =
        err instanceof Error ? (err.stack ?? err.message) : String(err);

      await this.alertService.send(
        'MQTT Connection Error',
        `Failed to connect to MQTT broker:\n\n${errMessage}.`,
        ALERT_TIME_GAP.TEN_MINUTE,
      );

      this.scheduleReconnect();
    } finally {
      this.isConnecting = false;
    }
  }

  // ---- WATCHDOG: detect missing data ----
  private startWatchdog() {
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);

    this.watchdogTimer = setInterval(async () => {
      if (!isTradingTime()) return;

      const now = Date.now();
      const gap = now - this.lastMessageTime;

      if (gap > this.MESSAGE_TIMEOUT) {
        this.logger.warn(`No MQTT data for ${gap / 1000}s`);

        await this.alertService.send(
          'MQTT No Data Warning',
          `No MQTT data received for ${Math.floor(gap / 1000)} seconds.
The broker may have stopped sending data or the TOPIC might have changed.`,
          ALERT_TIME_GAP.FIVE_MINUTE,
        );

        this.scheduleReconnect(); // reconnect ngay khi mất data
      }
    }, 10_000); // check mỗi 10s
  }

  scheduleReconnect() {
    if (!isTradingTime()) return;

    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    const jitter = Math.random() * (this.reconnectDelay / 2);
    const delay = this.reconnectDelay + jitter;

    this.logger.warn(`🔄 Reconnect scheduled in ${delay / 1000}s`);
    this.reconnectTimer = setTimeout(() => void this.connect(), delay);

    this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.reconnectMax);
  }

  end() {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    if (this.client) this.client.end(true);

    this.client = null;
    this.reconnectDelay = 5000;
  }
}
