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

  constructor(
    private readonly authService: AuthService,
    private readonly configService: ConfigService,
    private readonly alertService: MqttAlertService,
    private readonly quoteService: QuoteService,
    private readonly mqttAllertService: MqttAlertService,
  ) {}

  async connect() {
    // ensure only try to connect in trading hours
    if (!isTradingTime()) {
      await this.mqttAllertService.send(
        'MQTT Connect Skipped',
        'Outside trading hours — skipping connect',
        ALERT_TIME_GAP.TEN_MINUTE,
      );
      return;
    }

    // prevent concurrent connects
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

      // register events: provide onMessage that updates lastMessageTime
      registerMqttEvents(
        this.client,
        topic ?? '',
        this.logger,
        (json) =>
          // fire-and-forget the save (it returns Promise)
          void this.quoteService.saveQuoteIfChanged(json as Partial<DnseQuote>),
        // reconnect callback -> schedule backoff reconnect
        () => this.scheduleReconnect(),
      );

      // reset reconnect delay to initial so next failures start small
      this.reconnectDelay = 5000;
      this.logger.log('🚀 MQTT connected');
    } catch (err) {
      // ensure we pass useful error info to alert
      const errMessage =
        err instanceof Error ? (err.stack ?? err.message) : String(err);

      // send alert but don't block flow if mail fails
      await this.alertService.send(
        'MQTT Connection Error',
        `Failed to connect to MQTT broker:\n\n${errMessage}.`,
        ALERT_TIME_GAP.TEN_MINUTE,
      );

      // schedule reconnect with backoff (if in trading hours)
      this.scheduleReconnect();
    } finally {
      this.isConnecting = false;
    }
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
    if (this.client) this.client.end(true);

    this.client = null;
    this.reconnectDelay = 5000;
  }
}
