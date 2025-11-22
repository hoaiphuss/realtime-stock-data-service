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
  private client: MqttClient | null = null;
  private readonly logger = new Logger(MqttService.name);

  private lastAlertSent: Record<string, number> = {};
  private readonly alertCooldown = 5 * 60 * 1000; // 10 phút

  // health & reconnect
  private lastMessageTime = Date.now();
  private healthInterval: NodeJS.Timeout | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private reconnectDelay = 5000; // ms
  private readonly reconnectMax = 15 * 60 * 1000; // 15 minutes

  // connection guard to avoid concurrent connects
  private isConnecting = false;

  constructor(
    private readonly quoteService: QuoteService,
    private readonly authService: AuthService,
    private readonly configService: ConfigService,
    private readonly alertService: AlertService,
  ) {}

  async onModuleInit() {
    // If server starts during trading hours, connect immediately
    if (isTradingTime()) {
      await this.connectToBroker();
    }

    // start health check even if not trading: it will be no-op outside trading hours
    this.startHealthCheck();
  }

  onModuleDestroy() {
    this.stopHealthCheck();
    if (this.client) {
      try {
        this.client.end(true);
      } catch (e) {
        this.logger.warn('Error while ending MQTT client on destroy', e);
      }
    }
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  /**
   * CRON: start sessions at 09:00 and 13:00 (Mon-Fri)
   * Cron format: minute hour day month weekday
   */
  @Cron('0 9 * * 1-5') // 09:00 Mon-Fri
  @Cron('0 13 * * 1-5') // 13:00 Mon-Fri
  async cronStartSession() {
    this.logger.log('🔔 Trading session started → connecting MQTT...');
    await this.connectToBroker();
  }

  /**
   * CRON: end sessions at 11:30 and 15:30 (Mon-Fri)
   */
  @Cron('30 11 * * 1-5') // 11:30 Mon-Fri
  @Cron('30 15 * * 1-5') // 15:30 Mon-Fri
  cronEndSession() {
    this.logger.log('🔕 Trading session ended → disconnecting MQTT...');
    // stop reconnect attempts, reset delay
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.reconnectDelay = 5000;

    // end client and null it
    if (this.client) {
      try {
        this.client.end(true);
      } catch (e) {
        this.logger.warn(
          'Error while disconnecting MQTT client at session end',
          e,
        );
      }
    }
    this.client = null;

    // stop health check while out of session
    this.stopHealthCheck();
  }

  /**
   * Health check: ensure we receive messages in reasonable time during session.
   * - runs every 5 minutes, but only acts if we're in trading time.
   * - when health fails, sends alert and triggers a reconnect (fire-and-forget).
   */
  private startHealthCheck() {
    if (this.healthInterval) return;

    this.healthInterval = setInterval(
      () => {
        // Only perform health actions during trading time
        if (!isTradingTime()) return;

        const diff = Date.now() - this.lastMessageTime;
        // threshold: 15 minutes without a message
        if (diff > 15 * 60 * 1000) {
          // fire-and-forget but log errors inside service if they occur
          void this.safeSendAlert(
            'MQTT Health Check',
            `No MQTT messages for ${Math.round(diff / 60000)} minutes. Forcing reconnect.`,
          ).catch((err) =>
            this.logger.error('AlertService sendError failed', err),
          );

          // force reconnect (async) - intentionally fire-and-forget here
          void this.forceReconnect().catch((err) =>
            this.logger.error('forceReconnect failed', err),
          );
        }
      },
      5 * 60 * 1000,
    ); // every 5 minutes
  }

  private stopHealthCheck() {
    if (this.healthInterval) {
      clearInterval(this.healthInterval);
      this.healthInterval = null;
    }
  }

  private async forceReconnect(): Promise<void> {
    // quick defensive cleanup
    if (this.client) {
      try {
        this.client.end(true);
      } catch (e) {
        this.logger.warn('Error ending MQTT client during forceReconnect', e);
      }
    }
    this.client = null;

    // small delay before trying to reconnect to let resources free
    await new Promise((r) => setTimeout(r, 200));
    await this.connectToBroker();
  }

  /**
   * scheduleReconnect uses exponential backoff + jitter.
   * - Will NOT schedule reconnect outside trading hours.
   * - Marks reconnectTimer and increases reconnectDelay.
   */
  private scheduleReconnect() {
    // don't schedule reconnect outside trading hours
    if (!isTradingTime()) {
      this.logger.log('Skipping reconnect: outside trading hours.');
      return;
    }

    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

    const jitter = Math.floor(Math.random() * (this.reconnectDelay / 2));
    const delay = this.reconnectDelay + jitter;

    this.logger.log(`🔄 Scheduling reconnect in ~${Math.round(delay / 1000)}s`);

    this.reconnectTimer = setTimeout(() => {
      // call connectToBroker but explicitly ignore promise here (ESLint)
      void this.connectToBroker();
    }, delay);

    this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.reconnectMax);
  }

  /**
   * Connect to broker with guards:
   * - avoid concurrent connect attempts (isConnecting)
   * - reset reconnectDelay on success
   * - register events and ensure lastMessageTime updates on message
   */
  private async connectToBroker(): Promise<void> {
    // ensure only try to connect in trading hours
    if (!isTradingTime()) {
      this.logger.log('Outside trading hours — skipping connect');
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
      // be careful: expected env key is BROKER_URL
      const brokerUrl = this.configService.get<string>('BROKER_URL');
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

      // create client
      this.client = connect(brokerUrl, options);

      // register events: provide onMessage that updates lastMessageTime
      registerMqttEvents<Partial<DnseQuote>>(
        this.client,
        topic ?? '',
        this.logger,
        (json) => {
          // update lastMessageTime whenever we get a valid message
          this.lastMessageTime = Date.now();

          // fire-and-forget the save (it returns Promise)
          void this.quoteService.saveQuoteIfChanged(json as Partial<DnseQuote>);
        },
        // reconnect callback -> schedule backoff reconnect
        () => {
          this.scheduleReconnect();
        },
      );

      // connected (note: 'connect' event fires asynchronously; still reset backoff here)
      this.logger.log('MQTT client instantiated, waiting for connect event...');
      // reset reconnect delay to initial so next failures start small
      this.reconnectDelay = 5000;
    } catch (rawErr) {
      // ensure we pass useful error info to alert
      const errMessage =
        rawErr instanceof Error
          ? (rawErr.stack ?? rawErr.message)
          : String(rawErr);

      // send alert but don't block flow if mail fails
      void this.safeSendAlert(
        'MQTT Connection Error',
        `Failed to connect to MQTT broker:\n\n${errMessage}.`,
      );

      // schedule reconnect with backoff (if in trading hours)
      this.scheduleReconnect();

      this.logger.error('Error during connectToBroker', rawErr);
    } finally {
      this.isConnecting = false;
    }
  }

  private async safeSendAlert(type: string, message: string) {
    const now = Date.now();
    const lastSent = this.lastAlertSent[type] ?? 0;
    const addMessage = `Alert "${type}" skipped (cooldown). Will send again after 5 minutes if issue persists.`;

    if (now - lastSent < this.alertCooldown) {
      this.logger.warn(addMessage);
      return;
    }

    this.lastAlertSent[type] = now;

    try {
      await this.alertService.sendError(type, message + `\n\n${addMessage}`);
    } catch (err) {
      this.logger.error(`Failed to send alert: ${type}`, err);
    }
  }
}
