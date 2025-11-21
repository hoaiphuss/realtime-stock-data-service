import {
  Injectable,
  Logger,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { connect, MqttClient } from 'mqtt';
import { AuthService } from 'src/auth/services/auth.service';
import { DnseQuote } from 'src/quote/schemas/dnse-quote.schema';
import { QuoteService } from 'src/quote/services/quote.service';

@Injectable()
export class MqttService implements OnModuleInit {
  // MQTT client instance now
  private client: MqttClient | null = null;
  private readonly logger = new Logger(MqttService.name);
  private lastMessageTime = Date.now();

  constructor(
    private readonly quoteService: QuoteService,
    private readonly authService: AuthService,
    private readonly configService: ConfigService,
  ) {}

  async onModuleInit() {
    await this.connectToBroker();
  }

  /** Connect Broker */
  private async connectToBroker() {
    try {
      const { token, investorId } = await this.authService.getValidToken();
      const brokerUrl = this.configService.get<string>('BROKEN_URL');
      if (!brokerUrl) throw new NotFoundException('No broker URL provided');

      const clientId = `${this.configService.get<string>('CLIENT_ID')}-${Math.floor(
        Math.random() * 10000,
      )}`;

      this.client = connect(brokerUrl, {
        clientId,
        username: investorId,
        password: token,
        rejectUnauthorized: false,
        protocol: 'wss',
        reconnectPeriod: 0,
      });

      this.registerEvents();
    } catch (err) {
      this.logger.error('Error connecting to MQTT broker:', err);
    }
  }

  /** Handle MQTT events */
  private registerEvents() {
    if (!this.client) return;

    this.client.on('connect', () => {
      console.log('MQTT connected');
      this.client!.subscribe(this.configService.get<string>('TOPIC') || '');
    });

    this.client.on('close', () => {
      console.warn('MQTT connection closed');
    });

    this.client.on('offline', () => {
      console.warn('MQTT offline');
    });

    this.client.on('error', (err) => {
      console.error('MQTT Error:', err.message);
      this.client?.end(true);
    });

    this.client.on('message', (_, message) => {
      this.lastMessageTime = Date.now();
      try {
        const raw = JSON.parse(message.toString()) as Partial<DnseQuote>;
        void this.quoteService.saveQuoteIfChanged(raw);
      } catch (err) {
        console.error('Error processing message:', err);
      }
    });
  }
}
