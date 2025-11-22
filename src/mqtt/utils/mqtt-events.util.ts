import { MqttClient } from 'mqtt';
import { Logger } from '@nestjs/common';
import { isTradingTime } from './mqtt-session.util';

export function registerMqttEvents<T>(
  client: MqttClient,
  topic: string,
  logger: Logger,
  onMessage: (json: any) => void,
  reconnect: () => void,
) {
  client.on('connect', () => {
    logger.log('MQTT connected');
    client.subscribe(topic);
  });

  client.on('close', () => {
    logger.warn('MQTT closed');

    if (!isTradingTime()) return;

    reconnect();
  });

  client.on('offline', () => {
    logger.warn('MQTT offline');
  });

  client.on('error', (err) => {
    logger.error(`MQTT Error: ${err.message}`);
    client.end(true);

    if (isTradingTime()) reconnect();
  });

  client.on('message', (_, message) => {
    try {
      const parsed: unknown = JSON.parse(message.toString());

      // ép kiểu ra T
      onMessage(parsed as T);
    } catch (e) {
      logger.error('Error parsing MQTT message', e);
    }
  });
}
