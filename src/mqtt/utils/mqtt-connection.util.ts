import { IClientOptions } from 'mqtt';

export function buildMqttConnectOptions({
  clientId,
  username,
  password,
}: {
  clientId: string;
  username: string;
  password: string;
}): IClientOptions {
  return {
    clientId,
    username,
    password,
    protocol: 'wss',
    rejectUnauthorized: false,
    reconnectPeriod: 0, // tự kiểm soát reconnect
  };
}
