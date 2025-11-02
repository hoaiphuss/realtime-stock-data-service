import { Logger, NotFoundException } from '@nestjs/common';

const logger = new Logger('AuthHelper');

interface JwtPayload {
  exp: number;
  [key: string]: unknown;
}

export function isTokenValid(
  token: string,
  expiry: number,
  now = Date.now(),
): boolean {
  console.log('expiry: ', expiry);
  console.log('now: ', now);
  return !!token && expiry > now;
}

export function extractTokenExpiry(token: string): number {
  try {
    const [, payloadBase64] = token.split('.');
    const payload = JSON.parse(
      Buffer.from(payloadBase64, 'base64').toString(),
    ) as JwtPayload;

    if (!payload.exp) throw new Error('Token has no expiration claim !');

    return payload.exp * 1000;
  } catch (err) {
    logger.error('Invalid token structure', err);
    throw new NotFoundException('Failed to decode token expiration');
  }
}
