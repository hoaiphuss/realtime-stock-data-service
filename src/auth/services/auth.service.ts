import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Auth } from '../schemas/auth.schema';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { extractTokenExpiry, isTokenValid } from '../helpers/auth.helper';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';

interface AuthResponse {
  token: string;
  investorId: string;
}

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);
  private tokenPromise: Promise<Auth> | null = null;

  constructor(
    private readonly configService: ConfigService,
    @InjectModel(Auth.name) private readonly authModel: Model<Auth>,
  ) {}

  async getValidToken(): Promise<Auth> {
    // Kiểm tra token trong DB
    const auth = await this.authModel.findOne();

    if (auth && isTokenValid(auth.token, auth.tokenExpiredAt))
      return {
        token: auth.token,
        investorId: auth.investorId,
        tokenExpiredAt: auth.tokenExpiredAt,
      } as Auth;

    // Nếu có promise đang chạy, chờ nó hoàn thành
    if (this.tokenPromise) return this.tokenPromise;

    // Tạo promise cho việc lấy và lưu token
    this.tokenPromise = this.authenticateAndSaveToken();

    try {
      const result = await this.tokenPromise;
      return result;
    } finally {
      // Reset Promise sau khi hoàn thành (thành công hoặc lỗi)
      this.tokenPromise = null;
    }
  }

  private async authenticateAndSaveToken(): Promise<Auth> {
    try {
      const { token, investorId } = await this.authenticate();
      await this.saveTokenToDB(token, investorId);
      return { token, investorId, tokenExpiredAt: extractTokenExpiry(token) };
    } catch (err) {
      this.logger.error(
        'Authentication failed in authenticationAndSaveToken method',
        err,
      );
      throw err;
    }
  }

  private async authenticate(): Promise<AuthResponse> {
    const authUrl = this.configService.get<string>('DNSE_AUTH_URL');
    const meUrl = this.configService.get<string>('DNSE_ME_URL');
    const username = this.configService.get<string>('DNSE_USER');
    const password = this.configService.get<string>('DNSE_PASSWORD');

    if (!authUrl || !meUrl || !username || !password)
      throw new NotFoundException('Missing Authentication Information in .env');

    try {
      // Gọi API lấy token
      const authRes = await axios.post(authUrl, { username, password });
      const authData = authRes?.data as AuthResponse;
      const token = authData?.token;

      if (!token)
        throw new NotFoundException('Token not returned from auth API');

      // Gọi API để lấy investorId
      const meRes = await axios.get(meUrl, {
        headers: { Authorization: `Bearer ${token}` },
      });

      const infoRes = meRes?.data as AuthResponse;
      const investorId = infoRes?.investorId;

      if (!investorId) throw new NotFoundException('Investor ID not found !');

      return { token, investorId };
    } catch (err) {
      this.logger.error('Authenticaiton failed !', err);
      throw new NotFoundException('Authentication failed !');
    }
  }

  private async saveTokenToDB(
    token: string,
    investorId: string,
  ): Promise<void> {
    await this.authModel.findOneAndUpdate(
      {},
      {
        token,
        investorId,
        tokenExpiredAt: extractTokenExpiry(token),
      },
      { upsert: true, new: true },
    );
  }
}
