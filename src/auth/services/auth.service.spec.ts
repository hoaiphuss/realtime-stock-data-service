import { Test, TestingModule } from '@nestjs/testing';
import { AuthService } from './auth.service';
import { getModelToken } from '@nestjs/mongoose';
import { Auth } from '../schemas/auth.schema';
import { ConfigService } from '@nestjs/config';
import axios from 'axios';
import { NotFoundException } from '@nestjs/common';
import * as authHelper from '../helpers/auth.helper';

jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('AuthService', () => {
  let service: AuthService;
  let authModel: any;
  let configService: any;

  beforeEach(async () => {
    authModel = {
      findOne: jest.fn(),
      findOneAndUpdate: jest.fn(),
    };
    configService = {
      get: jest.fn((key: string) => {
        const env = {
          DNSE_AUTH_URL: 'http://auth',
          DNSE_ME_URL: 'http://me',
          DNSE_USER: 'user',
          DNSE_PASSWORD: 'pass',
        };
        return env[key];
      }),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AuthService,
        { provide: getModelToken(Auth.name), useValue: authModel },
        { provide: ConfigService, useValue: configService },
      ],
    }).compile();

    service = module.get<AuthService>(AuthService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  // =========================
  // Token hợp lệ trong DB
  // =========================
  it('should return token from DB if valid', async () => {
    const fakeAuth = {
      token: 'valid-token',
      investorId: '123',
      tokenExpiredAt: Date.now() + 1000,
    };
    jest.spyOn(authHelper, 'isTokenValid').mockReturnValue(true);
    authModel.findOne.mockResolvedValue(fakeAuth);

    const result = await service.getValidToken();

    expect(result).toEqual(fakeAuth);
    expect(authModel.findOne).toHaveBeenCalled();
  });

  // =========================
  // Token hết hạn → lấy mới
  // =========================
  it('should call API to get new token if expired', async () => {
    const expiredAuth = {
      token: 'expired',
      investorId: '123',
      tokenExpiredAt: Date.now() - 1000,
    };
    authModel.findOne.mockResolvedValue(expiredAuth);

    jest.spyOn(authHelper, 'isTokenValid').mockReturnValue(false);
    jest
      .spyOn(authHelper, 'extractTokenExpiry')
      .mockReturnValue(Date.now() + 3600 * 1000);

    mockedAxios.post.mockResolvedValue({
      data: { token: 'new-token', investorId: '123' },
    });
    mockedAxios.get.mockResolvedValue({ data: { investorId: '123' } });

    const result = await service.getValidToken();

    expect(result.token).toBe('new-token');
    expect(authModel.findOneAndUpdate).toHaveBeenCalledWith(
      {},
      expect.objectContaining({ token: 'new-token', investorId: '123' }),
      { upsert: true, new: true },
    );
  });

  // =========================
  // Lỗi thiếu biến môi trường
  // =========================
  it('should throw NotFoundException if env missing', async () => {
    configService.get.mockReturnValue(undefined);

    await expect(service['authenticate']()).rejects.toThrow(NotFoundException);
  });

  // =========================
  // Lỗi API trả về không có token
  // =========================
  it('should throw NotFoundException if auth API returns no token', async () => {
    mockedAxios.post.mockResolvedValue({ data: {} });

    await expect(service['authenticate']()).rejects.toThrow(NotFoundException);
  });

  // =========================
  // Lỗi API trả về không có investorId
  // =========================
  it('should throw NotFoundException if me API returns no investorId', async () => {
    mockedAxios.post.mockResolvedValue({ data: { token: 'abc' } });
    mockedAxios.get.mockResolvedValue({ data: {} });

    await expect(service['authenticate']()).rejects.toThrow(NotFoundException);
  });

  // =========================
  // tokenPromise tránh race condition
  // =========================
  it('should reuse tokenPromise to avoid multiple requests', async () => {
    authModel.findOne.mockResolvedValue(null);
    jest
      .spyOn(authHelper, 'extractTokenExpiry')
      .mockReturnValue(Date.now() + 3600 * 1000);

    mockedAxios.post.mockResolvedValue({
      data: { token: 'token1', investorId: 'inv1' },
    });
    mockedAxios.get.mockResolvedValue({ data: { investorId: 'inv1' } });

    // Gọi đồng thời 2 request
    const [res1, res2] = await Promise.all([
      service.getValidToken(),
      service.getValidToken(),
    ]);

    expect(res1.token).toBe('token1');
    expect(res2.token).toBe('token1');
    expect(mockedAxios.post).toHaveBeenCalledTimes(1);
  });

  // =========================
  // Lưu token vào DB
  // =========================
  it('should save token to DB', async () => {
    jest
      .spyOn(authHelper, 'extractTokenExpiry')
      .mockReturnValue(Date.now() + 3600 * 1000);
    await service['saveTokenToDB']('tok', 'inv');
    expect(authModel.findOneAndUpdate).toHaveBeenCalledWith(
      {},
      expect.objectContaining({ token: 'tok', investorId: 'inv' }),
      { upsert: true, new: true },
    );
  });
});
