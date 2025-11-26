import { Test, TestingModule } from '@nestjs/testing';
import { QuoteService } from './quote.service';
import { QuoteDnseCacheService } from './quote-cache.service';
import { QuoteRepository } from '../repositories/quote.repository';
import { getModelToken } from '@nestjs/mongoose';
import { DnseQuote } from '../schemas/dnse-quote.schema';
import { MainQuote } from '../schemas/main-quote.schema';
import { Model } from 'mongoose';

describe('QuoteService Unit Tests', () => {
  let service: QuoteService;
  let cacheService: QuoteDnseCacheService;
  let dnseModel: Model<DnseQuote>;
  let mainModel: Model<MainQuote>;

  const mockDnseModel = {
    updateOne: jest.fn().mockResolvedValue({}),
  };
  const mockMainModel = {
    findOne: jest.fn().mockResolvedValue(null),
    updateOne: jest.fn().mockResolvedValue({}),
  };
  const mockRepo = {
    getByStockCode: jest.fn(),
    getByMarketID: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        QuoteService,
        QuoteDnseCacheService,
        { provide: QuoteRepository, useValue: mockRepo },
        { provide: getModelToken(DnseQuote.name), useValue: mockDnseModel },
        { provide: getModelToken(MainQuote.name), useValue: mockMainModel },
      ],
    }).compile();

    service = module.get<QuoteService>(QuoteService);
    cacheService = module.get<QuoteDnseCacheService>(QuoteDnseCacheService);
    dnseModel = module.get<Model<DnseQuote>>(getModelToken(DnseQuote.name));
    mainModel = module.get<Model<MainQuote>>(getModelToken(MainQuote.name));
  });

  afterEach(() => {
    jest.clearAllMocks();
    cacheService['latestCache'].clear();
  });

  // ============================
  // 1. Kiểm tra schema validation
  // ============================
  it('should throw error if required field StockCode is missing', async () => {
    const invalidPayload: Partial<DnseQuote> = {
      symbol: 'AAA', // giả sử StockCode mapping sẽ bị lỗi
    };
    // mapQuoteToInternalFormat sẽ trả mainQuote thiếu StockCode
    const mapSpy = jest
      .spyOn(service, 'mapQuoteToInternalFormat')
      .mockReturnValue({} as any);

    await expect(service.saveQuoteIfChanged(invalidPayload)).rejects.toThrow(
      'StockCode missing in mainQuote mapping',
    );

    mapSpy.mockRestore();
  });

  // ============================
  // 2. Xử lý dữ liệu đúng định dạng
  // ============================
  it('should save quote correctly if payload is valid', async () => {
    const payload: Partial<DnseQuote> = {
      symbol: 'AAA',
      matchPrice: 100,
      totalVolumeTraded: 5000,
      matchQuantity: 50,
      changedValue: 2,
      changedRatio: 0.02,
    };

    await service.saveQuoteIfChanged(payload);

    expect(dnseModel.updateOne).toHaveBeenCalled();
    expect(mainModel.updateOne).toHaveBeenCalled();
    const cached = cacheService.get('AAA');
    expect(cached).toEqual(payload);
  });

  // ============================
  // 3. Xử lý dữ liệu sai định dạng
  // ============================
  it('should catch error if payload is invalid', async () => {
    const invalidPayload: any = {
      // thiếu symbol
      matchPrice: 'abc', // sai kiểu
    };

    await expect(service.saveQuoteIfChanged(invalidPayload)).rejects.toThrow(
      'Symbol is required to save quote',
    );
  });

  // ============================
  // 4. Đo độ trễ xử lý
  // ============================
  it('should process quote within 5 seconds', async () => {
    const payload: Partial<DnseQuote> = {
      symbol: 'BBB',
      matchPrice: 200,
      totalVolumeTraded: 1000,
      matchQuantity: 20,
      changedValue: 1,
      changedRatio: 0.01,
    };

    const start = Date.now();
    await service.saveQuoteIfChanged(payload);
    const end = Date.now();

    const elapsed = end - start;
    console.log(`Processing time: ${elapsed} ms`);
    expect(elapsed).toBeLessThanOrEqual(5000);
  });

  // ============================
  // 5. Kiểm tra tính toàn vẹn dữ liệu
  // ============================
  it('should maintain data integrity when saving', async () => {
    const payload: Partial<DnseQuote> = {
      symbol: 'CCC',
      matchPrice: 150,
      totalVolumeTraded: 3000,
      matchQuantity: 30,
      changedValue: 3,
      changedRatio: 0.03,
    };

    await service.saveQuoteIfChanged(payload);

    const cached = cacheService.get('CCC');
    for (const key in payload) {
      expect(cached![key]).toEqual(payload[key]);
    }
  });
});
