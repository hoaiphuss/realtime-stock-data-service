import { Controller, Get } from '@nestjs/common';
import { AuthService } from '../services/auth.service';
import { Auth } from '../schemas/auth.schema';

@Controller()
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Get('auth')
  getToken(): Promise<Auth> {
    return this.authService.getValidToken();
  }
}
