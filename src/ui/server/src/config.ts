import dotenv from 'dotenv';

dotenv.config();

export const config = {
  port: parseInt(process.env.BFF_PORT || '3000', 10),
  fastapiUrl: process.env.FASTAPI_URL || 'http://localhost:8000',
  corsOrigins: process.env.CORS_ORIGINS?.split(',') || ['http://localhost:4200'],
};
