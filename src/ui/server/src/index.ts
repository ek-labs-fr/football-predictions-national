import express from 'express';
import cors from 'cors';
import morgan from 'morgan';
import path from 'path';
import { config } from './config';
import { errorHandler } from './middleware/error-handler';
import healthRoutes from './routes/health';
import predictionRoutes from './routes/predictions';
import teamRoutes from './routes/teams';

const app = express();

// Middleware
app.use(cors({ origin: config.corsOrigins }));
app.use(morgan('short'));
app.use(express.json());

// API routes (proxied to FastAPI)
app.use('/api', healthRoutes);
app.use('/api', predictionRoutes);
app.use('/api', teamRoutes);

// Serve Angular static files in production
const clientDist = path.join(__dirname, '../../client/dist/client/browser');
app.use(express.static(clientDist));
app.get('*', (_req, res) => {
  res.sendFile(path.join(clientDist, 'index.html'));
});

// Error handler
app.use(errorHandler);

app.listen(config.port, () => {
  console.log(`BFF running on http://localhost:${config.port}`);
  console.log(`Proxying API to ${config.fastapiUrl}`);
});

export default app;
