import { Router } from 'express';
import axios from 'axios';
import { config } from '../config';

const router = Router();

router.get('/health', async (_req, res) => {
  try {
    const response = await axios.get(`${config.fastapiUrl}/health`);
    res.json({ bff: 'ok', api: response.data });
  } catch {
    res.json({ bff: 'ok', api: { status: 'unreachable' } });
  }
});

export default router;
