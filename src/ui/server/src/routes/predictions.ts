import { Router } from 'express';
import axios from 'axios';
import { config } from '../config';

const router = Router();

router.post('/predict', async (req, res, next) => {
  try {
    const response = await axios.post(`${config.fastapiUrl}/predict`, req.body);
    res.json(response.data);
  } catch (err: any) {
    if (err.response) {
      res.status(err.response.status).json(err.response.data);
    } else {
      next(err);
    }
  }
});

router.get('/matches', async (_req, res, next) => {
  try {
    const response = await axios.get(`${config.fastapiUrl}/matches`);
    res.json(response.data);
  } catch (err: any) {
    if (err.response) {
      res.status(err.response.status).json(err.response.data);
    } else {
      next(err);
    }
  }
});

router.get('/matches/:id', async (req, res, next) => {
  try {
    const response = await axios.get(`${config.fastapiUrl}/matches/${req.params.id}`);
    res.json(response.data);
  } catch (err: any) {
    if (err.response) {
      res.status(err.response.status).json(err.response.data);
    } else {
      next(err);
    }
  }
});

export default router;
