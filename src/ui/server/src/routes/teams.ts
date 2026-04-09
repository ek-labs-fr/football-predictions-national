import { Router } from 'express';
import axios from 'axios';
import { config } from '../config';

const router = Router();

router.get('/teams', async (_req, res, next) => {
  try {
    const response = await axios.get(`${config.fastapiUrl}/teams`);
    res.json(response.data);
  } catch (err: any) {
    if (err.response) {
      res.status(err.response.status).json(err.response.data);
    } else {
      next(err);
    }
  }
});

router.get('/teams/:id', async (req, res, next) => {
  try {
    const response = await axios.get(`${config.fastapiUrl}/teams/${req.params.id}`);
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
