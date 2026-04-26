import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export type Outcome = 'home_win' | 'draw' | 'away_win';

export interface MatchPrediction {
  fixture_id: number;
  date: string;
  round: string;
  home_team_name: string;
  away_team_name: string;
  predicted_score: string;
  lambda_home: number;
  lambda_away: number;
  p_home_win: number;
  p_draw: number;
  p_away_win: number;
  predicted_outcome: Outcome;
  prediction_made_at?: string;
  actual_home_goals?: number;
  actual_away_goals?: number;
  actual_score?: string;
  actual_outcome?: Outcome;
  correct_outcome?: boolean;
  correct_score?: boolean;
}

export interface PerformanceSummary {
  total_matches: number;
  correct_outcomes: number;
  correct_scores: number;
  outcome_accuracy: number;
  score_accuracy: number;
  mae_avg: number;
}

export interface UpcomingResponse {
  competition_id: string;
  competition_name: string;
  matches: MatchPrediction[];
}

export interface RecentResponse {
  competition_id: string;
  competition_name: string;
  window_days: number;
  matches: MatchPrediction[];
  performance: PerformanceSummary;
}

export interface PastResponse {
  competition_id: string;
  competition_name: string;
  label: string;
  matches: MatchPrediction[];
  performance: PerformanceSummary;
}

export interface Competition {
  id: string;
  name: string;
  mode: 'national' | 'club';
  past_label: string;
  recent_window_days: number;
  upcoming_count: number;
  recent_count: number;
  past_count: number;
}

@Injectable({ providedIn: 'root' })
export class PredictionService {
  private readonly dataUrl = environment.dataUrl;

  constructor(private http: HttpClient) {}

  getCompetitions(): Observable<Competition[]> {
    return this.http.get<Competition[]>(`${this.dataUrl}/competitions.json`);
  }

  getUpcoming(competitionId: string): Observable<UpcomingResponse> {
    return this.http.get<UpcomingResponse>(`${this.dataUrl}/upcoming_${competitionId}.json`);
  }

  getRecent(competitionId: string): Observable<RecentResponse> {
    return this.http.get<RecentResponse>(`${this.dataUrl}/recent_${competitionId}.json`);
  }

  getPast(competitionId: string): Observable<PastResponse> {
    return this.http.get<PastResponse>(`${this.dataUrl}/past_${competitionId}.json`);
  }
}
