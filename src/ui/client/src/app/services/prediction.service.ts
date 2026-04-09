import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface ScorelineProbability {
  home_goals: number;
  away_goals: number;
  probability: number;
}

export interface Prediction {
  home_team_id: number;
  away_team_id: number;
  lambda_home: number;
  lambda_away: number;
  most_likely_score: string;
  home_win: number;
  draw: number;
  away_win: number;
  top_scorelines: ScorelineProbability[];
}

export interface MatchResult {
  fixture_id: number;
  date: string;
  home_team_id: number;
  home_team_name: string;
  away_team_id: number;
  away_team_name: string;
  predicted_home_goals: number;
  predicted_away_goals: number;
  predicted_score: string;
  actual_home_goals: number | null;
  actual_away_goals: number | null;
  actual_score: string | null;
  predicted_outcome: string;
  actual_outcome: string | null;
  correct_outcome: boolean | null;
  correct_score: boolean | null;
  home_win_prob: number;
  draw_prob: number;
  away_win_prob: number;
  league_name: string;
  round: string;
}

export interface PerformanceSummary {
  total_matches: number;
  completed_matches: number;
  correct_outcomes: number;
  correct_scores: number;
  outcome_accuracy: number;
  score_accuracy: number;
  avg_mae: number;
}

export interface MatchListResponse {
  matches: MatchResult[];
  performance: PerformanceSummary;
}

@Injectable({ providedIn: 'root' })
export class PredictionService {
  private readonly apiUrl = environment.apiUrl;

  constructor(private http: HttpClient) {}

  predict(homeTeamId: number, awayTeamId: number): Observable<Prediction> {
    return this.http.post<Prediction>(`${this.apiUrl}/predict`, {
      home_team_id: homeTeamId,
      away_team_id: awayTeamId,
    });
  }

  getMatches(): Observable<MatchListResponse> {
    return this.http.get<MatchListResponse>(`${this.apiUrl}/matches`);
  }

  getMatch(fixtureId: number): Observable<MatchResult> {
    return this.http.get<MatchResult>(`${this.apiUrl}/matches/${fixtureId}`);
  }
}
