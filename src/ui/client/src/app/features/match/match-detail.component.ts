import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ActivatedRoute } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { Location } from '@angular/common';
import { MatchResult, PredictionService } from '../../services/prediction.service';

@Component({
  selector: 'app-match-detail',
  standalone: true,
  imports: [CommonModule, MatCardModule, MatProgressSpinnerModule, MatButtonModule, MatIconModule],
  template: `
    <button mat-button (click)="goBack()">
      <mat-icon>arrow_back</mat-icon> Back
    </button>

    @if (loading) {
      <div class="spinner-container">
        <mat-spinner diameter="40" />
      </div>
    } @else if (error) {
      <p class="error">{{ error }}</p>
    } @else if (match) {
      <mat-card class="detail-card">
        <mat-card-header>
          <mat-card-title>{{ match.home_team_name }} vs {{ match.away_team_name }}</mat-card-title>
          <mat-card-subtitle>{{ match.date | date:'fullDate' }} &middot; {{ match.league_name }} &middot; {{ match.round }}</mat-card-subtitle>
        </mat-card-header>
        <mat-card-content>
          <div class="scores-section">
            <div class="score-block">
              <span class="score-label">Predicted</span>
              <span class="score-value">{{ match.predicted_score }}</span>
            </div>
            @if (match.actual_score) {
              <div class="score-block">
                <span class="score-label">Actual</span>
                <span class="score-value" [class.correct]="match.correct_score">{{ match.actual_score }}</span>
              </div>
            }
          </div>

          <h3>Outcome Probabilities</h3>
          <div class="prob-bars">
            <div class="prob-row">
              <span class="prob-label">{{ match.home_team_name }}</span>
              <div class="prob-bar-bg">
                <div class="prob-bar home" [style.width.%]="match.home_win_prob * 100"></div>
              </div>
              <span class="prob-pct">{{ match.home_win_prob * 100 | number:'1.1-1' }}%</span>
            </div>
            <div class="prob-row">
              <span class="prob-label">Draw</span>
              <div class="prob-bar-bg">
                <div class="prob-bar draw" [style.width.%]="match.draw_prob * 100"></div>
              </div>
              <span class="prob-pct">{{ match.draw_prob * 100 | number:'1.1-1' }}%</span>
            </div>
            <div class="prob-row">
              <span class="prob-label">{{ match.away_team_name }}</span>
              <div class="prob-bar-bg">
                <div class="prob-bar away" [style.width.%]="match.away_win_prob * 100"></div>
              </div>
              <span class="prob-pct">{{ match.away_win_prob * 100 | number:'1.1-1' }}%</span>
            </div>
          </div>

          <h3>Expected Goals</h3>
          <div class="lambda-row">
            <div class="lambda">
              <span class="lambda-team">{{ match.home_team_name }}</span>
              <span class="lambda-val">{{ match.predicted_home_goals | number:'1.2-2' }}</span>
            </div>
            <div class="lambda">
              <span class="lambda-team">{{ match.away_team_name }}</span>
              <span class="lambda-val">{{ match.predicted_away_goals | number:'1.2-2' }}</span>
            </div>
          </div>
        </mat-card-content>
      </mat-card>
    }
  `,
  styles: `
    .spinner-container { display: flex; justify-content: center; padding: 40px; }
    .error { color: #c62828; text-align: center; }
    .detail-card { margin-top: 12px; }

    .scores-section {
      display: flex;
      gap: 32px;
      justify-content: center;
      margin: 16px 0;
    }
    .score-block { text-align: center; }
    .score-label {
      display: block;
      font-size: 0.75rem;
      text-transform: uppercase;
      color: rgba(0,0,0,0.5);
    }
    .score-value {
      font-size: 2rem;
      font-weight: 700;
    }
    .score-value.correct { color: #2e7d32; }

    h3 {
      font-size: 1rem;
      margin: 20px 0 8px;
      color: rgba(0,0,0,0.7);
    }

    .prob-bars { display: flex; flex-direction: column; gap: 8px; }
    .prob-row {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .prob-label {
      min-width: 100px;
      font-size: 0.85rem;
      text-align: right;
    }
    .prob-bar-bg {
      flex: 1;
      height: 20px;
      background: #e0e0e0;
      border-radius: 4px;
      overflow: hidden;
    }
    .prob-bar { height: 100%; border-radius: 4px; }
    .prob-bar.home { background: #2e7d32; }
    .prob-bar.draw { background: #757575; }
    .prob-bar.away { background: #c62828; }
    .prob-pct { min-width: 45px; font-size: 0.85rem; font-weight: 500; }

    .lambda-row {
      display: flex;
      gap: 32px;
      justify-content: center;
      margin: 12px 0;
    }
    .lambda { text-align: center; }
    .lambda-team { display: block; font-size: 0.8rem; color: rgba(0,0,0,0.6); }
    .lambda-val { font-size: 1.5rem; font-weight: 700; }

    @media (max-width: 599px) {
      .prob-label { min-width: 70px; font-size: 0.75rem; }
      .scores-section { gap: 20px; }
      .score-value { font-size: 1.5rem; }
      .lambda-row { gap: 20px; }
    }
  `,
})
export class MatchDetailComponent implements OnInit {
  match: MatchResult | null = null;
  loading = true;
  error: string | null = null;

  constructor(
    private route: ActivatedRoute,
    private predictionService: PredictionService,
    private location: Location,
  ) {}

  ngOnInit(): void {
    const id = Number(this.route.snapshot.paramMap.get('id'));
    this.predictionService.getMatch(id).subscribe({
      next: data => {
        this.match = data;
        this.loading = false;
      },
      error: () => {
        this.error = 'Match not found';
        this.loading = false;
      },
    });
  }

  goBack(): void {
    this.location.back();
  }
}
