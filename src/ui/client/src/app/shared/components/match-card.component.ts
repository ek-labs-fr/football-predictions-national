import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';
import { MatIconModule } from '@angular/material/icon';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatchResult } from '../../services/prediction.service';

@Component({
  selector: 'app-match-card',
  standalone: true,
  imports: [CommonModule, RouterLink, MatCardModule, MatChipsModule, MatIconModule, MatTooltipModule],
  template: `
    <mat-card class="match-card" [routerLink]="['/match', match.fixture_id]">
      <mat-card-header>
        <mat-card-subtitle>{{ match.date | date:'mediumDate' }} &middot; {{ match.league_name }} &middot; {{ match.round }}</mat-card-subtitle>
      </mat-card-header>

      <mat-card-content>
        <div class="teams-row">
          <div class="team home">
            <span class="team-name">{{ match.home_team_name }}</span>
          </div>
          <div class="scores">
            <div class="predicted">
              <span class="label">Predicted</span>
              <span class="score">{{ match.predicted_score }}</span>
            </div>
            @if (match.actual_score) {
              <div class="actual">
                <span class="label">Actual</span>
                <span class="score" [class.correct]="match.correct_score">{{ match.actual_score }}</span>
              </div>
            }
          </div>
          <div class="team away">
            <span class="team-name">{{ match.away_team_name }}</span>
          </div>
        </div>

        <div class="probs-bar">
          <div class="prob home-win" [style.width.%]="match.home_win_prob * 100"
               [matTooltip]="'Home: ' + (match.home_win_prob * 100 | number:'1.0-1') + '%'">
            {{ match.home_win_prob * 100 | number:'1.0-0' }}%
          </div>
          <div class="prob draw-prob" [style.width.%]="match.draw_prob * 100"
               [matTooltip]="'Draw: ' + (match.draw_prob * 100 | number:'1.0-1') + '%'">
            {{ match.draw_prob * 100 | number:'1.0-0' }}%
          </div>
          <div class="prob away-win" [style.width.%]="match.away_win_prob * 100"
               [matTooltip]="'Away: ' + (match.away_win_prob * 100 | number:'1.0-1') + '%'">
            {{ match.away_win_prob * 100 | number:'1.0-0' }}%
          </div>
        </div>

        @if (match.actual_outcome) {
          <div class="result-chip">
            @if (match.correct_outcome) {
              <mat-icon class="correct-icon">check_circle</mat-icon>
              <span class="correct-text">Correct outcome</span>
            } @else {
              <mat-icon class="wrong-icon">cancel</mat-icon>
              <span class="wrong-text">Wrong outcome</span>
            }
            @if (match.correct_score) {
              <mat-icon class="correct-icon">stars</mat-icon>
              <span class="correct-text">Exact score</span>
            }
          </div>
        }
      </mat-card-content>
    </mat-card>
  `,
  styles: `
    .match-card {
      cursor: pointer;
      margin-bottom: 12px;
      transition: box-shadow 0.2s;
    }
    .match-card:hover {
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .teams-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin: 8px 0;
    }
    .team {
      flex: 1;
      text-align: center;
    }
    .team-name {
      font-weight: 500;
      font-size: 0.95rem;
    }
    .scores {
      display: flex;
      gap: 16px;
      align-items: center;
    }
    .predicted, .actual {
      text-align: center;
    }
    .label {
      display: block;
      font-size: 0.7rem;
      text-transform: uppercase;
      color: rgba(0,0,0,0.5);
    }
    .score {
      font-size: 1.3rem;
      font-weight: 700;
    }
    .score.correct {
      color: #2e7d32;
    }
    .probs-bar {
      display: flex;
      height: 24px;
      border-radius: 4px;
      overflow: hidden;
      margin: 8px 0;
      font-size: 0.7rem;
      color: white;
      font-weight: 500;
    }
    .prob {
      display: flex;
      align-items: center;
      justify-content: center;
      min-width: 20px;
    }
    .home-win { background: #2e7d32; }
    .draw-prob { background: #757575; }
    .away-win { background: #c62828; }
    .result-chip {
      display: flex;
      align-items: center;
      gap: 4px;
      margin-top: 4px;
      font-size: 0.8rem;
    }
    .correct-icon { color: #2e7d32; font-size: 18px; width: 18px; height: 18px; }
    .wrong-icon { color: #c62828; font-size: 18px; width: 18px; height: 18px; }
    .correct-text { color: #2e7d32; }
    .wrong-text { color: #c62828; }

    @media (max-width: 599px) {
      .teams-row { flex-direction: column; gap: 4px; }
      .scores { gap: 12px; }
      .team-name { font-size: 0.85rem; }
      .score { font-size: 1.1rem; }
    }
  `,
})
export class MatchCardComponent {
  @Input({ required: true }) match!: MatchResult;
}
