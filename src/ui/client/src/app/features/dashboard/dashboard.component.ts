import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatchCardComponent } from '../../shared/components/match-card.component';
import { PerformanceSummaryComponent } from '../../shared/components/performance-summary.component';
import {
  MatchResult,
  PerformanceSummary,
  PredictionService,
} from '../../services/prediction.service';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [CommonModule, MatProgressSpinnerModule, MatchCardComponent, PerformanceSummaryComponent],
  template: `
    <h2>Match Predictions</h2>

    @if (loading) {
      <div class="spinner-container">
        <mat-spinner diameter="40" />
      </div>
    } @else if (error) {
      <p class="error">{{ error }}</p>
    } @else {
      @if (performance) {
        <app-performance-summary [perf]="performance" />
      }

      <div class="match-list">
        @for (match of matches; track match.fixture_id) {
          <app-match-card [match]="match" />
        } @empty {
          <p>No matches available yet. Run the data pipeline to load predictions.</p>
        }
      </div>
    }
  `,
  styles: `
    h2 {
      margin: 0 0 16px;
      font-size: 1.3rem;
    }
    .spinner-container {
      display: flex;
      justify-content: center;
      padding: 40px;
    }
    .error {
      color: #c62828;
      text-align: center;
      padding: 20px;
    }
    .match-list {
      display: flex;
      flex-direction: column;
    }
  `,
})
export class DashboardComponent implements OnInit {
  matches: MatchResult[] = [];
  performance: PerformanceSummary | null = null;
  loading = true;
  error: string | null = null;

  constructor(private predictionService: PredictionService) {}

  ngOnInit(): void {
    this.predictionService.getMatches().subscribe({
      next: data => {
        this.matches = data.matches;
        this.performance = data.performance;
        this.loading = false;
      },
      error: err => {
        this.error = 'Failed to load matches. Is the API running?';
        this.loading = false;
      },
    });
  }
}
