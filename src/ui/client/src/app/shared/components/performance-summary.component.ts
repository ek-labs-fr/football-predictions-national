import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { PerformanceSummary } from '../../services/prediction.service';

@Component({
  selector: 'app-performance-summary',
  standalone: true,
  imports: [CommonModule, MatCardModule, MatIconModule],
  template: `
    <mat-card class="perf-card">
      <mat-card-header>
        <mat-card-title>Algorithm Performance</mat-card-title>
        <mat-card-subtitle>{{ perf.completed_matches }} / {{ perf.total_matches }} matches completed</mat-card-subtitle>
      </mat-card-header>
      <mat-card-content>
        <div class="stats-grid">
          <div class="stat">
            <div class="stat-value">{{ perf.outcome_accuracy * 100 | number:'1.1-1' }}%</div>
            <div class="stat-label">Outcome Accuracy</div>
            <div class="stat-detail">{{ perf.correct_outcomes }} / {{ perf.completed_matches }}</div>
          </div>
          <div class="stat">
            <div class="stat-value">{{ perf.score_accuracy * 100 | number:'1.1-1' }}%</div>
            <div class="stat-label">Exact Score</div>
            <div class="stat-detail">{{ perf.correct_scores }} / {{ perf.completed_matches }}</div>
          </div>
          <div class="stat">
            <div class="stat-value">{{ perf.avg_mae | number:'1.2-2' }}</div>
            <div class="stat-label">Avg MAE</div>
            <div class="stat-detail">goals per team</div>
          </div>
        </div>
      </mat-card-content>
    </mat-card>
  `,
  styles: `
    .perf-card {
      margin-bottom: 16px;
    }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
      margin-top: 12px;
    }
    .stat {
      text-align: center;
    }
    .stat-value {
      font-size: 1.5rem;
      font-weight: 700;
      color: var(--mat-sys-primary);
    }
    .stat-label {
      font-size: 0.8rem;
      text-transform: uppercase;
      color: rgba(0,0,0,0.6);
      margin-top: 2px;
    }
    .stat-detail {
      font-size: 0.75rem;
      color: rgba(0,0,0,0.4);
    }

    @media (max-width: 599px) {
      .stats-grid {
        grid-template-columns: 1fr;
        gap: 12px;
      }
      .stat {
        display: flex;
        align-items: center;
        gap: 12px;
        text-align: left;
      }
      .stat-value {
        font-size: 1.3rem;
        min-width: 60px;
      }
    }
  `,
})
export class PerformanceSummaryComponent {
  @Input({ required: true }) perf!: PerformanceSummary;
}
