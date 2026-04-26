import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterLink } from '@angular/router';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { Competition, PredictionService } from '../../services/prediction.service';

@Component({
  selector: 'app-competition-list',
  standalone: true,
  imports: [CommonModule, RouterLink, MatCardModule, MatIconModule, MatProgressSpinnerModule],
  template: `
    <h2>Competitions</h2>

    @if (loading) {
      <div class="spinner"><mat-spinner diameter="40" /></div>
    } @else if (error) {
      <p class="error">{{ error }}</p>
    } @else {
      <div class="grid">
        @for (comp of competitions; track comp.id) {
          <mat-card class="comp-card" [routerLink]="['/competition', comp.id]">
            <mat-card-header>
              <mat-icon mat-card-avatar>{{ iconFor(comp.mode) }}</mat-icon>
              <mat-card-title>{{ comp.name }}</mat-card-title>
              <mat-card-subtitle>{{ comp.mode === 'national' ? 'National team' : 'Domestic league' }}</mat-card-subtitle>
            </mat-card-header>
            <mat-card-content>
              <div class="counts">
                <div class="count">
                  <span class="num">{{ comp.upcoming_count }}</span>
                  <span class="lbl">upcoming</span>
                </div>
                <div class="count">
                  <span class="num">{{ comp.recent_count }}</span>
                  <span class="lbl">recent</span>
                </div>
                <div class="count">
                  <span class="num">{{ comp.past_count }}</span>
                  <span class="lbl">holdout</span>
                </div>
              </div>
              <div class="hint">{{ comp.past_label }}</div>
            </mat-card-content>
          </mat-card>
        }
      </div>
    }
  `,
  styles: `
    h2 { margin: 0 0 16px; font-size: 1.3rem; }
    .spinner { display: flex; justify-content: center; padding: 40px; }
    .error { color: #c62828; text-align: center; padding: 20px; }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 16px;
    }
    .comp-card {
      cursor: pointer;
      transition: box-shadow 0.2s;
    }
    .comp-card:hover {
      box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .counts {
      display: flex;
      gap: 24px;
      margin: 12px 0;
    }
    .count { text-align: center; }
    .num { display: block; font-size: 1.5rem; font-weight: 700; color: var(--mat-sys-primary); }
    .lbl { display: block; font-size: 0.75rem; text-transform: uppercase; color: rgba(0,0,0,0.6); }
    .hint { font-size: 0.8rem; color: rgba(0,0,0,0.5); }
  `,
})
export class CompetitionListComponent implements OnInit {
  competitions: Competition[] = [];
  loading = true;
  error: string | null = null;

  constructor(private predictionService: PredictionService) {}

  ngOnInit(): void {
    this.predictionService.getCompetitions().subscribe({
      next: data => { this.competitions = data; this.loading = false; },
      error: () => { this.error = 'Failed to load competitions.'; this.loading = false; },
    });
  }

  iconFor(mode: 'national' | 'club'): string {
    return mode === 'national' ? 'public' : 'sports_soccer';
  }
}
