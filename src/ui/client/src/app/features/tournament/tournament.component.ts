import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';
import { MatIconModule } from '@angular/material/icon';

@Component({
  selector: 'app-tournament',
  standalone: true,
  imports: [CommonModule, MatCardModule, MatIconModule],
  template: `
    <h2>Tournament Simulation</h2>
    <mat-card>
      <mat-card-content class="placeholder">
        <mat-icon class="placeholder-icon">emoji_events</mat-icon>
        <p>Tournament simulation will be available once the World Cup 2026 groups are drawn and models are trained.</p>
        <p class="detail">This page will show group stage advancement probabilities and knockout bracket progression odds from Monte Carlo simulation.</p>
      </mat-card-content>
    </mat-card>
  `,
  styles: `
    h2 { margin: 0 0 16px; font-size: 1.3rem; }
    .placeholder {
      text-align: center;
      padding: 40px 20px;
    }
    .placeholder-icon {
      font-size: 48px;
      width: 48px;
      height: 48px;
      color: rgba(0,0,0,0.3);
    }
    .detail {
      font-size: 0.85rem;
      color: rgba(0,0,0,0.5);
      max-width: 400px;
      margin: 8px auto 0;
    }
  `,
})
export class TournamentComponent {}
