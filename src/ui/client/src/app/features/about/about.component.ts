import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';

@Component({
  selector: 'app-about',
  standalone: true,
  imports: [CommonModule, MatCardModule],
  template: `
    <h2>About</h2>

    <mat-card class="about-card">
      <mat-card-header>
        <mat-card-title>Methodology</mat-card-title>
      </mat-card-header>
      <mat-card-content>
        <p>This system predicts national team football match <strong>scorelines</strong> using independent Poisson regression models for home and away goals.</p>

        <h3>How it works</h3>
        <ol>
          <li><strong>Expected goals</strong> &mdash; Two separate Poisson models predict the expected number of goals for each team (&lambda;<sub>home</sub> and &lambda;<sub>away</sub>).</li>
          <li><strong>Scoreline matrix</strong> &mdash; A probability grid is computed for all plausible scorelines using the Poisson distribution.</li>
          <li><strong>Outcome probabilities</strong> &mdash; Win/draw/loss probabilities are derived by summing over the scoreline matrix.</li>
          <li><strong>Tournament simulation</strong> &mdash; Monte Carlo simulation runs thousands of tournament iterations to estimate each team's chances of advancing through each stage.</li>
        </ol>

        <h3>Features used</h3>
        <ul>
          <li>Rolling team form (last 10 matches): win rate, goals scored/conceded, clean sheets</li>
          <li>Squad quality: average age, average rating, proportion of players in top-5 leagues</li>
          <li>Head-to-head record between the two teams</li>
          <li>FIFA world rankings</li>
          <li>Tournament context: stage, match importance weight, rest days</li>
        </ul>

        <h3>Data source</h3>
        <p>All match data is sourced from <strong>API-Football v3</strong>, covering international fixtures from 1990 to present across World Cup, EURO, Copa America, AFCON, and other major tournaments.</p>
      </mat-card-content>
    </mat-card>
  `,
  styles: `
    h2 { margin: 0 0 16px; font-size: 1.3rem; }
    .about-card { max-width: 700px; }
    h3 { font-size: 1rem; margin: 16px 0 8px; }
    ol, ul { padding-left: 20px; }
    li { margin-bottom: 6px; line-height: 1.5; }
    p { line-height: 1.6; }
  `,
})
export class AboutComponent {}
