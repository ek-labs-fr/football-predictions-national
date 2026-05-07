import { ChangeDetectionStrategy, Component, OnInit, computed, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { catchError, of } from 'rxjs';
import {
  Competition,
  PredictionService,
  RecentResponse,
  UpcomingResponse,
} from '../../services/prediction.service';
import { LeagueSwitcherComponent } from '../../shared/components/league-switcher.component';
import { UpcomingMatchComponent } from '../../shared/components/upcoming-match.component';
import { RecentResultComponent } from '../../shared/components/recent-result.component';
import { CardSkeletonComponent } from '../../shared/components/card-skeleton.component';

const STORAGE_KEY = 'ericfc.selectedLeague';
const DEFAULT_LEAGUE = 'premier-league';

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [CommonModule, LeagueSwitcherComponent, UpcomingMatchComponent, RecentResultComponent, CardSkeletonComponent],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <header class="banner">
      <div class="banner-inner">
        <div class="brand">
          <img src="logo.png" alt="Eric FC" class="logo" />
          <div class="wordmark">
            <span class="ericfc">ERIC FC</span>
            <span class="tag">AI FOOTBALL PREDICTIONS</span>
          </div>
        </div>
        @if (competitions().length > 0) {
          <app-league-switcher
            class="switcher"
            [competitions]="competitions()"
            [selectedId]="selectedId()"
            (select)="onSelect($event)"
          />
        }
      </div>
    </header>

    <main class="main">
      @if (loadingComps()) {
        <p class="status">Loading…</p>
      } @else if (errorMsg()) {
        <p class="error">{{ errorMsg() }}</p>
      } @else if (selectedCompetition(); as comp) {
        <section class="block">
          <h2 class="section-title">UPCOMING PREDICTIONS</h2>
          @if (loadingUpcoming()) {
            <div class="grid">
              @for (_ of [0,1,2,3,4,5]; track $index) {
                <app-card-skeleton variant="upcoming" />
              }
            </div>
          } @else if (visibleUpcoming().length === 0) {
            <p class="empty">{{ upcomingEmptyMessage() }}</p>
          } @else {
            <div class="grid">
              @for (m of visibleUpcoming(); track m.fixture_id) {
                <app-upcoming-match [match]="m" />
              }
            </div>
          }
        </section>

        <section class="block">
          <h2 class="section-title">RECENT RESULTS &amp; ACCURACY</h2>
          @if (loadingRecent()) {
            <div class="grid">
              @for (_ of [0,1,2,3,4,5]; track $index) {
                <app-card-skeleton variant="recent" />
              }
            </div>
          } @else if (visibleRecent().length === 0) {
            <p class="empty">
              {{ recentMatches().length === 0 ? 'No recent results yet for this league.' : 'No results in the last 7 days.' }}
            </p>
          } @else {
            <div class="grid">
              @for (m of visibleRecent(); track m.fixture_id) {
                <app-recent-result [match]="m" />
              }
            </div>
          }
        </section>
      }
    </main>
  `,
  styles: `
    :host { display: block; }
    .banner {
      background: var(--ericfc-navy);
      color: #fff;
      padding: 18px 20px;
    }
    .banner-inner {
      max-width: 1280px;
      margin: 0 auto;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 18px;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
    }
    .logo {
      width: 96px;
      height: 96px;
      object-fit: contain;
    }
    .wordmark {
      display: flex;
      flex-direction: column;
      line-height: 1;
      font-family: 'Barlow Condensed', 'Inter', sans-serif;
    }
    .ericfc {
      font-weight: 800;
      font-size: 1.9rem;
      letter-spacing: 0.04em;
      color: #fff;
      text-transform: uppercase;
    }
    .tag {
      margin-top: 4px;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.22em;
      color: var(--ericfc-gold);
      text-transform: uppercase;
    }
    .switcher { width: 100%; }

    .main {
      max-width: 1280px;
      margin: 0 auto;
      padding: 28px 20px 56px;
    }
    .block {
      margin-bottom: 36px;
    }
    .block:last-child { margin-bottom: 0; }
    .section-title {
      margin: 0 0 16px;
      font-size: 1.05rem;
      font-weight: 800;
      letter-spacing: 0.02em;
      color: var(--ericfc-navy);
      text-transform: uppercase;
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    .status, .empty, .error {
      text-align: center;
      padding: 24px 12px;
      color: var(--ericfc-shadow-blue);
      font-size: 0.9rem;
    }
    .status.muted { padding: 12px; }
    .error { color: var(--ericfc-danger); }

    @media (min-width: 720px) {
      .grid { grid-template-columns: repeat(2, 1fr); gap: 16px; }
    }
    @media (min-width: 900px) {
      .banner { padding: 22px 32px; }
      .banner-inner {
        flex-direction: row;
        justify-content: space-between;
        align-items: center;
      }
      .brand { gap: 22px; }
      .logo { width: 168px; height: 168px; }
      .ericfc { font-size: 3.2rem; }
      .tag { font-size: 0.95rem; }
      .switcher { width: auto; }
      .main { padding: 36px 32px 64px; }
      .grid { grid-template-columns: repeat(3, 1fr); gap: 18px; }
      .section-title { font-size: 1.2rem; }
    }
  `,
})
export class HomeComponent implements OnInit {
  readonly competitions = signal<Competition[]>([]);
  readonly selectedId = signal<string | null>(null);
  readonly upcomingMatches = signal<UpcomingResponse['matches']>([]);
  readonly recentMatches = signal<RecentResponse['matches']>([]);
  readonly loadingComps = signal(true);
  readonly loadingUpcoming = signal(false);
  readonly loadingRecent = signal(false);
  readonly errorMsg = signal<string | null>(null);

  readonly selectedCompetition = computed<Competition | null>(() => {
    const id = this.selectedId();
    return this.competitions().find(c => c.id === id) ?? null;
  });

  readonly visibleUpcoming = computed<UpcomingResponse['matches']>(() => {
    const matches = this.upcomingMatches();
    const now = Date.now();
    const limit = this.selectedCompetition()?.upcoming_display_limit ?? null;
    const future = matches
      .filter(m => {
        if (!m.date) return false;
        const ts = new Date(m.date).getTime();
        return !isNaN(ts) && ts >= now;
      })
      .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

    if (limit && limit > 0) {
      return future.slice(0, limit);
    }

    const cutoff = now + 7 * 24 * 60 * 60 * 1000;
    return future.filter(m => new Date(m.date).getTime() <= cutoff);
  });

  readonly upcomingEmptyMessage = computed<string>(() => {
    if (this.upcomingMatches().length === 0) return 'No upcoming fixtures.';
    const limit = this.selectedCompetition()?.upcoming_display_limit ?? null;
    return limit && limit > 0 ? 'No upcoming fixtures.' : 'No fixtures in the next 7 days.';
  });

  readonly visibleRecent = computed<RecentResponse['matches']>(() => {
    const matches = this.recentMatches();
    const now = Date.now();
    const cutoff = now - 7 * 24 * 60 * 60 * 1000;
    return matches.filter(m => {
      if (!m.date) return false;
      const ts = new Date(m.date).getTime();
      return !isNaN(ts) && ts >= cutoff && ts <= now;
    });
  });

private readonly upcomingCache = new Map<string, UpcomingResponse['matches']>();
  private readonly recentCache = new Map<string, RecentResponse['matches']>();

  constructor(private predictionService: PredictionService) {}

  ngOnInit(): void {
    this.predictionService
      .getCompetitions()
      .pipe(
        catchError(() => {
          this.errorMsg.set('Failed to load leagues.');
          return of<Competition[]>([]);
        }),
      )
      .subscribe(comps => {
        this.competitions.set(comps);
        this.loadingComps.set(false);
        if (comps.length === 0) return;
        const stored = this.readStoredLeague();
        const id = comps.find(c => c.id === stored)?.id
          ?? comps.find(c => c.id === DEFAULT_LEAGUE)?.id
          ?? comps[0].id;
        this.onSelect(id);
      });
  }

  onSelect(id: string): void {
    if (this.selectedId() === id) return;
    this.selectedId.set(id);
    this.writeStoredLeague(id);
    this.loadLeague(id);
  }

  private loadLeague(id: string): void {
    const cachedUp = this.upcomingCache.get(id);
    if (cachedUp) {
      this.upcomingMatches.set(cachedUp);
    } else {
      this.upcomingMatches.set([]);
      this.loadingUpcoming.set(true);
      this.predictionService
        .getUpcoming(id)
        .pipe(catchError(() => of<UpcomingResponse | null>(null)))
        .subscribe(res => {
          const matches = res?.matches ?? [];
          this.upcomingCache.set(id, matches);
          if (this.selectedId() === id) this.upcomingMatches.set(matches);
          this.loadingUpcoming.set(false);
        });
    }

    const cachedRec = this.recentCache.get(id);
    if (cachedRec) {
      this.recentMatches.set(cachedRec);
    } else {
      this.recentMatches.set([]);
      this.loadingRecent.set(true);
      this.predictionService
        .getRecent(id)
        .pipe(catchError(() => of<RecentResponse | null>(null)))
        .subscribe(res => {
          const matches = res?.matches ?? [];
          this.recentCache.set(id, matches);
          if (this.selectedId() === id) this.recentMatches.set(matches);
          this.loadingRecent.set(false);
        });
    }
  }

  private readStoredLeague(): string | null {
    try {
      return localStorage.getItem(STORAGE_KEY);
    } catch {
      return null;
    }
  }

  private writeStoredLeague(id: string): void {
    try {
      localStorage.setItem(STORAGE_KEY, id);
    } catch {
      // localStorage unavailable — skip silently
    }
  }
}
