import { Routes } from '@angular/router';

export const routes: Routes = [
  {
    path: '',
    loadComponent: () =>
      import('./features/dashboard/dashboard.component').then(m => m.DashboardComponent),
  },
  {
    path: 'match/:id',
    loadComponent: () =>
      import('./features/match/match-detail.component').then(m => m.MatchDetailComponent),
  },
  {
    path: 'tournament',
    loadComponent: () =>
      import('./features/tournament/tournament.component').then(m => m.TournamentComponent),
  },
  {
    path: 'about',
    loadComponent: () =>
      import('./features/about/about.component').then(m => m.AboutComponent),
  },
  { path: '**', redirectTo: '' },
];
