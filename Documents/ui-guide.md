# UI Guide — Angular + Node.js

> Frontend architecture, component design, and Node.js BFF layer for the football predictions platform.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend framework | Angular 18+ (standalone components) |
| Language | TypeScript (strict mode) |
| Styling | SCSS + Angular Material or PrimeNG |
| Charts | ngx-charts or D3.js (for SHAP visualisations) |
| HTTP | Angular `HttpClient` with interceptors |
| State | RxJS services + Angular signals for local state |
| BFF | Node.js 20 LTS + Express + TypeScript |
| Testing (unit) | Karma + Jasmine (Angular), Jest (Node.js) |
| Testing (e2e) | Cypress or Playwright |

---

## Project Structure

```
ui/
├── client/                     # Angular application
│   ├── angular.json
│   ├── package.json
│   ├── tsconfig.json
│   └── src/
│       ├── app/
│       │   ├── app.component.ts
│       │   ├── app.routes.ts
│       │   ├── core/               # Guards, interceptors, singleton services
│       │   │   ├── interceptors/
│       │   │   │   ├── error.interceptor.ts
│       │   │   │   └── loading.interceptor.ts
│       │   │   └── guards/
│       │   ├── shared/             # Pipes, directives, reusable components
│       │   │   ├── components/
│       │   │   ├── pipes/
│       │   │   └── directives/
│       │   ├── features/           # Lazy-loaded feature modules
│       │   │   ├── dashboard/      # Home — upcoming matches with predictions
│       │   │   ├── match/          # Match detail — SHAP chart, H2H, form
│       │   │   ├── tournament/     # Bracket / group tables with predictions
│       │   │   ├── performance/    # Model accuracy, calibration curves
│       │   │   └── about/          # Methodology explanation
│       │   └── services/           # Application-wide data services
│       │       ├── prediction.service.ts
│       │       ├── team.service.ts
│       │       ├── fixture.service.ts
│       │       └── shap.service.ts
│       ├── assets/                 # Flags, icons, static images
│       └── environments/
│           ├── environment.ts
│           └── environment.prod.ts
│
└── server/                     # Node.js BFF (Express)
    ├── package.json
    ├── tsconfig.json
    └── src/
        ├── index.ts                # Express app entrypoint
        ├── routes/
        │   ├── predictions.ts      # Proxy to FastAPI /predict
        │   ├── teams.ts            # Proxy to FastAPI /teams
        │   └── health.ts           # BFF health check
        ├── middleware/
        │   ├── error-handler.ts
        │   └── request-logger.ts
        └── config.ts               # Env config (FASTAPI_URL, port, etc.)
```

---

## Pages

| Page | Route | Description |
|---|---|---|
| Dashboard | `/` | Upcoming matches with predicted scorelines, outcome probabilities, confidence bars |
| Match Detail | `/match/:id` | Scoreline probability matrix, SHAP waterfall, H2H, team form |
| Tournament | `/tournament` | Group tables with advancement probabilities, knockout bracket with progression odds |
| Performance | `/performance` | Historical accuracy, calibration curves, model comparison |
| About | `/about` | Methodology and data source explanation |

All feature routes are **lazy-loaded** via Angular Router.

---

## Key Angular Components

| Component | Purpose |
|---|---|
| `MatchCardComponent` | Team flags, predicted scoreline, outcome probabilities as stacked bar |
| `ScorelineMatrixComponent` | Heatmap grid showing probability of each scoreline (e.g., 1-0, 2-1) |
| `PredictionPanelComponent` | Expected goals (λ), most likely score, W/D/L probability bars |
| `ShapWaterfallComponent` | Interactive chart showing which features drove the goal prediction |
| `FormStripComponent` | W/D/L form displayed as coloured circles |
| `H2HSummaryComponent` | Head-to-head record mini table |
| `CalibrationChartComponent` | Calibration curve visualisation |
| `GroupSimulationComponent` | Group table with each team's advancement probability from Monte Carlo simulation |
| `BracketViewComponent` | Tournament knockout bracket with progression probabilities per team |

---

## Angular Services

| Service | Responsibility |
|---|---|
| `PredictionService` | Calls BFF to get match predictions and probabilities |
| `TeamService` | Fetches team data, flags, rosters |
| `FixtureService` | Fetches upcoming and historical fixtures |
| `ShapService` | Fetches SHAP explanation data for a given prediction |

All services use Angular `HttpClient` and return `Observable`s. Error handling via a shared `ErrorInterceptor`.

---

## Node.js BFF Role

The BFF (Backend For Frontend) sits between Angular and FastAPI:

- **Proxies requests** to the Python FastAPI service
- **Aggregates responses** when the frontend needs data from multiple FastAPI endpoints in a single call
- **Handles caching** of prediction results to reduce FastAPI load
- **Manages CORS** and request logging
- **Serves as the single entry point** for the Angular app in production

---

## Design Principles

- Mobile-first responsive layout
- Country flags for visual team identification
- Colour coding: green (home win), grey (draw), red (away win)
- All probabilities displayed as percentages rounded to 1 decimal
- Loading skeletons and error boundaries on all API calls
- Accessible: ARIA labels on interactive elements, keyboard navigation support
