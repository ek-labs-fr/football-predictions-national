# Action Plan — Data Ingestion, Data Processing & Model Development

> Step-by-step implementation plan for Phases 1–3 of the football predictions project.
> Each step includes its deliverable, the file(s) it produces, and a definition of done.

---

## Phase 1 — Data Ingestion

### Step 1.1 — Project Scaffolding

Set up the Python project skeleton, dependency management, and tooling.

- [ ] Initialise `pyproject.toml` with project metadata and dependency groups (`[project.dependencies]`, `[project.optional-dependencies.dev]`)
- [ ] Core deps: `requests`, `pydantic`, `pandas`, `sqlalchemy`, `psycopg2-binary`, `python-dotenv`
- [ ] Dev deps: `pytest`, `pytest-cov`, `ruff`, `mypy`, `pre-commit`
- [ ] Configure `ruff` in `pyproject.toml` (line length 100, default rules)
- [ ] Create `.env.example` with placeholder keys (`API_FOOTBALL_KEY=`, `DATABASE_URL=`)
- [ ] Create `.gitignore` (exclude `artefacts/`, `data/raw/`, `.env`, `__pycache__/`, `*.pkl`)
- [ ] Set up `pre-commit` config with `ruff format`, `ruff check`, `gitleaks`
- [ ] Create directory structure: `src/data/`, `src/features/`, `src/models/`, `tests/unit/`, `tests/fixtures/`, `data/raw/`, `data/processed/`
- [ ] Verify: `uv sync && ruff check src/ && pytest` all pass on empty project

**Deliverables:** `pyproject.toml`, `.gitignore`, `.env.example`, `.pre-commit-config.yaml`, empty directory tree

---

### Step 1.2 — API Client

Build a reusable, rate-limited API-Football client with local caching.

- [ ] Create `src/data/api_client.py`
- [ ] Implement `APIFootballClient` class:
  - Constructor takes API key from env var `API_FOOTBALL_KEY`
  - Sets base URL `https://v3.football.api-sports.io`
  - Sends `x-apisports-key` header on every request
- [ ] Add **rate limiting**: enforce max N requests/day (configurable, default 100 for free tier)
- [ ] Add **local disk caching**: before each request, check if `data/raw/{endpoint}_{params_hash}.json` exists; if so, return cached response instead of calling the API
- [ ] Add **retry logic**: retry on 429/5xx with exponential backoff (max 3 retries)
- [ ] Add **response validation**: check the `errors` field in every response; raise on non-empty errors
- [ ] Add **logging**: log every request (endpoint, params, cache hit/miss, status code)
- [ ] Write unit tests in `tests/unit/data/test_api_client.py`:
  - Test cache hit skips HTTP call
  - Test rate limit raises when quota exhausted
  - Test retry on 429
  - Test error field detection

**Deliverables:** `src/data/api_client.py`, `tests/unit/data/test_api_client.py`

---

### Step 1.3 — Pydantic Schemas

Define response schemas for all API-Football endpoints used.

- [ ] Create `src/data/schemas.py`
- [ ] Define Pydantic models for:
  - `League` (id, name, type, country, season)
  - `Team` (id, name, country, national flag, logo)
  - `Fixture` (id, date, league_id, season, round, status, home team, away team, goals, score)
  - `TeamStatistics` (team_id, league_id, season, form, wins, draws, losses, goals for/against)
  - `Player` (id, name, age, nationality, position, club, appearances, goals, assists, rating)
  - `HeadToHead` (list of fixtures between two teams)
  - `FixtureEvent` (fixture_id, team_id, type, detail, minute, player)
- [ ] All models should handle missing/null fields gracefully with `Optional` and defaults
- [ ] Write tests validating parsing of sample API responses stored in `tests/fixtures/`

**Deliverables:** `src/data/schemas.py`, `tests/unit/data/test_schemas.py`, sample JSON files in `tests/fixtures/`

---

### Step 1.4 — Reference Data Pull (Leagues & Teams)

Pull and store the static reference data: league IDs and national team IDs.

- [ ] Create `src/data/ingest.py` with functions:
  - `fetch_international_leagues() -> list[League]` — calls `GET /leagues?type=Cup&country=World` + `GET /leagues?type=League&country=World`
  - `fetch_national_teams() -> list[Team]` — iterates known competition IDs to discover all participating national teams
- [ ] Store results as JSON in `data/raw/leagues.json` and `data/raw/teams.json`
- [ ] Build a lookup mapping: `team_name -> team_id` saved to `data/processed/team_lookup.json`
- [ ] Verify the key competition IDs match expected values (World Cup = 1, EURO = 4, etc.)
- [ ] Log total counts: number of leagues found, number of national teams found

**Deliverables:** `data/raw/leagues.json`, `data/raw/teams.json`, `data/processed/team_lookup.json`

---

### Step 1.5 — Historical Fixtures Pull

Pull all international match results — the backbone of the training dataset.

- [ ] Add `fetch_fixtures(league_id: int, season: int) -> list[Fixture]` to `ingest.py`
- [ ] Create `scripts/bootstrap_data.py` that loops through all target competitions and seasons:
  - World Cup: 1990, 1994, 1998, 2002, 2006, 2010, 2014, 2018, 2022
  - EURO: 2000, 2004, 2008, 2012, 2016, 2020, 2024
  - Copa America, AFCON, Gold Cup, Asian Cup: all available seasons from 2000+
  - Nations League: 2018, 2020, 2022, 2024
  - WC Qualifiers (UEFA/CONMEBOL): 2018, 2022, 2026
  - International Friendlies: 2010–2025
- [ ] Cache each response to `data/raw/fixtures/{league_id}_{season}.json`
- [ ] After all pulls, merge into a single `data/processed/all_fixtures.csv` with columns:
  `fixture_id, date, league_id, season, round, stage, home_team_id, away_team_id, home_goals, away_goals, home_goals_ht, away_goals_ht, winner, status`
- [ ] Derive `stage` (group / round_of_16 / quarterfinal / semifinal / final) from `round` string
- [ ] Derive `outcome` label: `home_win`, `draw`, `away_win`
- [ ] Filter to only completed matches (`status == "FT"` or `"AET"` or `"PEN"`)
- [ ] Log: total fixtures pulled per competition, total row count, date range

**Deliverables:** `data/raw/fixtures/` directory, `data/processed/all_fixtures.csv`, `scripts/bootstrap_data.py`

---

### Step 1.6 — Team Statistics Pull

Pull aggregate team statistics per competition per season.

- [ ] Add `fetch_team_statistics(league_id: int, season: int, team_id: int) -> TeamStatistics` to `ingest.py`
- [ ] Loop through every unique `(team_id, league_id, season)` combination found in fixtures
- [ ] Cache to `data/raw/team_stats/{team_id}_{league_id}_{season}.json`
- [ ] Merge into `data/processed/team_statistics.csv` with columns:
  `team_id, league_id, season, form, matches_played, wins, draws, losses, goals_scored_total, goals_conceded_total, goals_scored_avg, goals_conceded_avg, clean_sheets, failed_to_score`
- [ ] Log: total team-season combinations pulled, any missing data

**Deliverables:** `data/raw/team_stats/` directory, `data/processed/team_statistics.csv`

---

### Step 1.7 — Player & Squad Data Pull

Pull player-level data for squad quality features.

- [ ] Add `fetch_players(team_id: int, season: int) -> list[Player]` to `ingest.py` (handle pagination via `paging.total`)
- [ ] Pull for every team that appears in fixtures from 2006 onward (pre-2006 data is sparse)
- [ ] Cache to `data/raw/players/{team_id}_{season}.json`
- [ ] Merge into `data/processed/players.csv` with columns:
  `player_id, player_name, team_id, season, age, nationality, position, club_league, appearances, goals, assists, yellow_cards, red_cards, rating`
- [ ] Log: total players pulled, teams with missing/incomplete squad data

**Deliverables:** `data/raw/players/` directory, `data/processed/players.csv`

---

### Step 1.8 — Head-to-Head Data Pull

Pull historical matchup records for all team pairs in the fixture set.

- [ ] Add `fetch_head_to_head(team_a_id: int, team_b_id: int, last: int = 10) -> list[Fixture]` to `ingest.py`
- [ ] Build set of unique team pairs from `all_fixtures.csv` (order-independent: `(min_id, max_id)`)
- [ ] Cache to `data/raw/h2h/{team_a_id}_{team_b_id}.json`
- [ ] Merge into `data/processed/h2h_raw.csv` — one row per H2H fixture
- [ ] Log: total unique pairs, total H2H fixtures pulled

**Deliverables:** `data/raw/h2h/` directory, `data/processed/h2h_raw.csv`

---

### Step 1.9 — Match Events Pull

Pull in-match events for tournament feature enrichment.

- [ ] Add `fetch_events(fixture_id: int) -> list[FixtureEvent]` to `ingest.py`
- [ ] Pull for all fixtures from 2006 onward only
- [ ] Cache to `data/raw/events/{fixture_id}.json`
- [ ] Merge into `data/processed/events.csv` with columns:
  `fixture_id, team_id, yellow_cards, red_cards, goals, own_goals, penalties_scored, penalties_missed`
- [ ] Log: total fixtures processed, any fixtures with no event data

**Deliverables:** `data/raw/events/` directory, `data/processed/events.csv`

---

### Step 1.10 — Data Ingestion Validation

Validate completeness and integrity of all ingested data before moving to processing.

- [ ] Write `scripts/validate_ingestion.py` that checks:
  - `all_fixtures.csv` has no duplicate `fixture_id` values
  - Every `home_team_id` and `away_team_id` exists in `team_lookup.json`
  - Every fixture has a valid `outcome` label
  - `team_statistics.csv` covers at least 80% of `(team_id, league_id, season)` combinations from fixtures
  - `players.csv` has data for teams in post-2006 fixtures
  - `h2h_raw.csv` covers all unique team pairs from fixtures
  - Date range spans 1990–2025
- [ ] Print a summary report: row counts per table, missing coverage percentages, date ranges
- [ ] Flag but don't block on non-critical gaps (missing events for older tournaments is expected)

**Deliverables:** `scripts/validate_ingestion.py`, validation report printed to console

---

## Phase 2 — Data Processing & Feature Engineering

### Step 2.1 — Rolling Team Performance Features

Compute pre-match form indicators from historical fixtures.

- [ ] Create `src/features/rolling.py`
- [ ] Sort `all_fixtures.csv` by date ascending
- [ ] For each match on date D, for each team (home and away), compute from all that team's prior fixtures where `date < D`:
  - `win_rate_l10` — win rate over last 10 matches
  - `goals_scored_avg_l10` — average goals scored, last 10
  - `goals_conceded_avg_l10` — average goals conceded, last 10
  - `points_per_game_l10` — (3×wins + 1×draws) / matches, last 10
  - `clean_sheet_rate_l10` — proportion of clean sheets, last 10
  - `form_last5` — encoded string (e.g., "WWDLW"), last 5
- [ ] Handle edge cases: teams with fewer than 10 prior matches (use all available, set a `matches_available` count)
- [ ] **Leakage test:** write a test that asserts every feature row only uses data from strictly before the match date
- [ ] Output: `data/processed/features_rolling.csv` keyed by `(fixture_id, team_id)`

**Deliverables:** `src/features/rolling.py`, `data/processed/features_rolling.csv`, `tests/unit/features/test_rolling.py`

---

### Step 2.2 — Squad Quality Features

Aggregate player-level data into per-team-per-season squad strength indicators.

- [ ] Create `src/features/squad.py`
- [ ] For each `(team_id, season)` in `players.csv`, compute:
  - `squad_avg_age` — mean age of all listed players
  - `squad_avg_rating` — mean player rating (where available; use NaN if <50% have ratings)
  - `top5_league_ratio` — % of players from top-5 European leagues (Premier League, La Liga, Bundesliga, Serie A, Ligue 1)
  - `squad_goals_club_season` — total goals scored by all players at club level
  - `star_player_present` — boolean: does the team have a player with rating >= 8.0
- [ ] Identify top-5 leagues by string matching on `club_league` field (define a config list)
- [ ] Handle missing seasons: forward-fill from the nearest available season for that team
- [ ] Output: `data/processed/features_squad.csv` keyed by `(team_id, season)`

**Deliverables:** `src/features/squad.py`, `data/processed/features_squad.csv`, `tests/unit/features/test_squad.py`

---

### Step 2.3 — Head-to-Head Features

Compute historical matchup statistics for each team pair.

- [ ] Create `src/features/h2h.py`
- [ ] For each match in the fixture set, look up all prior H2H fixtures between those two teams (from `h2h_raw.csv` where `date < match_date`)
- [ ] Compute:
  - `h2h_home_wins`, `h2h_away_wins`, `h2h_draws`
  - `h2h_home_goals_avg`, `h2h_away_goals_avg`
  - `h2h_matches_total`
  - `h2h_last_winner` (categorical: home / away / draw)
  - `h2h_home_win_rate`
- [ ] **Minimum threshold:** only populate H2H features when teams have met >= 3 times; otherwise fill with neutral values (0 for counts, 0.33 for rates)
- [ ] Output: `data/processed/features_h2h.csv` keyed by `(fixture_id)`

**Deliverables:** `src/features/h2h.py`, `data/processed/features_h2h.csv`, `tests/unit/features/test_h2h.py`

---

### Step 2.4 — In-Tournament Features

Build running features that track team state within a tournament.

- [ ] Create `src/features/tournament.py`
- [ ] For each fixture within a given `(league_id, season)`, compute cumulative stats for each team up to but not including the current match:
  - `matches_played_in_tournament`
  - `tournament_goals_scored_so_far`
  - `tournament_goals_conceded_so_far`
  - `tournament_yellows_so_far` (from events)
  - `tournament_reds_so_far`
  - `days_since_last_match` (date difference to the team's previous match in the tournament)
  - `came_from_extra_time` — boolean (previous match went to AET)
  - `came_from_shootout` — boolean (previous match went to penalties)
- [ ] Group-stage matches start with zeros; knockout matches accumulate from the group stage
- [ ] Output: `data/processed/features_tournament.csv` keyed by `(fixture_id, team_id)`

**Deliverables:** `src/features/tournament.py`, `data/processed/features_tournament.csv`, `tests/unit/features/test_tournament.py`

---

### Step 2.5 — Match Context Features

Derive contextual features from the fixture metadata itself.

- [ ] Add logic to `src/features/build.py`:
  - `stage` — categorical: group / round_of_16 / quarterfinal / semifinal / final (derived from `round` string)
  - `is_knockout` — boolean: true for round_of_16 onward
  - `match_weight` — float: World Cup final = 1.0, WC knockout = 0.9, WC group = 0.8, continental final = 0.7, continental group = 0.6, qualifier = 0.4, friendly = 0.2
  - `neutral_venue` — boolean (true for all World Cup/continental tournament matches)

**Deliverables:** match context columns added in the build step

---

### Step 2.6 — External Data: FIFA Rankings

Integrate FIFA world rankings as a feature.

- [ ] Source historical FIFA rankings (monthly snapshots from fifa.com or a public dataset)
- [ ] Store as `data/external/fifa_rankings.csv` with columns: `team_name, team_id, rank, rank_date`
- [ ] For each fixture on date D, look up each team's most recent ranking where `rank_date <= D`
- [ ] Compute `rank_diff = home_rank - away_rank` (negative = home team ranked higher)
- [ ] Handle teams with no ranking: assign a default rank of 150 (weaker than all ranked teams)
- [ ] Output: ranking columns merged in the build step

**Deliverables:** `data/external/fifa_rankings.csv`, ranking lookup logic in `src/features/build.py`

---

### Step 2.7 — Assemble Flat Training Table

Join all feature sources into a single row-per-match training dataset.

- [ ] Implement `build_training_table()` in `src/features/build.py`
- [ ] Start from `all_fixtures.csv` as the backbone
- [ ] Left-join features for the **home team**:
  - Rolling features (`features_rolling.csv` where `team_id == home_team_id`)
  - Squad features (`features_squad.csv` where `team_id == home_team_id`)
  - Tournament features (`features_tournament.csv` where `team_id == home_team_id`)
  - FIFA rank
- [ ] Left-join features for the **away team** (same tables, prefixed with `away_`)
- [ ] Left-join H2H features (`features_h2h.csv` on `fixture_id`)
- [ ] Add match context features
- [ ] Compute **differential features:**
  - `rank_diff = home_fifa_rank - away_fifa_rank`
  - `form_diff = home_points_per_game_l10 - away_points_per_game_l10`
  - `squad_rating_diff = home_squad_avg_rating - away_squad_avg_rating`
  - `goals_scored_avg_diff = home_goals_scored_avg_l10 - away_goals_scored_avg_l10`
  - `top5_ratio_diff = home_top5_league_ratio - away_top5_league_ratio`
- [ ] Add labels: `home_goals`, `away_goals`, `goal_diff`, `outcome`
- [ ] Drop any rows where the match was not completed
- [ ] Output: `data/processed/training_table.csv`
- [ ] Log: total rows, column count, class distribution of `outcome`, date range, missing value summary

**Deliverables:** `src/features/build.py`, `data/processed/training_table.csv`

---

### Step 2.8 — Data Quality & Leakage Audit

Final validation before model training.

- [ ] Create `scripts/validate_features.py`
- [ ] Check no future data leakage: for a sample of 100 random rows, verify all feature source dates < match date
- [ ] Check missing values: report % missing per column; flag columns with >30% missing
- [ ] Check class balance: print `outcome` value counts and percentages
- [ ] Check feature distributions: print mean, std, min, max for all numeric columns; flag any with zero variance
- [ ] Check for duplicate rows
- [ ] Check correlations: identify feature pairs with |correlation| > 0.95
- [ ] Output a summary report to `outputs/data_quality_report.txt`

**Deliverables:** `scripts/validate_features.py`, `outputs/data_quality_report.txt`

---

## Phase 3 — Model Development

### Step 3.1 — Train/Test Split

Create time-based splits for evaluation.

- [ ] Implement splitting logic in `src/models/train.py`
- [ ] **Holdout test set:** all World Cup 2022 matches (`league_id == 1, season == 2022`) — ~64 matches
- [ ] **Training set:** all matches with `date < "2022-11-20"` (WC 2022 start)
- [ ] **CV strategy:** `TimeSeriesSplit(n_splits=5)` on the training set, sorted by date
- [ ] Compute `sample_weight` per row using `match_weight` column
- [ ] Save split indices for reproducibility
- [ ] Log: train size, test size, date ranges, class distribution in each split

**Deliverables:** splitting logic in `src/models/train.py`, logged split summary

---

### Step 3.2 — Baseline Models

Establish performance floors that all candidate models must beat.

- [ ] Implement in `src/models/train.py`:
  - **Baseline 1 — Mean goals:** predict average home/away goals from training set for every match
  - **Baseline 2 — FIFA rank only:** `PoissonRegressor` on `rank_diff` alone
  - **Baseline 3 — Majority class:** `DummyClassifier(strategy="most_frequent")` for derived W/D/L comparison
  - **Baseline 4 — Betting odds** (if available): convert to implied probabilities
- [ ] Evaluate each on the holdout test set using goal prediction metrics (MAE, RPS, exact scoreline accuracy) and derived outcome metrics (accuracy, log loss, Brier score)
- [ ] Log results to `outputs/baseline_results.csv`

**Deliverables:** baseline models in `src/models/train.py`, `outputs/baseline_results.csv`

---

### Step 3.3 — Candidate Model Training

Train the core model candidates on the full feature set. **Poisson goal models are the primary approach.**

- [ ] Implement model definitions in `src/models/train.py`:
  - **Primary — Poisson Regression (linear):** two `PoissonRegressor` models (home goals, away goals) with `StandardScaler`
  - **Primary — XGBoost Poisson:** two `XGBRegressor(objective="count:poisson")` models (home goals, away goals) with early stopping
  - **Primary — LightGBM Poisson:** two `LGBMRegressor(objective="poisson")` models for comparison
  - **Secondary — XGBoost Classifier:** `objective="multi:softprob", num_class=3` for direct W/D/L comparison
  - **Secondary — Logistic Regression:** `class_weight="balanced"` as interpretable sanity check
- [ ] For all Poisson models: train on `home_goals` and `away_goals` as separate targets
- [ ] For all Poisson models: derive outcome probabilities via scoreline matrix for evaluation against classifiers
- [ ] Train each model using `TimeSeriesSplit` CV with `sample_weight`
- [ ] For XGBoost/LightGBM: use `eval_set` with early stopping (50 rounds)
- [ ] Save each trained model pair to `artefacts/{model_name}_home.pkl` and `artefacts/{model_name}_away.pkl`
- [ ] Log training time per model

**Deliverables:** trained models in `artefacts/`, training logic in `src/models/train.py`

---

### Step 3.4 — Model Evaluation

Rigorously compare all models using multiple metrics.

- [ ] Implement `src/models/evaluate.py`
- [ ] For each Poisson model pair, compute on each CV fold and on the holdout test set:
  - **Goal metrics (primary):** MAE per team, exact scoreline accuracy, Ranked Probability Score
  - **Derived outcome metrics:** Accuracy (W/D/L), Log Loss, Brier Score
  - Per-class Precision, Recall, F1 (on derived outcomes)
  - Confusion matrix (on derived outcomes)
- [ ] For each classifier model, compute the same outcome metrics for direct comparison
- [ ] Compute **overfitting diagnostic:** `test_MAE - train_MAE` (flag if gap > 0.15)
- [ ] Compute **stage-stratified metrics:** report metrics separately for group stage vs knockout
- [ ] Produce comparison table: `outputs/model_comparison.csv` — unified table with goal and outcome metrics
- [ ] Generate plots:
  - Predicted vs actual goal distribution histograms (`outputs/goal_distribution_{model}.png`)
  - Confusion matrix per model on derived outcomes (`outputs/confusion_matrix_{model}.png`)
  - Bar chart comparing MAE and log loss across models (`outputs/model_comparison_chart.png`)
- [ ] Write unit tests for metric computation functions

**Deliverables:** `src/models/evaluate.py`, `outputs/model_comparison.csv`, evaluation plots, `tests/unit/models/test_evaluate.py`

---

### Step 3.5 — Feature Selection

Reduce features to improve generalisation and interpretability.

- [ ] Implement `src/models/select.py`
- [ ] Run the following pipeline sequentially:
  1. **Variance threshold** (remove features with variance < 0.01)
  2. **Correlation filter** (remove one from each pair with |r| > 0.90)
  3. **RFECV** using Logistic Regression with `TimeSeriesSplit`, `scoring="neg_log_loss"`, `min_features_to_select=10`
  4. **Permutation importance** on XGBoost (30 repeats) — drop features with negative importance
- [ ] Save selected feature list to `artefacts/selected_features.pkl`
- [ ] Generate RFECV curve plot: `outputs/rfecv_curve.png`
- [ ] Generate correlation heatmap: `outputs/correlation_heatmap.png`
- [ ] Retrain best model on selected features only; verify test performance does not degrade
- [ ] Log: features removed at each stage, final feature count

**Deliverables:** `src/models/select.py`, `artefacts/selected_features.pkl`, selection plots

---

### Step 3.6 — Hyperparameter Tuning

Optimise the top 1–2 models using Bayesian search.

- [ ] Implement `src/models/tune.py`
- [ ] Add `optuna` to project dependencies
- [ ] Define objective function for XGBoost (and LightGBM if competitive):
  - Search space: `n_estimators`, `learning_rate`, `max_depth`, `min_child_weight`, `subsample`, `colsample_bytree`, `gamma`, `reg_alpha`, `reg_lambda`
  - CV: `TimeSeriesSplit(n_splits=5)` with `neg_log_loss` scoring
  - Use selected features only (from Step 3.5)
- [ ] Run 100 trials with 1-hour timeout
- [ ] Save best parameters to `artefacts/best_params.json`
- [ ] Retrain model with best parameters on full training set
- [ ] Evaluate on holdout test set; compare to pre-tuning performance
- [ ] Log: best trial score, parameter values, improvement over default

**Deliverables:** `src/models/tune.py`, `artefacts/best_params.json`, tuned model in `artefacts/`

---

### Step 3.7 — Probability Calibration

Ensure predicted goal distributions and derived outcome probabilities are trustworthy.

- [ ] Implement `src/models/calibrate.py`
- [ ] Split training data into train + calibration sets (last 15% by date as calibration)
- [ ] Train final Poisson model pair on the train portion
- [ ] Calibrate the predicted λ values: compare predicted vs actual goal distributions, fit a bivariate Poisson correlation parameter (ρ) on the calibration set to correct draw probability
- [ ] Generate calibration curves for derived W/D/L probabilities: `outputs/calibration_curves.png`
- [ ] Generate predicted vs actual goal count comparison: `outputs/goal_calibration.png`
- [ ] Compare calibrated vs uncalibrated Brier scores and RPS
- [ ] Save calibrated models: `artefacts/model_home_calibrated.pkl`, `artefacts/model_away_calibrated.pkl`
- [ ] Save bivariate correlation parameter: `artefacts/rho.json`
- [ ] Save scaler: `artefacts/scaler.pkl`

**Deliverables:** `src/models/calibrate.py`, calibrated model artefacts, `artefacts/scaler.pkl`, calibration plots

---

### Step 3.8 — SHAP Explainability

Generate feature attributions for model transparency.

- [ ] Implement `src/models/explain.py`
- [ ] Compute SHAP values using `TreeExplainer` (XGBoost/LightGBM) on the test set
- [ ] Generate and save:
  - Summary dot plot for home_win class: `outputs/shap_summary_home_win.png`
  - Bar importance plot: `outputs/shap_bar_importance.png`
  - Dependence plots for top 5 features: `outputs/shap_dependence_{feature}.png`
  - Waterfall plot for 3 notable matches (e.g., WC 2022 final): `outputs/shap_waterfall_{fixture_id}.png`
  - Interactive force plot: `outputs/shap_force_plot.html`
- [ ] Compute SHAP interaction values on a 200-row sample: `outputs/shap_interactions.png`
- [ ] Save SHAP explainer: `artefacts/shap_explainer.pkl`
- [ ] Save SHAP feature importance: `artefacts/shap_feature_importance.csv`

**Deliverables:** `src/models/explain.py`, SHAP plots in `outputs/`, SHAP artefacts

---

### Step 3.9 — Ensemble & Final Model Selection

Select or combine the best Poisson model pair for production.

- [ ] Compare linear Poisson vs XGBoost Poisson vs LightGBM Poisson on holdout test set
- [ ] If multiple Poisson variants are competitive: average their λ predictions (weighted by inverse MAE)
- [ ] Optionally blend Poisson-derived outcome probabilities with direct classifier probabilities (weighted ensemble)
- [ ] Only adopt ensemble if it improves MAE by >= 0.02 or RPS by >= 0.005 over the best single model pair
- [ ] Calibrate the final model pair (repeat Step 3.7)
- [ ] Save final production models: `artefacts/model_final_home.pkl`, `artefacts/model_final_away.pkl`

**Deliverables:** ensemble logic in `src/models/train.py`, final model artefacts

---

### Step 3.10 — Tournament Simulation

Build Monte Carlo simulation to predict group stage outcomes and knockout bracket probabilities.

- [ ] Implement `src/models/simulate.py`
- [ ] Implement `simulate_match(lambda_home, lambda_away, rho)`:
  - Sample a scoreline from the bivariate Poisson distribution
  - Return (home_goals, away_goals)
- [ ] Implement `simulate_group_stage(group_teams, model_home, model_away, feature_store, n_sims=10000)`:
  - For each simulation: play all group matches, compute points / GD / GS
  - Apply FIFA tiebreaker rules (points → GD → GS → H2H → drawing of lots)
  - Record which teams finish 1st, 2nd, 3rd in each simulation
  - Return advancement probabilities per team
- [ ] Implement `simulate_knockout(bracket, model_home, model_away, feature_store, n_sims=10000)`:
  - Simulate each knockout match; if draw after 90 min, simulate extra time (inflate λ by 1/3) then penalty shootout (50/50)
  - Track each team's probability of reaching QF, SF, Final, and winning
- [ ] Implement `simulate_tournament(groups, model_home, model_away, feature_store, n_sims=10000)`:
  - End-to-end: group stage → knockout → champion
  - Return DataFrame with columns: `team, group_win_prob, advance_prob, qf_prob, sf_prob, final_prob, champion_prob`
- [ ] Validate against WC 2022: simulate using pre-tournament data and compare advancement predictions to actual results
- [ ] Save simulation results to `outputs/tournament_simulation.csv`
- [ ] Generate bracket probability visualisation: `outputs/tournament_bracket.png`

**Deliverables:** `src/models/simulate.py`, `outputs/tournament_simulation.csv`, bracket visualisation, `tests/unit/models/test_simulate.py`

---

### Step 3.11 — Production Readiness Checklist

Final validation before the model is used for real predictions and tournament simulation.

- [ ] Run checklist in `scripts/validate_model.py`:
  - No temporal leakage in train/test split ✓
  - Poisson model pair calibrated on held-out data ✓
  - Bivariate correlation parameter (ρ) fitted ✓
  - All features available at inference time (no post-match fields used) ✓
  - Missing value handling implemented (defaults defined) ✓
  - Scaler fitted on train only, saved to disk ✓
  - SHAP explainer saved to disk ✓
  - Test MAE within 0.05 of CV MAE ✓
  - Derived outcome probabilities sum to 1.0 ✓
  - Tournament simulation produces valid bracket probabilities ✓
  - Model version logged ✓
- [ ] Implement `predict_match()` inference function in `src/models/train.py`:
  - Takes `home_team_id`, `away_team_id`, `league_id`, `match_date`
  - Builds feature row from processed data
  - Returns `{"lambda_home": float, "lambda_away": float, "most_likely_score": str, "home_win": float, "draw": float, "away_win": float}`
- [ ] Test `predict_match()` on 5 known historical matches and verify output format
- [ ] Test `simulate_tournament()` on WC 2022 data and verify output format
- [ ] Save all final artefacts to `artefacts/`:
  - `model_final_home.pkl`, `model_final_away.pkl`, `scaler.pkl`, `rho.json`
  - `shap_explainer.pkl`, `selected_features.pkl`
  - `model_comparison.csv`, `shap_feature_importance.csv`, `best_params.json`
- [ ] Print final performance summary: best model pair name, test MAE, test RPS, exact scoreline accuracy, derived accuracy/log loss vs baselines

**Deliverables:** `scripts/validate_model.py`, inference function, tournament simulation, all artefacts saved, final summary report
