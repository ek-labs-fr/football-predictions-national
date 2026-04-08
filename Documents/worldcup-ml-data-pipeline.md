# World Cup ML Prediction — Data Pipeline Guide

> How to pull and structure data from API-Football (v3) to train Poisson-based goal prediction models and derive scoreline/outcome probabilities.

---

## Table of Contents

1. [Overview](#overview)
2. [Step 1 — Discover League & Team IDs](#step-1--discover-league--team-ids)
3. [Step 2 — Pull Historical Match Results](#step-2--pull-historical-match-results)
4. [Step 3 — Pull Team Statistics](#step-3--pull-team-statistics)
5. [Step 4 — Pull Player & Squad Data](#step-4--pull-player--squad-data)
6. [Step 5 — Pull Head-to-Head History](#step-5--pull-head-to-head-history)
7. [Step 6 — Pull In-Match Events](#step-6--pull-in-match-events)
8. [Feature Engineering Reference](#feature-engineering-reference)
9. [Data Schema (Flat Training Row)](#data-schema-flat-training-row)
10. [Recommended Pull Order](#recommended-pull-order)
11. [Rate Limit Strategy](#rate-limit-strategy)

---

## Overview

The goal is to build one **flat row per match** where each row contains features describing both teams *before* the match was played, plus goal counts as labels. Every API call below feeds one or more columns in that final table.

**Target variables (primary — Poisson goal models):**

| Label | Type | Notes |
|---|---|---|
| `home_goals` | Integer (count) | Primary target for home Poisson model |
| `away_goals` | Integer (count) | Primary target for away Poisson model |

**Derived outputs (computed from predicted λ values):**

| Output | How | Notes |
|---|---|---|
| Scoreline matrix | P(h,a) = Poisson(h;λ_home) × Poisson(a;λ_away) | Full probability grid |
| `outcome` | Sum over matrix diagonals | W/D/L probabilities |
| `goal_diff` | λ_home − λ_away | Expected goal difference |
| Most likely scoreline | argmax P(h,a) | Point prediction |

**Data sources required:**

| Data Type | API Endpoint | Priority |
|---|---|---|
| Historical fixtures & results | `/fixtures` | 🔴 Essential |
| Team form statistics | `/teams/statistics` | 🔴 Essential |
| Squad & player ratings | `/players/squads` + `/players` | 🟡 Important |
| Head-to-head history | `/fixtures/headtohead` | 🟡 Important |
| Match events (goals, cards) | `/fixtures/events` | 🟢 Supplementary |
| Line-ups | `/fixtures/lineups` | 🟢 Supplementary |
| Standings / rankings | `/standings` | 🟢 Supplementary |

---

## Step 1 — Discover League & Team IDs

Before pulling any match data, you need the IDs for every competition and team you'll use. **Do this once and store the results locally.**

### 1a. Get International Competition IDs

```
GET /leagues?type=Cup&country=World
GET /leagues?type=League&country=World
```

Save the `league.id` for every competition you want to include in training:

| Competition | League ID | Seasons Available |
|---|---|---|
| FIFA World Cup | 1 | 1930–2022 |
| UEFA EURO | 4 | 1960–2024 |
| UEFA Nations League | 5 | 2018–2024 |
| Copa América | 9 | 1916–2024 |
| Africa Cup of Nations | 6 | 1957–2023 |
| CONCACAF Gold Cup | 7 | 1991–2023 |
| AFC Asian Cup | 10 | 1956–2023 |
| International Friendlies | 11 | 2010–2024 |
| WC Qualification (UEFA) | 32 | 2018–2026 |
| WC Qualification (CONMEBOL) | 34 | 2018–2026 |

> **Why include friendlies and qualifiers?** World Cups only have ~64 games. You need qualifiers, friendlies, and continental tournaments to get enough data. Weight them differently in your model (a World Cup final carries more signal than a March friendly).

### 1b. Get National Team IDs

```
GET /teams?national=true
```

Loop through results and save `team.id` + `team.name` for all national teams you want to track. Key ones:

```
GET /teams?name=France       # id: 2
GET /teams?name=Brazil       # id: 6
GET /teams?name=Germany      # id: 25
GET /teams?name=Argentina    # id: 26
GET /teams?name=Spain        # id: 9
GET /teams?name=England      # id: 10
```

Store these in a local `teams.json` or database table — you'll reference them constantly.

---

## Step 2 — Pull Historical Match Results

This is your **core training data**. Each fixture becomes one row in your dataset.

### 2a. Pull All World Cup Fixtures

```
GET /fixtures?league=1&season=2022    # Qatar 2022
GET /fixtures?league=1&season=2018    # Russia 2018
GET /fixtures?league=1&season=2014    # Brazil 2014
GET /fixtures?league=1&season=2010    # South Africa 2010
GET /fixtures?league=1&season=2006    # Germany 2006
GET /fixtures?league=1&season=2002    # Korea/Japan 2002
GET /fixtures?league=1&season=1998    # France 1998
GET /fixtures?league=1&season=1994    # USA 1994
GET /fixtures?league=1&season=1990    # Italy 1990
```

> Only go back to 1990. Earlier tournaments had significantly different formats and squad quality data is unreliable.

### 2b. Pull Supplementary Competition Fixtures

Repeat the same pattern for other leagues (EURO, Copa América, qualifiers, etc.) using the IDs from Step 1a.

```
GET /fixtures?league=4&season=2024    # EURO 2024
GET /fixtures?league=4&season=2020    # EURO 2020 (played 2021)
GET /fixtures?league=5&season=2024    # Nations League 2024
GET /fixtures?league=32&season=2026   # WC Qualification UEFA 2026
```

### 2c. What to Extract from Each Fixture

From each fixture response, extract and store:

```json
{
  "fixture_id":     855748,
  "date":           "2022-11-20",
  "league_id":      1,
  "season":         2022,
  "round":          "Group Stage - 1",
  "stage":          "group",          // derive: group / round_of_16 / qf / sf / final
  "home_team_id":   2,
  "away_team_id":   6,
  "home_goals":     2,
  "away_goals":     1,
  "home_goals_ht":  1,               // half-time score
  "away_goals_ht":  0,
  "winner":         "home",          // home / away / draw
  "status":         "FT"
}
```

> **Label derivation:** `home_goals` and `away_goals` are the primary training targets for the Poisson models. `outcome = home_win / draw / away_win` and `goal_diff = home_goals - away_goals` are derived for evaluation.

---

## Step 3 — Pull Team Statistics

This provides **pre-match team strength features**. For each team in each competition, pull their aggregate stats.

### 3a. Team Season Statistics

```
GET /teams/statistics?league=1&season=2022&team=2     # France at WC 2022
GET /teams/statistics?league=1&season=2022&team=6     # Brazil at WC 2022
```

**Repeat for every team × competition × season combination in your dataset.**

### 3b. What to Extract

```json
{
  "team_id":              2,
  "league_id":            1,
  "season":               2022,
  "form":                 "WWDWW",           // last 5 results string
  "matches_played":       7,
  "wins":                 5,
  "draws":                1,
  "losses":               1,
  "goals_scored_total":   14,
  "goals_conceded_total": 5,
  "goals_scored_avg":     2.0,
  "goals_conceded_avg":   0.71,
  "clean_sheets":         3,
  "failed_to_score":      0,
  "biggest_win":          "4-1",
  "biggest_loss":         "0-1"
}
```

### 3c. Form Leading Into Each Match

For each match in your dataset, you need the team's form **before** that specific game, not their end-of-tournament stats (that would be data leakage). To reconstruct pre-match form:

1. Sort all fixtures by date.
2. For each match on date `D`, look up each team's last 5–10 results from fixtures with `date < D`.
3. Compute rolling averages: goals scored, goals conceded, points per game, win rate.

This is computed from the fixtures table (Step 2), not from a direct API call.

---

## Step 4 — Pull Player & Squad Data

Squad quality is one of the strongest predictors. Pull this for every team at every major tournament.

### 4a. Squad Rosters

```
GET /players/squads?team=2     # France squad (all seasons)
GET /players/squads?team=6     # Brazil squad
```

This returns the current registered players per team. For historical squads, you'll need to query by season:

```
GET /players?team=2&season=2022    # France players with 2022 stats
GET /players?team=2&season=2018    # France players with 2018 stats
```

Paginate through all results (`paging.total` tells you how many pages).

### 4b. What to Extract per Player

```json
{
  "player_id":        276,
  "player_name":      "K. Mbappé",
  "team_id":          2,
  "season":           2022,
  "age":              23,
  "nationality":      "France",
  "position":         "Attacker",
  "club_league":      "Ligue 1",      // derive: is_top5_league (boolean)
  "appearances":      35,
  "goals":            28,
  "assists":          7,
  "yellow_cards":     4,
  "red_cards":        0,
  "rating":           8.1             // Sofascore-style rating if available
}
```

### 4c. Squad-Level Aggregates to Derive

From player data, compute these **per-team features** for each tournament:

| Feature | How to Compute |
|---|---|
| `squad_avg_age` | Mean age of starting 11 |
| `squad_avg_rating` | Mean player rating |
| `top5_league_ratio` | % of players from top-5 European leagues |
| `squad_goals_club_season` | Total goals scored by all players at club level that season |
| `squad_caps_avg` | Average international caps (proxy for experience) |
| `star_player_flag` | Boolean: does team have a top-20 world-ranked player |

---

## Step 5 — Pull Head-to-Head History

H2H captures recurring tactical matchups and psychological edges between specific national teams.

### 5a. H2H Fixtures

```
GET /fixtures/headtohead?h2h=2-6      # France vs Brazil all time
GET /fixtures/headtohead?h2h=2-6&last=10   # Last 10 meetings only
```

Format is always `teamA_id-teamB_id`. Pull H2H for every team pair that appears in your training fixtures.

### 5b. What to Extract

For each match in your dataset, before querying, compute from H2H history:

```json
{
  "h2h_home_wins":        4,
  "h2h_away_wins":        2,
  "h2h_draws":            3,
  "h2h_home_goals_avg":   1.8,
  "h2h_away_goals_avg":   1.2,
  "h2h_matches_total":    9,
  "h2h_last_winner":      "home"    // who won the most recent meeting
}
```

> **Minimum threshold:** Only use H2H features if the two teams have met at least 3 times. Otherwise, fill with 0 / neutral values to avoid noise from single data points.

---

## Step 6 — Pull In-Match Events

These are used for **feature enrichment within a tournament** — tracking fatigue, discipline, and momentum as the competition progresses.

### 6a. Match Events

```
GET /fixtures/events?fixture=855748
```

Extract per fixture:

```json
{
  "fixture_id":         855748,
  "team_id":            2,
  "yellow_cards":       1,
  "red_cards":          0,
  "goals_scored":       2,
  "own_goals":          0,
  "penalties_scored":   0,
  "penalties_missed":   0
}
```

Use these to build **within-tournament running features** per team:

| Feature | Description |
|---|---|
| `tournament_yellows_so_far` | Cumulative yellow cards before this match |
| `tournament_reds_so_far` | Players suspended |
| `matches_played_in_tournament` | Fatigue proxy |
| `days_since_last_match` | Rest between games |
| `came_from_shootout` | Extra physical/psychological load |

### 6b. Line-ups

```
GET /fixtures/lineups?fixture=855748
```

From line-ups, you can derive:

- Whether a team's key players actually started (vs. being rested)
- Formation played (tactical context)
- Bench depth (substitution options available)

---

## Feature Engineering Reference

This is the complete list of features to include in your final training row, grouped by category.

### Match Context Features

| Feature | Type | Source |
|---|---|---|
| `league_id` | Categorical | Fixture |
| `season` | Integer | Fixture |
| `stage` | Categorical (group/r16/qf/sf/final) | Derived from round |
| `is_knockout` | Boolean | Derived |
| `match_weight` | Float | Manual (WC final=1.0, friendly=0.2) |
| `is_host_nation_home` | Boolean | Manual lookup |
| `neutral_venue` | Boolean | Derived (always true for WC) |

### Team Strength Features (compute for both home and away)

| Feature | Type | Source |
|---|---|---|
| `team_fifa_rank` | Integer | External (FIFA.com) |
| `team_rank_diff` | Integer | Derived (home rank − away rank) |
| `team_win_rate_l10` | Float | Rolling from fixtures |
| `team_goals_scored_avg_l10` | Float | Rolling from fixtures |
| `team_goals_conceded_avg_l10` | Float | Rolling from fixtures |
| `team_points_per_game_l10` | Float | Rolling from fixtures |
| `team_clean_sheet_rate_l10` | Float | Rolling from fixtures |
| `team_form_last5` | Encoded string (WWDLW) | Rolling |

### Squad Quality Features (both teams)

| Feature | Type | Source |
|---|---|---|
| `squad_avg_age` | Float | Players endpoint |
| `squad_avg_rating` | Float | Players endpoint |
| `top5_league_ratio` | Float (0–1) | Players endpoint |
| `squad_goals_club_season` | Integer | Players endpoint |
| `star_player_present` | Boolean | Derived |

### Head-to-Head Features

| Feature | Type | Source |
|---|---|---|
| `h2h_home_win_rate` | Float | H2H endpoint |
| `h2h_avg_goals_home` | Float | H2H endpoint |
| `h2h_avg_goals_away` | Float | H2H endpoint |
| `h2h_matches_played` | Integer | H2H endpoint |
| `h2h_last_result` | Categorical | H2H endpoint |

### In-Tournament Features (both teams)

| Feature | Type | Source |
|---|---|---|
| `matches_played_in_tournament` | Integer | Running count |
| `days_since_last_match` | Integer | Date diff |
| `tournament_goals_scored_so_far` | Integer | Running sum |
| `tournament_goals_conceded_so_far` | Integer | Running sum |
| `tournament_yellows_so_far` | Integer | Events endpoint |
| `tournament_reds_so_far` | Integer | Events endpoint |
| `came_from_extra_time` | Boolean | Events |
| `came_from_shootout` | Boolean | Events |

---

## Data Schema (Flat Training Row)

Each row in your final CSV / DataFrame represents one match:

```
fixture_id, date, league_id, season, stage, is_knockout, match_weight,
home_team_id, away_team_id,

# Home team features
home_fifa_rank, home_win_rate_l10, home_goals_scored_avg_l10,
home_goals_conceded_avg_l10, home_clean_sheet_rate_l10,
home_squad_avg_age, home_squad_avg_rating, home_top5_league_ratio,
home_matches_in_tournament, home_days_rest, home_yellows_so_far,
home_came_from_shootout,

# Away team features (mirror of above)
away_fifa_rank, away_win_rate_l10, away_goals_scored_avg_l10,
away_goals_conceded_avg_l10, away_clean_sheet_rate_l10,
away_squad_avg_age, away_squad_avg_rating, away_top5_league_ratio,
away_matches_in_tournament, away_days_rest, away_yellows_so_far,
away_came_from_shootout,

# Head-to-head
h2h_home_win_rate, h2h_avg_goals_home, h2h_avg_goals_away, h2h_matches_played,

# Derived differentials (often more predictive than raw values)
rank_diff, form_diff, squad_rating_diff, goals_scored_avg_diff,

# Labels
home_goals, away_goals, goal_diff, outcome   # outcome: home_win / draw / away_win
```

---

## Recommended Pull Order

Follow this sequence to avoid wasted API calls and manage dependencies correctly:

```
1. GET /leagues          → Save all international league IDs
2. GET /teams            → Save all national team IDs
3. GET /fixtures         → Pull all historical matches per league/season
                           (this is your backbone — do it first)
4. GET /teams/statistics → One call per team × league × season
5. GET /players          → One call per team × season (paginate)
6. GET /fixtures/headtohead → One call per unique team pair in your fixture set
7. GET /fixtures/events  → One call per fixture (only needed for in-tournament features)
8. GET /fixtures/lineups → One call per fixture (optional, for formation features)
```

---

## Rate Limit Strategy

With the free plan (100 requests/day), pulling a full historical dataset will take several days. Plan accordingly.

| Step | Estimated API Calls | Notes |
|---|---|---|
| Leagues + Teams | ~5 | One-time |
| Fixtures (9 World Cups) | ~15 | One per season |
| Fixtures (EURO, Copa, etc.) | ~30 | One per season |
| Team statistics | ~500+ | All teams × all tournaments |
| Players | ~300+ | Paginated, ~3 calls per team/season |
| Head-to-head | ~200+ | One per unique team pair |
| Events | ~600+ | One per fixture |

**Total estimate: 1,500–2,000 API calls** — meaning ~15–20 days on the free plan, or a single day on the Pro plan ($19/mo).

### Tips to Minimize Calls

- Cache every response to disk immediately after fetching. Never re-fetch data you already have.
- Pull fixtures first — derive as many features as possible from fixtures alone (form, rolling averages) before spending calls on events or lineups.
- Skip events and lineups for very old tournaments (pre-2006) — data completeness is lower and the marginal signal is small.
- Use `last=10` in H2H calls rather than fetching full history — it saves calls and recent meetings are more predictive anyway.

---

## Useful Links

| Resource | URL |
|---|---|
| API-Football Docs | https://www.api-football.com/documentation-v3 |
| Coverage (which leagues/seasons) | https://www.api-football.com/coverage |
| FIFA Rankings (supplement) | https://www.fifa.com/fifa-world-ranking |
| Dashboard & API Key | https://dashboard.api-football.com |
