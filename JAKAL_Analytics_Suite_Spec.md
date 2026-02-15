# JAKAL Analytics Plugin Suite
## V2 + V3 Spec - Build on Complete Data

---

## DATA MODEL (confirmed in DB)

```
8 tables, 128 columns

players               → who we track
stats_snapshots       → season aggregate stats
map_stats             → per-map performance
operator_stats        → per-operator performance  
match_history         → 40-200 matches per player
match_detail_players  → all 10 players per match
round_outcomes        → round end reason + winner
player_rounds         → every player, every round
                        (operator, side, K/D, FB, clutch)
```

---

## PLUGIN ARCHITECTURE

```
src/plugins/
├── __init__.py
├── base.py              # BasePlugin class
├── v2_map_pool.py
├── v2_session.py
├── v2_operator_pool.py
├── v3_round_analysis.py
├── v3_teammate_chemistry.py
├── v3_lobby_quality.py
└── v3_trajectory.py

Each plugin:
  - Takes db: Database + player_id as input
  - Returns structured dict with findings
  - Has a summary() method for terminal display
  - Has findings[] list for UI rendering
```

---

## V2 PLUGINS (map + operator + session data)

---

### V2.1 - Map Pool Analyzer
**File:** `src/plugins/v2_map_pool.py`
**Data:** `map_stats` table

```python
def analyze(player_id) -> Dict:

INPUT QUERY:
  SELECT map_name, matches, win_pct, kd,
         atk_win_pct, def_win_pct, hs_pct, esr
  FROM map_stats
  WHERE player_id = ?
  ORDER BY matches DESC

CALCULATIONS:
  # Sample size threshold
  RELIABLE_MAP = matches >= 10

  # Starting side detection (from match data)
  # Maps where you start attack vs defense
  
  # Win rate vs league average (~50%)
  map_edge = win_pct - 50.0
  
  # Attack/defense imbalance
  side_gap = abs(atk_win_pct - def_win_pct)
  weak_side = 'attack' if atk_win_pct < def_win_pct else 'defense'
  
  # RP cost calculation
  # Avg RP per loss: ~-20, per win: ~+25
  # Expected RP from map = (win_pct * 25) + ((1-win_pct) * -20)
  # vs neutral map expected RP = (0.5 * 25) + (0.5 * -20) = 2.5
  map_rp_value = (win_pct/100 * 25) - ((1 - win_pct/100) * 20)
  neutral_rp = 2.5
  rp_above_neutral = map_rp_value - neutral_rp

OUTPUTS:
  best_maps: top 3 by win_pct (min 10 matches)
  worst_maps: bottom 3 by win_pct (min 10 matches)
  ban_recommendation: worst map with most matches
  side_weak_maps: maps where side gap > 15%
  rp_drain_maps: maps costing negative expected RP
  map_pool_score: weighted avg win_pct across maps

FINDINGS (plain English):
  "Ban Fortress. You win 28% there (72 matches).
   That's costing you ~180 RP this season."

  "Your Clubhouse defense is elite (71%).
   Your Clubhouse attack is weak (41%).
   You start attack on Clubhouse.
   This is your biggest correctable loss source."

  "Best maps: Border (61%), Kafe (58%), Bank (62%)"
```

---

### V2.2 - Session Analyzer
**File:** `src/plugins/v2_session.py`
**Data:** `match_history` table (timestamp + result + rp_change)

```python
def analyze(player_id) -> Dict:

INPUT QUERY:
  SELECT map_name, result, rp_change, kd, scraped_at
  FROM match_history
  WHERE player_id = ?
  ORDER BY scraped_at DESC

CALCULATIONS:
  # Group matches into sessions
  # New session = gap > 2 hours between matches
  
  # Per session:
  session_win_rate by game_number_in_session
  # Game 1: win %
  # Game 2: win %
  # Game 3: win %... etc
  
  # Tilt detection: win rate after N consecutive losses
  win_rate_after_loss_streak(1), (2), (3), (4)
  
  # Session length sweet spot
  best_session_length: game N where win rate peaks
  
  # RP per session: running total per session
  avg_rp_gain_per_session
  avg_rp_loss_per_session
  
  # Time of day (if timestamps reliable)
  performance_by_hour

OUTPUTS:
  avg_session_length: float
  win_rate_by_game_number: Dict[int, float]
  win_rate_after_streak: Dict[int, float]
  tilt_threshold: int (game where win rate drops below 40%)
  peak_game: int (game number with highest win rate)
  rp_per_session: float
  sessions_analyzed: int

FINDINGS:
  "You win 61% in games 1-3.
   You win 38% in games 4-6.
   You win 24% in games 7+.
   Stop at game 5. You have lost ~340 RP ignoring this."

  "After 3 consecutive losses your win rate drops to 29%.
   Take a break after 2 losses in a row."

  "Your average session: 7.2 games.
   Your optimal session: 4 games."
```

---

### V2.3 - Operator Pool Analyzer
**File:** `src/plugins/v2_operator_pool.py`
**Data:** `operator_stats` + `player_rounds` tables

```python
def analyze(player_id) -> Dict:

INPUT QUERY 1 (season aggregate):
  SELECT operator_name, side, rounds, win_pct,
         kd, hs_pct, esr, kills, deaths
  FROM operator_stats
  WHERE player_id = ?
  ORDER BY rounds DESC

INPUT QUERY 2 (round-level, from player_rounds):
  SELECT pr.operator, pr.side, pr.result,
         pr.kills, pr.deaths, pr.first_blood
  FROM player_rounds pr
  JOIN match_detail_players mdp 
    ON pr.match_id = mdp.match_id
    AND pr.username = mdp.username
  WHERE mdp.username = (
    SELECT username FROM players WHERE player_id = ?
  )

CALCULATIONS:
  # Min sample threshold
  RELIABLE_OP = rounds >= 15

  # Win rate vs position average
  # Avg attacker win rate ~48%, defender ~52%
  atk_avg = 48.0
  def_avg = 52.0
  op_edge = win_pct - (atk_avg if side == 'attacker' else def_avg)

  # Role consistency
  # Does operator pick match player's identified role?

  # Core pool recommendation
  # Top 3 attack ops by win_pct (min 15 rounds)
  # Top 3 defense ops by win_pct (min 15 rounds)

  # Cut list: ops with negative edge AND 15+ rounds

OUTPUTS:
  top_attack_ops: List[Dict] (top 3, win_pct + rounds)
  top_defense_ops: List[Dict] (top 3)
  cut_list: List[str] (ops hurting win rate)
  most_played_op: str
  highest_win_rate_op: str (min 15 rounds)
  operator_diversity_score: int (unique ops with 10+ rounds)

FINDINGS:
  "Your top 3 attack ops: Ash (58%, 45rnd), 
   Thermite (55%, 38rnd), Nomad (52%, 31rnd)
   Stick to these."

  "Drop Hibana. 31% win rate over 34 rounds.
   You are being countered on hard breach."

  "Your defense pool: Kaid (68%), Warden (65%), 
   Vigil (61%). Strong. Don't change it."
```

---

## V3 PLUGINS (round + match detail data)

---

### V3.1 - Round Win Condition Analyzer
**File:** `src/plugins/v3_round_analysis.py`
**Data:** `player_rounds` + `round_outcomes` tables

```python
def analyze(player_id) -> Dict:

INPUT QUERY:
  SELECT 
    pr.round_id, pr.match_id, pr.side, pr.result,
    pr.operator, pr.kills, pr.deaths,
    pr.first_blood, pr.first_death,
    pr.clutch_won, pr.clutch_lost,
    ro.end_reason, ro.winner_side
  FROM player_rounds pr
  JOIN round_outcomes ro 
    ON pr.match_id = ro.match_id 
    AND pr.round_id = ro.round_id
  WHERE pr.username = (
    SELECT username FROM players WHERE player_id = ?
  )

CALCULATIONS:
  total_rounds = COUNT(*)
  
  # First blood impact
  rounds_where_player_got_fb = COUNT(first_blood=1)
  team_win_rate_when_player_fb = 
    COUNT(first_blood=1 AND team_won) / COUNT(first_blood=1)
  team_win_rate_when_no_fb = 
    COUNT(first_blood=0 AND team_won) / COUNT(first_blood=0)
  fb_win_delta = fb_win_rate - no_fb_win_rate

  # First death impact
  rounds_where_player_died_first = COUNT(first_death=1)
  team_win_rate_when_player_fd =
    COUNT(first_death=1 AND team_won) / COUNT(first_death=1)

  # Round end reasons (from player's team perspective)
  win_by_elimination = COUNT(end_reason='defenders_eliminated' AND won)
                     + COUNT(end_reason='attackers_eliminated' AND won)
  win_by_objective = COUNT(end_reason='bomb_exploded' AND won)
                   + COUNT(end_reason='bomb_defused' AND won)

  # Clutch reality
  clutch_win_rate = clutch_won / (clutch_won + clutch_lost)
  rounds_entered_as_clutch = clutch_won + clutch_lost
  clutch_entry_rate = rounds_entered_as_clutch / total_rounds

  # Side performance
  atk_rounds = COUNT(side='attacker')
  def_rounds = COUNT(side='defender')
  atk_win_rate = COUNT(side='attacker' AND team_won) / atk_rounds
  def_win_rate = COUNT(side='defender' AND team_won) / def_rounds

OUTPUTS:
  total_rounds_analyzed: int
  fb_rate: float (% of rounds you get first blood)
  team_win_rate_with_your_fb: float
  team_win_rate_without_your_fb: float
  fb_impact: float (delta)
  fd_rate: float
  team_win_rate_when_you_die_first: float
  clutch_entry_rate: float
  clutch_win_rate: float
  atk_round_win_rate: float
  def_round_win_rate: float
  primary_win_condition: str ('elimination' or 'objective')

FINDINGS:
  "When you get first blood your team wins 74% of rounds.
   Without your first blood: 48%.
   You are a +26% impact entry fragger.
   Play for first blood every round."

  "You die first in 18% of rounds.
   Your team wins only 31% of those rounds.
   Stop taking early off-angle duels on defense."

  "You enter clutch situations in 12% of rounds.
   You win 21% of them.
   At this rate you are neutral on clutches.
   Don't force 1v3+ - trade and reset instead."
```

---

### V3.2 - Teammate Chemistry Analyzer
**File:** `src/plugins/v3_teammate_chemistry.py`
**Data:** `match_detail_players` table

```python
def analyze(player_id) -> Dict:

# Find all matches for this player
INPUT QUERY:
  SELECT 
    mdp1.match_id,
    mdp1.result as player_result,
    mdp2.username as teammate,
    mdp2.kd_ratio as teammate_kd,
    mdp2.kills as teammate_kills,
    mdp2.rank_points_delta as teammate_rp
  FROM match_detail_players mdp1
  JOIN match_detail_players mdp2 
    ON mdp1.match_id = mdp2.match_id
    AND mdp1.team_id = mdp2.team_id
    AND mdp1.username != mdp2.username
  WHERE mdp1.username = (
    SELECT username FROM players WHERE player_id = ?
  )

CALCULATIONS:
  # Per teammate (min 5 shared matches):
  shared_matches = COUNT(*)
  shared_win_rate = COUNT(player_result='win') / shared_matches
  
  # vs player's solo win rate (matches without that teammate)
  solo_win_rate = player's overall win_rate from stats_snapshots
  chemistry_delta = shared_win_rate - solo_win_rate
  
  # Teammate performance in shared matches
  avg_teammate_kd = AVG(teammate_kd)
  
  # Sort by chemistry_delta (best to worst)

OUTPUTS:
  best_teammate: str (highest chemistry_delta, min 10 games)
  worst_teammate: str (lowest chemistry_delta, min 10 games)
  top_5_teammates: List[Dict]
  chemistry_scores: Dict[username, delta]
  most_played_with: str (most shared matches)

FINDINGS:
  "With KrazyKake5: 61% win rate (113 matches)
   Your solo win rate: 48%
   KrazyKake5 is +13% to your win rate.
   Queue with him whenever possible."

  "With ThirdHawk259126: 39% win rate (91 matches)
   That's -9% vs your solo rate.
   Consider whether this stack is worth it."

  "With Glory2590: 51% win rate (183 matches)
   Roughly neutral. Comfortable but not boosting."
```

---

### V3.3 - Lobby Quality Tracker
**File:** `src/plugins/v3_lobby_quality.py`
**Data:** `match_detail_players` table (enemy team data)

```python
def analyze(player_id) -> Dict:

# Get enemy player stats from shared matches
INPUT QUERY:
  SELECT 
    mdp_me.match_id,
    mdp_me.result,
    mdp_me.rank_points as my_rp,
    AVG(mdp_enemy.rank_points) as enemy_avg_rp,
    AVG(mdp_enemy.kd_ratio) as enemy_avg_kd
  FROM match_detail_players mdp_me
  JOIN match_detail_players mdp_enemy
    ON mdp_me.match_id = mdp_enemy.match_id
    AND mdp_me.team_id != mdp_enemy.team_id
  WHERE mdp_me.username = (
    SELECT username FROM players WHERE player_id = ?
  )
  GROUP BY mdp_me.match_id

CALCULATIONS:
  # RP bracket performance
  # Bin enemy avg RP into brackets
  brackets = [
    (0, 2000, 'low'),
    (2000, 2500, 'silver'),
    (2500, 3000, 'gold'),
    (3000, 3500, 'plat'),
    (3500, 4000, 'emerald'),
    (4000, 9999, 'diamond+')
  ]
  win_rate_per_bracket = {}
  
  # RP mismatch detection
  avg_rp_diff = my_rp - enemy_avg_rp
  # Positive = punching down, Negative = punching up
  
  # Overall lobby avg RP vs player RP
  avg_enemy_rp = AVG(enemy_avg_rp across all matches)

OUTPUTS:
  avg_enemy_rp: float
  rp_mismatch: float (your RP vs avg enemy RP)
  win_rate_by_bracket: Dict[str, float]
  hardest_bracket_played: str
  easiest_win_rate: float
  performance_vs_higher: float (win rate when facing higher RP)
  performance_vs_lower: float (win rate when facing lower RP)

FINDINGS:
  "Average enemy lobby: 3,180 RP
   Your RP: 3,053 — well matched.

   vs <3,000 lobbies: 67% win rate (18 matches)
   vs 3,000-3,500:    48% win rate (31 matches)
   vs 3,500+ lobbies: 31% win rate (11 matches)

   You perform well in your bracket.
   High Emerald lobbies are your ceiling right now."
```

---

### V3.4 - Rank Trajectory Analyzer
**File:** `src/plugins/v3_trajectory.py`
**Data:** `match_detail_players` (rp_delta per match) + `stats_snapshots` (multiple syncs)

```python
def analyze(player_id) -> Dict:

# RP progression over time
INPUT QUERY:
  SELECT match_id, rank_points, rank_points_delta,
         result
  FROM match_detail_players
  WHERE username = (
    SELECT username FROM players WHERE player_id = ?
  )
  ORDER BY match_id  -- proxy for time order

CALCULATIONS:
  # Running RP total reconstruction
  rp_over_time = cumulative sum of rank_points_delta
  
  # Net RP per N matches
  net_per_10 = rp_over_time[-10] - rp_over_time[-20]
  net_per_20 = rp_over_time[0] - rp_over_time[-20]
  
  # Trajectory classification
  if net_per_20 > 100:   trajectory = 'climbing'
  elif net_per_20 > -50: trajectory = 'plateau'
  else:                  trajectory = 'declining'
  
  # Win rate needed to climb
  # Each win ~+25 RP, each loss ~-20 RP
  # Break-even win rate = 20 / (25+20) = 44.4%
  # To climb +100 RP over 20 games need ~55% WR
  
  # Streak analysis
  current_streak = consecutive W or L
  longest_win_streak, longest_loss_streak
  
  # Volatility
  rp_std_dev = standard deviation of rp_delta per match

OUTPUTS:
  trajectory: str ('climbing', 'plateau', 'declining')
  net_rp_last_20: int
  net_rp_last_10: int
  current_streak: int (positive = wins, negative = losses)
  win_rate_last_20: float
  win_rate_needed_to_climb: float (always ~55%)
  estimated_games_to_next_rank: int
  peak_rp_this_season: int
  current_rp: int
  rp_from_peak: int

FINDINGS:
  "Last 20 matches: +47 RP net. Slow climb.
   You need ~55% win rate to climb meaningfully.
   You're at 52% over last 20. On track, just slow."

  "Current streak: 3 losses (-61 RP)
   Your win rate after 3-loss streaks: 29%.
   Stop now. Come back tomorrow."

  "Peak this season: 3,098 RP (Platinum I)
   Current: 3,053 RP (-45 from peak)
   You are in a minor slump, not a hard decline."
```

---

## INTEGRATION INTO MAIN MENU

```
Current menu:
6. View player details

Expand to:
6. View player details
   Shows: season stats, role, top insights (existing)

NEW:
10. Player Analytics
    a. Map Pool Analysis
    b. Session Patterns  
    c. Operator Pool
    d. Round Analysis
    e. Teammate Chemistry
    f. Lobby Quality
    g. Rank Trajectory
    h. Full Report (all of above)

11. Stack Analytics  
    a. Stack Map Pool (combined/intersection)
    b. Stack Chemistry Matrix
    c. Stack vs Enemy Profile
```

---

## BUILD ORDER

```
Phase 1 (V2 - pure SQL, no round data needed):
  1. v2_map_pool.py      ← most actionable, easiest
  2. v2_session.py       ← needs timestamp grouping
  3. v2_operator_pool.py ← needs operator_stats join

Phase 2 (V3 - requires round data):
  4. v3_round_analysis.py     ← most insightful
  5. v3_teammate_chemistry.py ← needs match_detail_players
  6. v3_lobby_quality.py      ← needs enemy team data
  7. v3_trajectory.py         ← needs match history order

Phase 3 (Stack-level):
  8. Stack map pool intersection
  9. Stack chemistry matrix
  10. Enemy network (needs encounters data)
```

---

## DEFINITION OF DONE (v1.0.0)

```
[ ] All 7 plugins implemented
[ ] Each plugin tested with real player data
[ ] Analytics menu wired into main.py
[ ] Full report option generates all 7
[ ] Stack analytics for map pool + chemistry
[ ] 160+ tests passing
[ ] Smoke tested on real player data
```
