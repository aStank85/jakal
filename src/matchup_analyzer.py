# src/matchup_analyzer.py

from typing import Dict, List, Any
from src.database import Database
from src.team_analyzer import TeamAnalyzer
import json


class MatchupAnalyzer:
    """5v5 head-to-head matchup analysis."""

    CATEGORY_WEIGHTS = {
        'kd': 0.20,
        'entry': 0.20,
        'clutch': 0.15,
        'support': 0.10,
        'hs_pct': 0.15,
        'win_rate': 0.20,
    }

    def __init__(self, db: Database):
        self.db = db
        self.team_analyzer = TeamAnalyzer(db)

    def analyze_matchup(self, stack_a_id: int, stack_b_id: int) -> Dict[str, Any]:
        """Main entry point - analyze a 5v5 matchup between two stacks."""
        analysis_a = self.team_analyzer.analyze_stack(stack_a_id)
        analysis_b = self.team_analyzer.analyze_stack(stack_b_id)

        category_comparisons = self.compare_all_categories(analysis_a, analysis_b)
        role_matchups = self.analyze_role_matchups(analysis_a['members'], analysis_b['members'])
        prediction = self.predict_outcome(category_comparisons)
        recommendations = self.generate_recommendations(analysis_a, analysis_b, category_comparisons)
        battlegrounds = self.identify_key_battlegrounds(category_comparisons)

        matchup = {
            'stack_a_id': stack_a_id,
            'stack_b_id': stack_b_id,
            'stack_a': analysis_a['stack'],
            'stack_b': analysis_b['stack'],
            'analysis_a': analysis_a,
            'analysis_b': analysis_b,
            'category_comparisons': category_comparisons,
            'role_matchups': role_matchups,
            'predicted_winner': prediction['predicted_winner'],
            'confidence': prediction['confidence'],
            'reasoning': prediction['reasoning'],
            'recommendations': recommendations,
            'key_battlegrounds': battlegrounds,
            # Flatten advantages for DB storage
            'kd_advantage': category_comparisons.get('kd'),
            'entry_advantage': category_comparisons.get('entry'),
            'clutch_advantage': category_comparisons.get('clutch'),
            'support_advantage': category_comparisons.get('support'),
            'hs_advantage': category_comparisons.get('hs_pct'),
            'win_rate_advantage': category_comparisons.get('win_rate'),
        }

        self.db.save_matchup_analysis(matchup)
        return matchup

    # --- Category comparisons ---

    def compare_category(self, stack_a_value: float, stack_b_value: float, category: str) -> Dict[str, Any]:
        """Compare a single stat category between two stacks."""
        # Guard against None values
        a_val = stack_a_value or 0
        b_val = stack_b_value or 0

        diff = a_val - b_val
        abs_diff = abs(diff)

        if abs_diff < 0.02:
            winner = 'Even'
        elif diff > 0:
            winner = 'A'
        else:
            winner = 'B'

        if abs_diff > 0.15:
            significance = 'large'
        elif abs_diff > 0.05:
            significance = 'medium'
        else:
            significance = 'small'

        return {
            'category': category,
            'value_a': round(a_val, 3),
            'value_b': round(b_val, 3),
            'winner': winner,
            'margin': round(abs_diff, 3),
            'significance': significance
        }

    def compare_all_categories(self, analysis_a: Dict, analysis_b: Dict) -> Dict[str, Dict]:
        """Compare all stat categories."""
        comparisons = {}

        comparisons['kd'] = self.compare_category(
            analysis_a['team_avg_kd'],
            analysis_b['team_avg_kd'],
            'K/D'
        )

        comparisons['entry'] = self.compare_category(
            analysis_a['team_entry_efficiency'],
            analysis_b['team_entry_efficiency'],
            'Entry Eff.'
        )

        comparisons['clutch'] = self.compare_category(
            analysis_a['team_1v1_success'],
            analysis_b['team_1v1_success'],
            'Clutch 1v1'
        )

        comparisons['support'] = self.compare_category(
            analysis_a['team_avg_apr'],
            analysis_b['team_avg_apr'],
            'Teamplay'
        )

        # HS% is in percentage points (e.g. 54.2), need to normalize for comparison
        hs_a = analysis_a['team_avg_hs_pct'] or 0
        hs_b = analysis_b['team_avg_hs_pct'] or 0
        comparisons['hs_pct'] = self.compare_category(
            hs_a / 100 if hs_a > 1 else hs_a,
            hs_b / 100 if hs_b > 1 else hs_b,
            'HS %'
        )

        win_a = analysis_a['team_avg_win_pct'] or 0
        win_b = analysis_b['team_avg_win_pct'] or 0
        comparisons['win_rate'] = self.compare_category(
            win_a / 100 if win_a > 1 else win_a,
            win_b / 100 if win_b > 1 else win_b,
            'Win Rate'
        )

        return comparisons

    # --- Role matchups ---

    def analyze_role_matchups(self, members_a: List[Dict], members_b: List[Dict]) -> List[Dict]:
        """Match each role against opponent's same role."""
        # Build role lookup for each team
        roles_a = {}
        for m in members_a:
            role = m['role']
            if role not in roles_a:
                roles_a[role] = m

        roles_b = {}
        for m in members_b:
            role = m['role']
            if role not in roles_b:
                roles_b[role] = m

        matchups = []
        all_roles = set(list(roles_a.keys()) + list(roles_b.keys()))

        for role in sorted(all_roles):
            player_a = roles_a.get(role)
            player_b = roles_b.get(role)

            if player_a and player_b:
                score_a = (player_a['metrics'].get('carry_score') or 0)
                score_b = (player_b['metrics'].get('carry_score') or 0)

                diff = score_a - score_b
                if abs(diff) < 2:
                    advantage = 'even'
                elif diff > 0:
                    advantage = 'yours'
                else:
                    advantage = 'theirs'

                matchups.append({
                    'role': role,
                    'your_player': player_a['username'],
                    'their_player': player_b['username'],
                    'advantage': advantage,
                    'your_value': round(score_a, 1),
                    'their_value': round(score_b, 1)
                })
            elif player_a:
                matchups.append({
                    'role': role,
                    'your_player': player_a['username'],
                    'their_player': '-',
                    'advantage': 'yours',
                    'your_value': round((player_a['metrics'].get('carry_score') or 0), 1),
                    'their_value': 0
                })
            elif player_b:
                matchups.append({
                    'role': role,
                    'your_player': '-',
                    'their_player': player_b['username'],
                    'advantage': 'theirs',
                    'your_value': 0,
                    'their_value': round((player_b['metrics'].get('carry_score') or 0), 1)
                })

        return matchups

    # --- Prediction ---

    def predict_outcome(self, category_comparisons: Dict) -> Dict[str, Any]:
        """Predict match outcome based on weighted category advantages."""
        weighted_score = 0.0
        total_weight = 0.0
        a_wins = 0
        b_wins = 0

        for cat_key, comparison in category_comparisons.items():
            weight = self.CATEGORY_WEIGHTS.get(cat_key, 0.1)
            total_weight += weight

            if comparison['winner'] == 'A':
                weighted_score += weight * comparison['margin']
                a_wins += 1
            elif comparison['winner'] == 'B':
                weighted_score -= weight * comparison['margin']
                b_wins += 1

        # Convert to confidence percentage
        if total_weight == 0:
            confidence = 50.0
        else:
            # Normalize: small margins -> close to 50%, large -> further out
            raw_score = weighted_score / total_weight
            confidence = 50 + (raw_score * 200)  # Scale factor
            confidence = max(30, min(70, confidence))  # Cap at 30-70%

        if confidence > 52:
            predicted_winner = 'A'
            reasoning = f"Stack A leads in {a_wins} of {len(category_comparisons)} categories"
        elif confidence < 48:
            predicted_winner = 'B'
            confidence = 100 - confidence
            reasoning = f"Stack B leads in {b_wins} of {len(category_comparisons)} categories"
        else:
            predicted_winner = 'Even'
            confidence = 50.0
            reasoning = f"Too close to call - {a_wins} vs {b_wins} category advantages"

        return {
            'predicted_winner': predicted_winner,
            'confidence': round(confidence, 1),
            'reasoning': reasoning
        }

    # --- Strategy ---

    def generate_recommendations(self, analysis_a: Dict, analysis_b: Dict,
                                  category_comparisons: Dict) -> List[str]:
        """Generate strategic recommendations for Stack A."""
        recs = []

        # Entry comparison
        entry = category_comparisons.get('entry', {})
        if entry.get('winner') == 'B':
            recs.append("Their entry is stronger - play passive early and hold angles")
        elif entry.get('winner') == 'A':
            recs.append("Win entry duels - your entry player has the edge")

        # Clutch comparison
        clutch = category_comparisons.get('clutch', {})
        if clutch.get('winner') == 'A':
            recs.append("Play for late round - your clutch game is stronger")
        elif clutch.get('winner') == 'B':
            recs.append("Avoid 1v1 situations - their clutch game is stronger")

        # KD comparison
        kd = category_comparisons.get('kd', {})
        if kd.get('winner') == 'B':
            recs.append("Watch their K/D - individually stronger fraggers")
        elif kd.get('winner') == 'A':
            recs.append("Press your fragging advantage - take early gunfights")

        # Win rate comparison
        wr = category_comparisons.get('win_rate', {})
        if wr.get('winner') == 'A' and kd.get('winner') == 'B':
            recs.append("Force teamfights - your win rate suggests better team coordination despite lower individual stats")
        elif wr.get('winner') == 'B':
            recs.append("They have the higher win rate - disrupt their coordination with aggressive play")

        # Support comparison
        support = category_comparisons.get('support', {})
        if support.get('winner') == 'B':
            recs.append("Their teamplay is stronger - counter by splitting their utility usage")
        elif support.get('winner') == 'A':
            recs.append("Your support game is an advantage - coordinate utility for site takes")

        if not recs:
            recs.append("Even matchup - focus on fundamentals and communication")

        return recs

    def identify_key_battlegrounds(self, category_comparisons: Dict) -> List[str]:
        """Identify categories where the match will be decided (closest margins)."""
        sorted_cats = sorted(
            category_comparisons.items(),
            key=lambda x: x[1]['margin']
        )

        battlegrounds = []
        for cat_key, comp in sorted_cats[:3]:  # Top 3 closest categories
            label = comp['category']
            if comp['winner'] == 'Even':
                battlegrounds.append(f"{label} - dead even, could swing either way")
            else:
                battlegrounds.append(f"{label} - closest margin, decides {label.lower()} control")

        return battlegrounds
