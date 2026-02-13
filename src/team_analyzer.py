# src/team_analyzer.py

from typing import Dict, List, Any
from src.database import Database
from src.calculator import MetricsCalculator
import json


class TeamAnalyzer:
    """Team-level analysis engine for stacks."""

    ALL_ROLES = ['Fragger', 'Entry', 'Support', 'Anchor', 'Clutch', 'Carry']

    def __init__(self, db: Database):
        self.db = db
        self.calculator = MetricsCalculator()

    def analyze_stack(self, stack_id: int) -> Dict[str, Any]:
        """Main entry point - analyze a complete stack."""
        stack = self.db.get_stack(stack_id)
        if not stack:
            raise ValueError(f"Stack {stack_id} not found")

        members_raw = self.db.get_stack_members(stack_id)
        if len(members_raw) < 2:
            raise ValueError("Stack needs at least 2 members for analysis")

        # Build enriched member list with snapshots and metrics
        members = []
        data_quality_warnings = []
        for m in members_raw:
            snapshot = self.db.get_latest_snapshot(m['username'])
            if snapshot is None:
                raise ValueError(f"No stats found for player '{m['username']}'")
            metrics = self.calculator.calculate_all(snapshot)
            defaulted_fields = metrics.get('_defaulted_snapshot_fields', [])
            if defaulted_fields:
                data_quality_warnings.append({
                    'username': m['username'],
                    'fields': defaulted_fields,
                    'message': (
                        f"{m['username']}: defaulted missing/invalid fields to 0 "
                        f"({', '.join(defaulted_fields)})"
                    )
                })
            role = m['role_override'] if m['role_override'] else metrics['primary_role']
            members.append({
                'username': m['username'],
                'player_id': m['player_id'],
                'role_override': m['role_override'],
                'snapshot': snapshot,
                'metrics': metrics,
                'role': role,
                'defaulted_snapshot_fields': defaulted_fields
            })

        # Compute all analysis components
        role_distribution = self.calculate_role_distribution(members)
        roles_covered = list(role_distribution.keys())
        roles_missing = self.identify_missing_roles(role_distribution)
        composition_score = self.calculate_composition_score(role_distribution)
        team_avgs = self.calculate_team_averages(members)
        entry_players = self.identify_entry_players(members)
        entry_stats = self.calculate_team_entry_stats(members)
        clutch_hierarchy = self.identify_clutch_hierarchy(members)
        clutch_gap = self.calculate_clutch_gap(members)
        carry_player = self.identify_carry_player(members)
        carry_dependency = self.calculate_carry_dependency(members)

        primary_clutch_player = clutch_hierarchy[0]['username'] if clutch_hierarchy else None

        analysis = {
            'stack': stack,
            'members': members,
            'role_distribution': role_distribution,
            'roles_covered': roles_covered,
            'roles_missing': roles_missing,
            'composition_score': composition_score,
            'team_avg_kd': team_avgs['kd'],
            'team_avg_win_pct': team_avgs['win_pct'],
            'team_avg_hs_pct': team_avgs['hs_pct'],
            'team_avg_kpr': team_avgs['kpr'],
            'team_avg_apr': team_avgs['apr'],
            'team_entry_efficiency': entry_stats['team_entry_efficiency'],
            'team_first_blood_rate': entry_stats['team_first_blood_rate'],
            'dedicated_entry_count': entry_stats['dedicated_entry_count'],
            'team_clutch_success': team_avgs.get('clutch_1v1', 0),
            'team_1v1_success': team_avgs.get('clutch_1v1', 0),
            'primary_clutch_player': primary_clutch_player,
            'clutch_gap': clutch_gap,
            'carry_player': carry_player,
            'carry_dependency': carry_dependency,
            'clutch_hierarchy': clutch_hierarchy,
            'entry_players': entry_players,
            'data_quality_warnings': data_quality_warnings,
        }

        # Generate insights, strengths, weaknesses
        analysis['team_insights'] = self.generate_team_insights(analysis)
        analysis['team_strengths'] = self.identify_team_strengths(analysis)
        analysis['team_weaknesses'] = self.identify_team_weaknesses(analysis)

        # Save to database
        self.db.save_stack_analysis(stack_id, analysis)

        return analysis

    # --- Composition ---

    def calculate_role_distribution(self, members: List[Dict]) -> Dict[str, int]:
        """Count how many players fill each role."""
        dist = {}
        for m in members:
            role = m['role']
            dist[role] = dist.get(role, 0) + 1
        return dist

    def identify_missing_roles(self, role_distribution: Dict[str, int]) -> List[str]:
        """Identify which standard roles are not covered."""
        return [r for r in self.ALL_ROLES if r not in role_distribution]

    def calculate_composition_score(self, role_distribution: Dict[str, int]) -> float:
        """Score team composition 0-100.
        100 = perfect balance (one of each role for 5 players).
        Penalizes duplicate roles and missing roles.
        """
        total_players = sum(role_distribution.values())
        if total_players == 0:
            return 0.0

        unique_roles = len(role_distribution)
        max_possible = min(total_players, len(self.ALL_ROLES))

        # Base score from role coverage
        coverage_score = (unique_roles / max_possible) * 60

        # Penalty for duplicates
        duplicates = sum(max(0, count - 1) for count in role_distribution.values())
        duplicate_penalty = duplicates * 10

        # Bonus for covering critical roles (Entry, Support, Fragger)
        critical_roles = ['Entry', 'Support', 'Fragger']
        critical_covered = sum(1 for r in critical_roles if r in role_distribution)
        critical_bonus = (critical_covered / len(critical_roles)) * 40

        score = coverage_score + critical_bonus - duplicate_penalty
        return max(0.0, min(100.0, round(score, 1)))

    # --- Team combat ---

    def calculate_team_averages(self, members: List[Dict]) -> Dict[str, float]:
        """Calculate team average stats."""
        n = len(members)
        if n == 0:
            return {'kd': 0, 'win_pct': 0, 'hs_pct': 0, 'kpr': 0, 'apr': 0, 'clutch_1v1': 0}

        totals = {'kd': 0, 'win_pct': 0, 'hs_pct': 0, 'kpr': 0, 'apr': 0, 'clutch_1v1': 0}
        for m in members:
            s = m['snapshot']
            met = m['metrics']
            totals['kd'] += s.get('kd', 0) or 0
            totals['win_pct'] += s.get('match_win_pct', 0) or 0
            totals['hs_pct'] += s.get('hs_pct', 0) or 0
            totals['kpr'] += s.get('kills_per_round', 0) or 0
            totals['apr'] += s.get('assists_per_round', 0) or 0
            totals['clutch_1v1'] += met.get('clutch_1v1_success', 0) or 0

        return {k: round(v / n, 3) for k, v in totals.items()}

    # --- Entry analysis ---

    def identify_entry_players(self, members: List[Dict]) -> List[str]:
        """Identify players who specialize in entry fragging (entry_efficiency > 0.55)."""
        return [
            m['username'] for m in members
            if (m['metrics'].get('entry_efficiency') or 0) > 0.55
        ]

    def calculate_team_entry_stats(self, members: List[Dict]) -> Dict[str, float]:
        """Calculate team-level entry stats."""
        n = len(members)
        if n == 0:
            return {'team_entry_efficiency': 0, 'team_first_blood_rate': 0, 'dedicated_entry_count': 0}

        total_ee = sum((m['metrics'].get('entry_efficiency') or 0) for m in members)
        total_rounds = sum((m['snapshot'].get('rounds_played', 0) or 0) for m in members)
        total_fb = sum((m['snapshot'].get('first_bloods', 0) or 0) for m in members)

        team_ee = total_ee / n
        team_fb_rate = total_fb / total_rounds if total_rounds > 0 else 0
        dedicated = sum(1 for m in members if (m['metrics'].get('entry_efficiency') or 0) > 0.55)

        return {
            'team_entry_efficiency': round(team_ee, 3),
            'team_first_blood_rate': round(team_fb_rate, 3),
            'dedicated_entry_count': dedicated
        }

    # --- Clutch analysis ---

    def identify_clutch_hierarchy(self, members: List[Dict]) -> List[Dict]:
        """Rank members by clutch ability."""
        ranked = sorted(
            members,
            key=lambda m: (m['metrics'].get('clutch_1v1_success') or 0),
            reverse=True
        )
        return [
            {
                'username': m['username'],
                'clutch_1v1': (m['metrics'].get('clutch_1v1_success') or 0),
                'clutch_disadv': (m['metrics'].get('clutch_disadvantaged_success') or 0),
                'rank': i + 1
            }
            for i, m in enumerate(ranked)
        ]

    def calculate_clutch_gap(self, members: List[Dict]) -> float:
        """Difference between best and worst clutch players."""
        if len(members) < 2:
            return 0.0
        clutch_vals = [(m['metrics'].get('clutch_1v1_success') or 0) for m in members]
        return round(max(clutch_vals) - min(clutch_vals), 3)

    # --- Carry analysis ---

    def identify_carry_player(self, members: List[Dict]) -> str:
        """Identify the highest-impact player based on carry score."""
        if not members:
            return ""
        best = max(members, key=lambda m: (m['metrics'].get('carry_score') or 0))
        return best['username']

    def calculate_carry_dependency(self, members: List[Dict]) -> float:
        """Calculate how dependent the team is on one player.
        0 = balanced, 100 = single carry.
        Based on how much the top player's carry_score exceeds the team average.
        """
        if len(members) < 2:
            return 0.0

        scores = [(m['metrics'].get('carry_score') or 0) for m in members]
        avg = sum(scores) / len(scores)
        max_score = max(scores)

        if avg == 0:
            return 0.0

        # How much does the carry exceed the average, as a percentage
        dependency = ((max_score - avg) / avg) * 100
        return max(0.0, min(100.0, round(dependency, 1)))

    # --- Insight generation ---

    def generate_team_insights(self, analysis: Dict) -> List[Dict]:
        """Generate team insights based on analysis data."""
        insights = []

        role_dist = analysis['role_distribution']
        roles_covered = analysis['roles_covered']
        composition_score = analysis['composition_score']
        carry_dep = analysis['carry_dependency']
        carry_player = analysis['carry_player']
        team_ee = analysis['team_entry_efficiency']
        ded_entry = analysis['dedicated_entry_count']
        clutch_gap = analysis['clutch_gap']
        primary_clutch = analysis['primary_clutch_player']
        team_1v1 = analysis['team_1v1_success']
        team_win_pct = analysis['team_avg_win_pct']

        # Composition insights
        fraggers = role_dist.get('Fragger', 0)
        if fraggers >= 3:
            insights.append({
                'severity': 'warning',
                'category': 'composition',
                'message': f"{fraggers} Fraggers - nobody is playing support",
                'evidence': f"Role distribution: {role_dist}",
                'action': "Have at least one player adapt to a Support or Entry role"
            })

        if 'Entry' not in roles_covered:
            insights.append({
                'severity': 'warning',
                'category': 'composition',
                'message': "No dedicated entry player - team may struggle opening sites",
                'evidence': f"Roles covered: {roles_covered}",
                'action': "Assign the most aggressive player to Entry role"
            })

        if 'Support' not in roles_covered:
            insights.append({
                'severity': 'warning',
                'category': 'composition',
                'message': "No support player - team playing without utility coordination",
                'evidence': f"Roles covered: {roles_covered}",
                'action': "Assign the highest teamplay-index player to Support"
            })

        if composition_score == 100:
            insights.append({
                'severity': 'positive',
                'category': 'composition',
                'message': "Perfect role balance across all positions",
                'evidence': f"Composition score: {composition_score}/100",
                'action': "Maintain current role assignments"
            })

        # Check for duplicate roles
        for role, count in role_dist.items():
            if count >= 2:
                insights.append({
                    'severity': 'warning',
                    'category': 'composition',
                    'message': f"{count} players classified as {role}. Consider one adapting to a different role",
                    'evidence': f"{role}: {count} players",
                    'action': f"Have one {role} player adapt to fill a missing role"
                })

        # Carry dependency
        if (carry_dep or 0) > 70:
            insights.append({
                'severity': 'warning',
                'category': 'carry',
                'message': f"{carry_player} is carrying too much load. Team win rate will drop significantly if they underperform",
                'evidence': f"Carry dependency: {carry_dep:.0f}%",
                'action': "Develop secondary carry options"
            })

        if (carry_dep or 0) < 30:
            insights.append({
                'severity': 'positive',
                'category': 'carry',
                'message': "Well-balanced team - no single player is a liability",
                'evidence': f"Carry dependency: {carry_dep:.0f}%",
                'action': "Maintain balanced contribution"
            })

        # Entry insights
        if (team_ee or 0) > 0.58:
            insights.append({
                'severity': 'positive',
                'category': 'entry',
                'message': "Team wins opening duels consistently",
                'evidence': f"Team entry efficiency: {team_ee:.1%}",
                'action': "Keep aggressive entry approach"
            })

        if ded_entry == 0:
            insights.append({
                'severity': 'warning',
                'category': 'entry',
                'message': "No player specializes in entry fragging",
                'evidence': f"0 players with entry efficiency > 55%",
                'action': "Designate an entry player for better site openings"
            })

        # Clutch insights
        if (clutch_gap or 0) > 0.35:
            insights.append({
                'severity': 'warning',
                'category': 'clutch',
                'message': f"Huge clutch gap - team is {clutch_gap*100:.0f}% worse at clutches without {primary_clutch}",
                'evidence': f"Clutch gap: {clutch_gap:.2f}",
                'action': "Develop clutch ability across more players"
            })

        if (team_1v1 or 0) > 0.65:
            insights.append({
                'severity': 'positive',
                'category': 'clutch',
                'message': "Team is elite at 1v1 clutches overall",
                'evidence': f"Team 1v1 success: {team_1v1:.1%}",
                'action': "Play for late-round advantages"
            })

        # Win rate insights
        if (team_win_pct or 0) < 47:
            insights.append({
                'severity': 'warning',
                'category': 'win_rate',
                'message': "Team win rate below 47% - something systematic is wrong",
                'evidence': f"Team avg win rate: {team_win_pct:.1f}%",
                'action': "Review team strategy and communication"
            })

        if (team_win_pct or 0) > 55:
            insights.append({
                'severity': 'positive',
                'category': 'win_rate',
                'message': "Team winning more than expected based on individual stats",
                'evidence': f"Team avg win rate: {team_win_pct:.1f}%",
                'action': "Strong teamwork - maintain current coordination"
            })

        return insights

    # --- Strengths and weaknesses ---

    def identify_team_strengths(self, analysis: Dict) -> List[str]:
        """Extract strengths from analysis."""
        strengths = []

        if (analysis['team_entry_efficiency'] or 0) > 0.55:
            strengths.append(f"Strong entry game ({analysis['team_entry_efficiency']:.0%} efficiency)")

        if (analysis['team_1v1_success'] or 0) > 0.60:
            strengths.append(f"Elite clutch coverage ({analysis['team_1v1_success']:.0%} 1v1)")

        if (analysis['carry_dependency'] or 0) < 35:
            strengths.append("Balanced contribution (no carry dependency)")

        if (analysis['composition_score'] or 0) >= 80:
            strengths.append(f"Good role composition ({analysis['composition_score']:.0f}/100)")

        if (analysis['team_avg_kd'] or 0) > 1.1:
            strengths.append(f"Strong fragging power (team avg K/D {analysis['team_avg_kd']:.2f})")

        if (analysis['team_avg_win_pct'] or 0) > 53:
            strengths.append(f"High win rate ({analysis['team_avg_win_pct']:.1f}%)")

        if (analysis['team_avg_hs_pct'] or 0) > 50:
            strengths.append(f"Excellent aim (team avg HS {analysis['team_avg_hs_pct']:.0f}%)")

        return strengths

    def identify_team_weaknesses(self, analysis: Dict) -> List[str]:
        """Extract weaknesses from analysis."""
        weaknesses = []

        if (analysis['team_entry_efficiency'] or 0) < 0.48:
            weaknesses.append(f"Weak entry game ({analysis['team_entry_efficiency']:.0%} efficiency)")

        if analysis['dedicated_entry_count'] == 0:
            weaknesses.append("No dedicated entry player")

        if (analysis['carry_dependency'] or 0) > 60:
            weaknesses.append(f"Carry dependent on {analysis['carry_player']}")

        if (analysis['composition_score'] or 0) < 50:
            weaknesses.append(f"Poor role composition ({analysis['composition_score']:.0f}/100)")

        if len(analysis['roles_missing']) >= 3:
            weaknesses.append(f"Missing {len(analysis['roles_missing'])} roles: {', '.join(analysis['roles_missing'])}")

        if (analysis['team_avg_kd'] or 0) < 0.95:
            weaknesses.append(f"Low fragging power (team avg K/D {analysis['team_avg_kd']:.2f})")

        if (analysis['team_avg_win_pct'] or 0) < 48:
            weaknesses.append(f"Below average win rate ({analysis['team_avg_win_pct']:.1f}%)")

        if (analysis['clutch_gap'] or 0) > 0.35:
            weaknesses.append(f"Clutch gap too large ({analysis['clutch_gap']:.0%})")

        return weaknesses
