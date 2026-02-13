# src/calculator.py

from typing import Dict, Any
import json

class MetricsCalculator:
    """Calculate all derived metrics from raw stats."""

    @staticmethod
    def _safe_get(snapshot: Dict[str, Any], key: str, default: Any = 0) -> Any:
        """Safely get a value from snapshot, handling None."""
        value = snapshot.get(key, default)
        return value if value is not None else default

    def calculate_all(self, snapshot: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate all derived metrics.
        
        Args:
            snapshot: Database row as dictionary
            
        Returns:
            Dictionary of computed metrics
        """
        # Parse clutch data
        clutches_data = self._safe_get(snapshot, 'clutches_data', '{}')
        clutches = json.loads(clutches_data) if clutches_data else {}

        metrics = {}

        # Entry metrics
        metrics['entry_efficiency'] = self.calc_entry_efficiency(
            self._safe_get(snapshot, 'first_bloods'),
            self._safe_get(snapshot, 'first_deaths')
        )

        metrics['aggression_score'] = self.calc_aggression_score(
            self._safe_get(snapshot, 'first_bloods'),
            self._safe_get(snapshot, 'first_deaths'),
            self._safe_get(snapshot, 'rounds_played')
        )
        
        # Clutch metrics
        metrics['clutch_attempt_rate'] = self.calc_clutch_attempt_rate(
            clutches['total'],
            clutches['lost_total'],
            snapshot['rounds_played']
        )
        
        metrics['clutch_1v1_success'] = self.calc_clutch_success(
            clutches.get('1v1', 0),
            clutches.get('lost_1v1', 0)
        )
        
        metrics['clutch_disadvantaged_success'] = self.calc_disadvantaged_clutch_success(clutches)

        metrics['overall_clutch_success'] = self.calc_clutch_success(
            clutches.get('total', 0),
            clutches.get('lost_total', 0)
        )

        # Calculate 1v2 success for dropoff rate
        clutch_1v2_success = self.calc_clutch_success(
            clutches.get('1v2', 0),
            clutches.get('lost_1v2', 0)
        )

        metrics['clutch_dropoff_rate'] = self.calc_clutch_dropoff_rate(
            metrics['clutch_1v1_success'],
            clutch_1v2_success
        )

        metrics['clutch_efficiency_score'] = self.calc_clutch_efficiency_score(clutches)

        # Teamplay
        metrics['teamplay_index'] = self.calc_teamplay_index(
            self._safe_get(snapshot, 'assists'),
            self._safe_get(snapshot, 'kills')
        )

        # Role scores
        metrics['fragger_score'] = self.calc_fragger_score(
            self._safe_get(snapshot, 'kd', 0.0),
            self._safe_get(snapshot, 'kills_per_round', 0.0),
            self._safe_get(snapshot, 'first_bloods'),
            self._safe_get(snapshot, 'rounds_played')
        )

        metrics['entry_score'] = self.calc_entry_score(
            metrics['entry_efficiency'],
            metrics['aggression_score']
        )

        metrics['support_score'] = self.calc_support_score(
            self._safe_get(snapshot, 'assists_per_round', 0.0),
            metrics['teamplay_index']
        )

        metrics['anchor_score'] = self.calc_anchor_score(
            metrics['clutch_attempt_rate'],
            metrics['clutch_1v1_success'],
            self._safe_get(snapshot, 'deaths_per_round', 0.0)
        )

        metrics['clutch_specialist_score'] = self.calc_clutch_specialist_score(
            clutches.get('total', 0),
            self._safe_get(snapshot, 'rounds_played'),
            metrics['clutch_1v1_success'],
            metrics['clutch_disadvantaged_success']
        )

        metrics['carry_score'] = self.calc_carry_score(
            self._safe_get(snapshot, 'kd', 0.0),
            self._safe_get(snapshot, 'match_win_pct', 0.0),
            self._safe_get(snapshot, 'kills_per_round', 0.0),
            metrics['clutch_1v1_success']
        )
        
        # Role classification
        role_info = self.identify_role(metrics)
        metrics['primary_role'] = role_info['primary']
        metrics['primary_confidence'] = role_info['primary_confidence']
        metrics['secondary_role'] = role_info.get('secondary')
        metrics['secondary_confidence'] = role_info.get('secondary_confidence', 0)

        # Additional metrics
        metrics['impact_rating'] = self.calc_impact_rating(
            self._safe_get(snapshot, 'kills'),
            self._safe_get(snapshot, 'assists'),
            clutches.get('total', 0),
            self._safe_get(snapshot, 'rounds_played')
        )

        metrics['wins_per_hour'] = self.calc_wins_per_hour(
            self._safe_get(snapshot, 'wins'),
            self._safe_get(snapshot, 'time_played_hours', 0.0)
        )

        metrics['kd_win_gap'] = self.calc_kd_win_gap(
            self._safe_get(snapshot, 'kd', 0.0),
            self._safe_get(snapshot, 'match_win_pct', 0.0)
        )

        return metrics
    
    # Individual calculation methods
    def calc_entry_efficiency(self, first_bloods: int, first_deaths: int) -> float:
        total = first_bloods + first_deaths
        return first_bloods / total if total > 0 else 0.0
    
    def calc_aggression_score(self, fb: int, fd: int, rounds: int) -> float:
        return (fb + fd) / rounds if rounds > 0 else 0.0
    
    def calc_clutch_attempt_rate(self, clutches: int, lost: int, rounds: int) -> float:
        return (clutches + lost) / rounds if rounds > 0 else 0.0
    
    def calc_clutch_success(self, wins: int, losses: int) -> float:
        total = wins + losses
        return wins / total if total > 0 else 0.0
    
    def calc_disadvantaged_clutch_success(self, clutches: Dict) -> float:
        wins = sum([
            clutches.get('1v2', 0),
            clutches.get('1v3', 0),
            clutches.get('1v4', 0),
            clutches.get('1v5', 0)
        ])
        
        losses = sum([
            clutches.get('lost_1v2', 0),
            clutches.get('lost_1v3', 0),
            clutches.get('lost_1v4', 0),
            clutches.get('lost_1v5', 0)
        ])
        
        total = wins + losses
        return wins / total if total > 0 else 0.0
    
    def calc_teamplay_index(self, assists: int, kills: int) -> float:
        total = assists + kills
        return assists / total if total > 0 else 0.0
    
    def calc_fragger_score(self, kd: float, kpr: float, fb: int, rounds: int) -> float:
        fb_rate = fb / rounds if rounds > 0 else 0
        return (kd * 30) + (kpr * 40) + (fb_rate * 300)
    
    def calc_entry_score(self, entry_eff: float, aggr: float) -> float:
        return (entry_eff * 40) + (aggr * 60)
    
    def calc_support_score(self, apr: float, teamplay: float) -> float:
        return (apr * 150) + (teamplay * 50)
    
    def calc_anchor_score(self, clutch_rate: float, clutch_1v1: float, dpr: float) -> float:
        return (clutch_rate * 40) + (clutch_1v1 * 40) + ((1 - dpr) * 20)
    
    def calc_clutch_specialist_score(self, clutches: int, rounds: int, 
                                     clutch_1v1: float, clutch_disadv: float) -> float:
        clutch_per_round = clutches / rounds if rounds > 0 else 0
        return (clutch_per_round * 100) + (clutch_1v1 * 30) + (clutch_disadv * 70)
    
    def calc_carry_score(self, kd: float, win_pct: float, kpr: float, clutch: float) -> float:
        return (kd * 25) + (win_pct * 0.3) + (kpr * 30) + (clutch * 20)

    def calc_clutch_dropoff_rate(self, clutch_1v1: float, clutch_1v2: float) -> float:
        """Calculate how much clutch success drops from 1v1 to 1v2."""
        return clutch_1v1 - clutch_1v2

    def calc_clutch_efficiency_score(self, clutches: Dict) -> float:
        """Calculate weighted clutch efficiency based on difficulty."""
        return (
            (clutches.get('1v1', 0) * 1) +
            (clutches.get('1v2', 0) * 2) +
            (clutches.get('1v3', 0) * 5) +
            (clutches.get('1v4', 0) * 10) +
            (clutches.get('1v5', 0) * 25)
        )

    def calc_impact_rating(self, kills: int, assists: int, clutch_wins: int, rounds: int) -> float:
        """Calculate overall impact per round."""
        if rounds == 0:
            return 0.0
        impact = (kills * 1.0) + (assists * 0.5) + (clutch_wins * 2.0)
        return impact / rounds

    def calc_wins_per_hour(self, wins: int, hours: float) -> float:
        """Calculate win rate per hour of playtime."""
        return wins / hours if hours > 0 else 0.0

    def calc_kd_win_gap(self, kd: float, win_pct: float) -> float:
        """Calculate gap between K/D and expected K/D based on win rate."""
        # Normalize win% to same scale as K/D (roughly)
        expected_kd = win_pct / 50  # 50% win rate = 1.0 K/D
        return kd - expected_kd

    def identify_role(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """Identify primary and secondary roles based on scores."""
        scores = {
            'Fragger': metrics['fragger_score'],
            'Entry': metrics['entry_score'],
            'Support': metrics['support_score'],
            'Anchor': metrics['anchor_score'],
            'Clutch': metrics['clutch_specialist_score'],
            'Carry': metrics['carry_score']
        }
        
        # Sort by score
        sorted_roles = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        primary = sorted_roles[0][0]
        primary_conf = sorted_roles[0][1]
        
        secondary = sorted_roles[1][0] if len(sorted_roles) > 1 else None
        secondary_conf = sorted_roles[1][1] if len(sorted_roles) > 1 else 0
        
        return {
            'primary': primary,
            'primary_confidence': primary_conf,
            'secondary': secondary if secondary_conf > 30 else None,
            'secondary_confidence': secondary_conf if secondary_conf > 30 else 0
        }