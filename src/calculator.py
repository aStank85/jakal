# src/calculator.py

from typing import Dict, Any
import json

class MetricsCalculator:
    """Calculate all derived metrics from raw stats."""

    SNAPSHOT_NUMERIC_FIELDS = (
        'first_bloods',
        'first_deaths',
        'rounds_played',
        'assists',
        'kills',
        'kd',
        'kills_per_round',
        'assists_per_round',
        'deaths_per_round',
        'match_win_pct',
    )

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        """Coerce nullable/dirty numeric inputs to a float default."""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def get_defaulted_snapshot_fields(self, snapshot: Dict[str, Any]) -> list[str]:
        """Report numeric snapshot fields that will be defaulted to 0 in calculations."""
        defaulted = []
        for field in self.SNAPSHOT_NUMERIC_FIELDS:
            value = snapshot.get(field)
            if value is None:
                defaulted.append(field)
                continue
            try:
                float(value)
            except (TypeError, ValueError):
                defaulted.append(field)
        return defaulted
    
    def calculate_all(self, snapshot: Dict[str, Any]) -> Dict[str, float]:
        """
        Calculate all derived metrics.
        
        Args:
            snapshot: Database row as dictionary
            
        Returns:
            Dictionary of computed metrics
        """
        # Parse clutch data
        clutches = json.loads(snapshot.get('clutches_data') or '{}')
        defaulted_fields = self.get_defaulted_snapshot_fields(snapshot)
        
        metrics = {}
        
        # Entry metrics
        metrics['entry_efficiency'] = self.calc_entry_efficiency(
            snapshot.get('first_bloods'),
            snapshot.get('first_deaths')
        )
        
        metrics['aggression_score'] = self.calc_aggression_score(
            snapshot.get('first_bloods'),
            snapshot.get('first_deaths'),
            snapshot.get('rounds_played')
        )
        
        # Clutch metrics
        metrics['clutch_attempt_rate'] = self.calc_clutch_attempt_rate(
            clutches.get('total', 0),
            clutches.get('lost_total', 0),
            snapshot.get('rounds_played')
        )
        
        metrics['clutch_1v1_success'] = self.calc_clutch_success(
            clutches.get('1v1', 0),
            clutches.get('lost_1v1', 0)
        )
        
        metrics['clutch_disadvantaged_success'] = self.calc_disadvantaged_clutch_success(clutches)
        
        # Teamplay
        metrics['teamplay_index'] = self.calc_teamplay_index(
            snapshot.get('assists'),
            snapshot.get('kills')
        )
        
        # Role scores
        metrics['fragger_score'] = self.calc_fragger_score(
            snapshot.get('kd'),
            snapshot.get('kills_per_round'),
            snapshot.get('first_bloods'),
            snapshot.get('rounds_played')
        )
        
        metrics['entry_score'] = self.calc_entry_score(
            metrics['entry_efficiency'],
            metrics['aggression_score']
        )
        
        metrics['support_score'] = self.calc_support_score(
            snapshot.get('assists_per_round'),
            metrics['teamplay_index']
        )
        
        metrics['anchor_score'] = self.calc_anchor_score(
            metrics['clutch_attempt_rate'],
            metrics['clutch_1v1_success'],
            snapshot.get('deaths_per_round')
        )
        
        metrics['clutch_specialist_score'] = self.calc_clutch_specialist_score(
            clutches.get('total', 0),
            snapshot.get('rounds_played'),
            metrics['clutch_1v1_success'],
            metrics['clutch_disadvantaged_success']
        )
        
        metrics['carry_score'] = self.calc_carry_score(
            snapshot.get('kd'),
            snapshot.get('match_win_pct'),
            snapshot.get('kills_per_round'),
            metrics['clutch_1v1_success']
        )
        
        # Overall clutch metrics
        clutches_won = self._num(clutches.get('total', 0))
        clutches_lost = self._num(clutches.get('lost_total', 0))
        clutch_attempts = int(clutches_won + clutches_lost)
        metrics['overall_clutch_success'] = (
            clutches_won / clutch_attempts if clutch_attempts > 0 else 0.0
        )
        metrics['clutch_attempts'] = clutch_attempts

        # Disadvantaged clutch breakdown
        disadv_wins = sum([
            self._num(clutches.get('1v2', 0)),
            self._num(clutches.get('1v3', 0)),
            self._num(clutches.get('1v4', 0)),
            self._num(clutches.get('1v5', 0)),
        ])
        disadv_losses = sum([
            self._num(clutches.get('lost_1v2', 0)),
            self._num(clutches.get('lost_1v3', 0)),
            self._num(clutches.get('lost_1v4', 0)),
            self._num(clutches.get('lost_1v5', 0)),
        ])
        disadv_attempts = int(disadv_wins + disadv_losses)
        metrics['disadv_attempts'] = disadv_attempts
        metrics['disadv_attempt_share'] = (
            disadv_attempts / clutch_attempts if clutch_attempts > 0 else 0.0
        )

        # Extreme clutch (1v4 / 1v5)
        extreme_wins = self._num(clutches.get('1v4', 0)) + self._num(clutches.get('1v5', 0))
        extreme_losses = self._num(clutches.get('lost_1v4', 0)) + self._num(clutches.get('lost_1v5', 0))
        metrics['extreme_attempts'] = int(extreme_wins + extreme_losses)

        # High-pressure clutch (1v3+)
        rounds_played = self._num(snapshot.get('rounds_played'))
        hp_wins = int(disadv_wins)
        hp_attempts = int(disadv_attempts)
        metrics['high_pressure_wins'] = hp_wins
        metrics['high_pressure_attempts'] = hp_attempts
        metrics['high_pressure_success'] = (
            hp_wins / hp_attempts if hp_attempts > 0 else 0.0
        )
        metrics['high_pressure_attempt_rate'] = (
            hp_attempts / rounds_played if rounds_played > 0 else 0.0
        )

        # Impact rating
        kills = self._num(snapshot.get('kills'))
        assists = self._num(snapshot.get('assists'))
        metrics['impact_rating'] = (
            (kills + assists) / rounds_played if rounds_played > 0 else 0.0
        )

        # Wins per hour
        wins = self._num(snapshot.get('wins'))
        time_played_hours = self._num(snapshot.get('time_played_hours'))
        metrics['wins_per_hour'] = (
            wins / time_played_hours if time_played_hours > 0 else 0.0
        )

        # Role classification
        role_info = self.identify_role(metrics)
        metrics['primary_role'] = role_info['primary']
        metrics['primary_confidence'] = role_info['primary_confidence']
        metrics['secondary_role'] = role_info.get('secondary')
        metrics['secondary_confidence'] = role_info.get('secondary_confidence', 0)
        metrics['overall_conf'] = role_info['primary_confidence']
        metrics['_defaulted_snapshot_fields'] = defaulted_fields

        return metrics
    
    # Individual calculation methods
    def calc_entry_efficiency(self, first_bloods: int, first_deaths: int) -> float:
        first_bloods = self._num(first_bloods)
        first_deaths = self._num(first_deaths)
        total = first_bloods + first_deaths
        return first_bloods / total if total > 0 else 0.0
    
    def calc_aggression_score(self, fb: int, fd: int, rounds: int) -> float:
        fb = self._num(fb)
        fd = self._num(fd)
        rounds = self._num(rounds)
        return (fb + fd) / rounds if rounds > 0 else 0.0
    
    def calc_clutch_attempt_rate(self, clutches: int, lost: int, rounds: int) -> float:
        clutches = self._num(clutches)
        lost = self._num(lost)
        rounds = self._num(rounds)
        return (clutches + lost) / rounds if rounds > 0 else 0.0
    
    def calc_clutch_success(self, wins: int, losses: int) -> float:
        wins = self._num(wins)
        losses = self._num(losses)
        total = wins + losses
        return wins / total if total > 0 else 0.0
    
    def calc_disadvantaged_clutch_success(self, clutches: Dict) -> float:
        clutches = clutches or {}
        wins = sum([
            self._num(clutches.get('1v2', 0)),
            self._num(clutches.get('1v3', 0)),
            self._num(clutches.get('1v4', 0)),
            self._num(clutches.get('1v5', 0))
        ])
        
        losses = sum([
            self._num(clutches.get('lost_1v2', 0)),
            self._num(clutches.get('lost_1v3', 0)),
            self._num(clutches.get('lost_1v4', 0)),
            self._num(clutches.get('lost_1v5', 0))
        ])
        
        total = wins + losses
        return wins / total if total > 0 else 0.0
    
    def calc_teamplay_index(self, assists: int, kills: int) -> float:
        assists = self._num(assists)
        kills = self._num(kills)
        total = assists + kills
        return assists / total if total > 0 else 0.0
    
    def calc_fragger_score(self, kd: float, kpr: float, fb: int, rounds: int) -> float:
        kd = self._num(kd)
        kpr = self._num(kpr)
        fb = self._num(fb)
        rounds = self._num(rounds)
        fb_rate = fb / rounds if rounds > 0 else 0
        return (kd * 30) + (kpr * 40) + (fb_rate * 300)
    
    def calc_entry_score(self, entry_eff: float, aggr: float) -> float:
        entry_eff = self._num(entry_eff)
        aggr = self._num(aggr)
        return (entry_eff * 40) + (aggr * 60)
    
    def calc_support_score(self, apr: float, teamplay: float) -> float:
        apr = self._num(apr)
        teamplay = self._num(teamplay)
        return (apr * 150) + (teamplay * 50)
    
    def calc_anchor_score(self, clutch_rate: float, clutch_1v1: float, dpr: float) -> float:
        clutch_rate = self._num(clutch_rate)
        clutch_1v1 = self._num(clutch_1v1)
        dpr = self._num(dpr)
        return (clutch_rate * 40) + (clutch_1v1 * 40) + ((1 - dpr) * 20)
    
    def calc_clutch_specialist_score(self, clutches: int, rounds: int, 
                                     clutch_1v1: float, clutch_disadv: float) -> float:
        clutches = self._num(clutches)
        rounds = self._num(rounds)
        clutch_1v1 = self._num(clutch_1v1)
        clutch_disadv = self._num(clutch_disadv)
        clutch_per_round = clutches / rounds if rounds > 0 else 0
        return (clutch_per_round * 100) + (clutch_1v1 * 30) + (clutch_disadv * 70)
    
    def calc_carry_score(self, kd: float, win_pct: float, kpr: float, clutch: float) -> float:
        kd = self._num(kd)
        win_pct = self._num(win_pct)
        kpr = self._num(kpr)
        clutch = self._num(clutch)
        return (kd * 25) + (win_pct * 0.3) + (kpr * 30) + (clutch * 20)
    
    def identify_role(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """Identify primary and secondary roles based on scores.

        Confidence is the normalized gap between the top two role scores
        (0-100).  A large gap means high certainty; a tiny gap means
        the role assignment is ambiguous.
        """
        scores = {
            'Fragger': metrics['fragger_score'],
            'Entry': metrics['entry_score'],
            'Support': metrics['support_score'],
            'Anchor': metrics['anchor_score'],
            'Clutch': metrics['clutch_specialist_score'],
            'Carry': metrics['carry_score']
        }

        sorted_roles = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        primary = sorted_roles[0][0]
        primary_score = sorted_roles[0][1]

        secondary = sorted_roles[1][0] if len(sorted_roles) > 1 else None
        secondary_score = sorted_roles[1][1] if len(sorted_roles) > 1 else 0

        # Confidence = how far ahead the primary is vs. the runner-up,
        # normalized against the primary score (the max possible gap).
        score_gap = primary_score - secondary_score
        if primary_score > 0:
            primary_conf = min((score_gap / primary_score) * 100.0, 100.0)
        else:
            primary_conf = 50.0

        # Secondary confidence: gap between 2nd and 3rd place.
        third_score = sorted_roles[2][1] if len(sorted_roles) > 2 else 0
        sec_gap = secondary_score - third_score
        if secondary_score > 0:
            secondary_conf = min((sec_gap / secondary_score) * 100.0, 100.0)
        else:
            secondary_conf = 0.0

        return {
            'primary': primary,
            'primary_confidence': round(primary_conf, 1),
            'secondary': secondary if secondary_conf > 5 else None,
            'secondary_confidence': round(secondary_conf, 1) if secondary_conf > 5 else 0,
        }
