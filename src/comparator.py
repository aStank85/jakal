# src/comparator.py

from typing import List, Dict, Any, Optional

class PlayerComparator:
    """Compare multiple players."""
    
    def compare(self, snapshots: List[Dict[str, Any]], 
                metrics_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Compare multiple players.
        
        Args:
            snapshots: List of snapshot dictionaries
            metrics_list: List of computed metrics
            
        Returns:
            Comparison results
        """
        # Stats to compare
        compare_stats = [
            ('rounds_played', 'Rounds Played', 'contextual'),
            ('kd', 'K/D', 'higher'),
            ('match_win_pct', 'Win %', 'higher'),
            ('hs_pct', 'HS %', 'higher'),
            ('kills_per_round', 'Kills/Round', 'higher'),
            ('assists_per_round', 'Assists/Round', 'higher'),
            ('first_blood_rate', 'First Blood Rate', 'higher'),
            ('entry_efficiency', 'Entry Efficiency', 'higher'),
            ('clutch_1v1_success', '1v1 Clutch %', 'higher'),
            ('overall_clutch_success', 'Overall Clutch %', 'higher'),
            ('impact_rating', 'Impact Rating', 'higher'),
            ('teamplay_index', 'Teamplay Index', 'higher'),
            ('aggression_score', 'Aggression', 'contextual'),
        ]
        
        comparison = {
            'players': [],
            'stats': [],
            'winners': {}
        }
        
        # Build player list
        for i, (snapshot, metrics) in enumerate(zip(snapshots, metrics_list)):
            comparison['players'].append({
                'username': snapshot['username'],
                'snapshot_date': snapshot['snapshot_date'],
                'primary_role': metrics['primary_role']
            })
        
        # Compare each stat
        for stat_key, stat_name, better_when in compare_stats:
            stat_comparison = {
                'name': stat_name,
                'values': []
            }
            
            for snapshot, metrics in zip(snapshots, metrics_list):
                value = self._get_compare_value(stat_key, snapshot, metrics)
                stat_comparison['values'].append(value)
            
            # Determine winner
            winner_idx = self._determine_winner(stat_comparison['values'], better_when)
            
            stat_comparison['winner_index'] = winner_idx
            comparison['stats'].append(stat_comparison)
        
        # Calculate overall advantages
        for i in range(len(snapshots)):
            comparison['winners'][i] = sum(
                1 for stat in comparison['stats'] 
                if stat['winner_index'] == i
            )
        
        return comparison

    def _get_compare_value(self, stat_key: str, snapshot: Dict[str, Any], metrics: Dict[str, Any]) -> Optional[float]:
        """Resolve comparison value from snapshot/metrics with inline derived support."""
        if stat_key == 'first_blood_rate':
            rounds = self._to_float(snapshot.get('rounds_played'))
            if rounds <= 0:
                return 0.0
            first_bloods = self._to_float(snapshot.get('first_bloods'))
            return first_bloods / rounds

        if stat_key in snapshot:
            return snapshot.get(stat_key)

        return metrics.get(stat_key)

    def _determine_winner(self, values: List[Optional[float]], better_when: str) -> Optional[int]:
        """None-safe and tie-safe winner selection."""
        if better_when == 'contextual':
            return None

        numeric_values: List[tuple[int, float]] = []
        for idx, value in enumerate(values):
            if value is None:
                continue
            numeric_values.append((idx, self._to_float(value)))

        if len(numeric_values) < 2:
            return None

        if better_when == 'lower':
            best_value = min(v for _, v in numeric_values)
        else:
            best_value = max(v for _, v in numeric_values)

        winners = [idx for idx, v in numeric_values if v == best_value]
        if len(winners) != 1:
            return None
        return winners[0]

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            if value is None:
                return 0.0
            return float(value)
        except (TypeError, ValueError):
            return 0.0
