# src/comparator.py

from typing import List, Dict, Any

PER_HOUR_METRIC_KEYS = {
    'wins_per_hour',
    'kills_per_hour',
    'deaths_per_hour',
    'assists_per_hour',
    'wcontrib_per_hour',
    'clutch_attempts_per_hour',
    'clutch_wins_per_hour',
    'tk_per_hour',
}

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
            ('kd', 'K/D', 'higher'),
            ('match_win_pct', 'Win %', 'higher'),
            ('hs_pct', 'HS %', 'higher'),
            ('kills_per_round', 'Kills/Round', 'higher'),
            ('assists_per_round', 'Assists/Round', 'higher'),
            ('first_bloods', 'First Bloods', 'higher'),
            ('entry_efficiency', 'Entry Efficiency', 'higher'),
            ('clutch_1v1_success', '1v1 Clutch %', 'higher'),
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
                # Get value from snapshot or metrics
                if stat_key in snapshot:
                    value = snapshot[stat_key]
                else:
                    value = metrics.get(stat_key, 0)
                
                if stat_key in PER_HOUR_METRIC_KEYS and metrics.get('time_played_unreliable', False):
                    value = None

                stat_comparison['values'].append(value)
            
            # Determine winner. Any suppressed value (None) makes this metric no-contest.
            if any(value is None for value in stat_comparison['values']):
                winner_idx = None
            elif better_when == 'higher':
                max_value = max(stat_comparison['values'])
                winner_indices = [idx for idx, value in enumerate(stat_comparison['values']) if value == max_value]
                winner_idx = winner_indices[0] if len(winner_indices) == 1 else None
            elif better_when == 'lower':
                min_value = min(stat_comparison['values'])
                winner_indices = [idx for idx, value in enumerate(stat_comparison['values']) if value == min_value]
                winner_idx = winner_indices[0] if len(winner_indices) == 1 else None
            else:
                winner_idx = None  # Contextual, no clear winner
            
            stat_comparison['winner_index'] = winner_idx
            comparison['stats'].append(stat_comparison)
        
        # Calculate overall advantages
        for i in range(len(snapshots)):
            comparison['winners'][i] = sum(
                1 for stat in comparison['stats'] 
                if stat['winner_index'] == i
            )
        
        return comparison



