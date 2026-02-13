import json

from src.calculator import MetricsCalculator


def test_calculate_all_handles_none_numeric_fields():
    calc = MetricsCalculator()
    snapshot = {
        'clutches_data': json.dumps({
            'total': 0,
            'lost_total': 0,
            '1v1': 0,
            'lost_1v1': 0,
            '1v2': 0,
            'lost_1v2': 0,
            '1v3': 0,
            'lost_1v3': 0,
            '1v4': 0,
            'lost_1v4': 0,
            '1v5': 0,
            'lost_1v5': 0,
        }),
        'first_bloods': None,
        'first_deaths': None,
        'rounds_played': None,
        'assists': None,
        'kills': None,
        'kd': None,
        'kills_per_round': None,
        'assists_per_round': None,
        'deaths_per_round': None,
        'match_win_pct': None,
    }

    metrics = calc.calculate_all(snapshot)

    assert metrics['entry_efficiency'] == 0.0
    assert metrics['aggression_score'] == 0.0
    assert metrics['clutch_attempt_rate'] == 0.0
    assert metrics['carry_score'] >= 0.0
    assert metrics['primary_role']
