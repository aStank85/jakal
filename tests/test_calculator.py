# tests/test_calculator.py

import pytest
import json
from src.calculator import MetricsCalculator


class TestMetricsCalculator:
    """Test suite for metrics calculator."""

    @pytest.fixture
    def calculator(self):
        """Create calculator instance."""
        return MetricsCalculator()

    @pytest.fixture
    def sample_snapshot(self):
        """Sample snapshot data from database."""
        clutches_data = {
            'total': 28,
            '1v1': 16,
            '1v2': 7,
            '1v3': 5,
            '1v4': 0,
            '1v5': 0,
            'lost_total': 81,
            'lost_1v1': 6,
            'lost_1v2': 14,
            'lost_1v3': 24,
            'lost_1v4': 25,
            'lost_1v5': 12
        }

        return {
            'snapshot_id': 1,
            'player_id': 1,
            'username': 'TestPlayer',
            'matches': 117,
            'wins': 61,
            'losses': 56,
            'match_win_pct': 52.1,
            'rounds_played': 702,
            'kills': 680,
            'deaths': 493,
            'assists': 193,
            'kd': 1.38,
            'kills_per_round': 0.97,
            'deaths_per_round': 0.70,
            'assists_per_round': 0.27,
            'hs_pct': 60.0,
            'first_bloods': 96,
            'first_deaths': 57,
            'time_played_hours': 120.0,
            'clutches_data': json.dumps(clutches_data)
        }

    def test_calc_entry_efficiency(self, calculator):
        """Test entry efficiency calculation."""
        # 10 first bloods, 5 first deaths = 10/15 = 0.667
        assert calculator.calc_entry_efficiency(10, 5) == pytest.approx(0.667, rel=0.01)

        # Equal first bloods and deaths = 0.5
        assert calculator.calc_entry_efficiency(10, 10) == 0.5

        # No entries = 0
        assert calculator.calc_entry_efficiency(0, 0) == 0.0

        # Perfect entry = 1.0
        assert calculator.calc_entry_efficiency(10, 0) == 1.0

    def test_calc_aggression_score(self, calculator):
        """Test aggression score calculation."""
        # 100 total entries in 500 rounds = 0.2
        assert calculator.calc_aggression_score(60, 40, 500) == 0.2

        # No rounds = 0
        assert calculator.calc_aggression_score(10, 5, 0) == 0.0

        # High aggression
        assert calculator.calc_aggression_score(100, 50, 200) == 0.75

    def test_calc_clutch_attempt_rate(self, calculator):
        """Test clutch attempt rate calculation."""
        # 30 clutches + 70 lost in 500 rounds = 0.2
        assert calculator.calc_clutch_attempt_rate(30, 70, 500) == 0.2

        # No rounds = 0
        assert calculator.calc_clutch_attempt_rate(10, 10, 0) == 0.0

    def test_calc_clutch_success(self, calculator):
        """Test clutch success rate calculation."""
        # 20 wins, 10 losses = 0.667
        assert calculator.calc_clutch_success(20, 10) == pytest.approx(0.667, rel=0.01)

        # Perfect clutch = 1.0
        assert calculator.calc_clutch_success(10, 0) == 1.0

        # No clutches = 0
        assert calculator.calc_clutch_success(0, 0) == 0.0

        # 50% success
        assert calculator.calc_clutch_success(10, 10) == 0.5

    def test_calc_disadvantaged_clutch_success(self, calculator):
        """Test disadvantaged clutch (1v2+) success rate."""
        clutches = {
            '1v2': 10,
            '1v3': 5,
            '1v4': 2,
            '1v5': 1,
            'lost_1v2': 20,
            'lost_1v3': 15,
            'lost_1v4': 10,
            'lost_1v5': 5
        }

        # Total wins: 18, Total losses: 50, Rate: 18/68 = 0.265
        result = calculator.calc_disadvantaged_clutch_success(clutches)
        assert result == pytest.approx(0.265, rel=0.01)

        # No disadvantaged clutches
        empty_clutches = {
            '1v2': 0,
            '1v3': 0,
            '1v4': 0,
            '1v5': 0,
            'lost_1v2': 0,
            'lost_1v3': 0,
            'lost_1v4': 0,
            'lost_1v5': 0
        }
        assert calculator.calc_disadvantaged_clutch_success(empty_clutches) == 0.0

    def test_calc_teamplay_index(self, calculator):
        """Test teamplay index calculation."""
        # 100 assists, 300 kills = 100/400 = 0.25
        assert calculator.calc_teamplay_index(100, 300) == 0.25

        # 50-50 split = 0.5
        assert calculator.calc_teamplay_index(200, 200) == 0.5

        # No assists or kills = 0
        assert calculator.calc_teamplay_index(0, 0) == 0.0

        # All assists, no kills = 1.0
        assert calculator.calc_teamplay_index(100, 0) == 1.0

    def test_calc_fragger_score(self, calculator):
        """Test fragger score calculation."""
        # K/D=1.5, KPR=1.0, FB=100, Rounds=500
        # FB rate = 100/500 = 0.2
        # Score = (1.5*30) + (1.0*40) + (0.2*300) = 45 + 40 + 60 = 145
        score = calculator.calc_fragger_score(1.5, 1.0, 100, 500)
        assert score == 145.0

        # Zero rounds
        assert calculator.calc_fragger_score(1.5, 1.0, 100, 0) == pytest.approx(85.0)

    def test_calc_entry_score(self, calculator):
        """Test entry score calculation."""
        # Entry efficiency = 0.7, Aggression = 0.3
        # Score = (0.7*40) + (0.3*60) = 28 + 18 = 46
        score = calculator.calc_entry_score(0.7, 0.3)
        assert score == 46.0

    def test_calc_support_score(self, calculator):
        """Test support score calculation."""
        # APR = 0.4, Teamplay = 0.3
        # Score = (0.4*150) + (0.3*50) = 60 + 15 = 75
        score = calculator.calc_support_score(0.4, 0.3)
        assert score == 75.0

    def test_calc_anchor_score(self, calculator):
        """Test anchor score calculation."""
        # Clutch rate = 0.2, 1v1 success = 0.8, DPR = 0.7
        # Score = (0.2*40) + (0.8*40) + ((1-0.7)*20) = 8 + 32 + 6 = 46
        score = calculator.calc_anchor_score(0.2, 0.8, 0.7)
        assert score == 46.0

    def test_calc_clutch_specialist_score(self, calculator):
        """Test clutch specialist score calculation."""
        # Clutches = 50, Rounds = 500, 1v1 = 0.75, Disadv = 0.3
        # CPR = 50/500 = 0.1
        # Score = (0.1*100) + (0.75*30) + (0.3*70) = 10 + 22.5 + 21 = 53.5
        score = calculator.calc_clutch_specialist_score(50, 500, 0.75, 0.3)
        assert score == 53.5

    def test_calc_carry_score(self, calculator):
        """Test carry score calculation."""
        # K/D = 1.5, Win% = 55, KPR = 1.2, Clutch = 0.8
        # Score = (1.5*25) + (55*0.3) + (1.2*30) + (0.8*20) = 37.5 + 16.5 + 36 + 16 = 106
        score = calculator.calc_carry_score(1.5, 55.0, 1.2, 0.8)
        assert score == 106.0

    def test_identify_role(self, calculator):
        """Test role identification."""
        metrics = {
            'fragger_score': 120.0,
            'entry_score': 45.0,
            'support_score': 50.0,
            'anchor_score': 40.0,
            'clutch_specialist_score': 35.0,
            'carry_score': 90.0
        }

        result = calculator.identify_role(metrics)

        # Primary should be Fragger (highest score)
        assert result['primary'] == 'Fragger'
        assert result['primary_confidence'] == 120.0

        # Secondary should be Carry (second highest, above 30 threshold)
        assert result['secondary'] == 'Carry'
        assert result['secondary_confidence'] == 90.0

    def test_identify_role_no_secondary(self, calculator):
        """Test role identification when secondary is below threshold."""
        metrics = {
            'fragger_score': 100.0,
            'entry_score': 25.0,  # Below 30 threshold
            'support_score': 20.0,
            'anchor_score': 15.0,
            'clutch_specialist_score': 10.0,
            'carry_score': 28.0  # Below 30 threshold
        }

        result = calculator.identify_role(metrics)

        assert result['primary'] == 'Fragger'
        assert result['secondary'] is None
        assert result['secondary_confidence'] == 0

    def test_calculate_all_integration(self, calculator, sample_snapshot):
        """Test full metrics calculation pipeline."""
        metrics = calculator.calculate_all(sample_snapshot)

        # Verify all expected keys are present
        expected_keys = [
            'entry_efficiency',
            'aggression_score',
            'clutch_attempt_rate',
            'clutch_1v1_success',
            'clutch_disadvantaged_success',
            'teamplay_index',
            'fragger_score',
            'entry_score',
            'support_score',
            'anchor_score',
            'clutch_specialist_score',
            'carry_score',
            'primary_role',
            'primary_confidence',
            'secondary_role',
            'secondary_confidence',
            'rounds_per_hour',
            'time_played_unreliable',
            'high_pressure_wins',
            'extreme_attempts',
        ]

        for key in expected_keys:
            assert key in metrics, f"Missing key: {key}"

        # Verify values are in reasonable ranges
        assert 0 <= metrics['entry_efficiency'] <= 1
        assert 0 <= metrics['clutch_1v1_success'] <= 1
        assert 0 <= metrics['teamplay_index'] <= 1
        assert metrics['primary_role'] in ['Fragger', 'Entry', 'Support', 'Anchor', 'Clutch', 'Carry']

    def test_calculate_all_with_real_data(self, calculator, sample_snapshot):
        """Test calculation with realistic sample data."""
        metrics = calculator.calculate_all(sample_snapshot)

        # Entry efficiency: 96 / (96 + 57) = 0.627
        assert metrics['entry_efficiency'] == pytest.approx(0.627, rel=0.01)

        # Teamplay index: 193 / (193 + 680) = 0.221
        assert metrics['teamplay_index'] == pytest.approx(0.221, rel=0.01)

        # 1v1 clutch success: 16 / (16 + 6) = 0.727
        assert metrics['clutch_1v1_success'] == pytest.approx(0.727, rel=0.01)

    def test_krazy_clutch_golden_metrics(self, calculator):
        """Golden test for clutch math consistency on a Krazy-like profile."""
        snapshot = {
            'rounds_played': 702,
            'kills': 680,
            'deaths': 493,
            'assists': 193,
            'kd': 1.38,
            'kills_per_round': 0.97,
            'deaths_per_round': 0.70,
            'assists_per_round': 0.27,
            'first_bloods': 96,
            'first_deaths': 57,
            'match_win_pct': 52.1,
            'wins': 61,
            'time_played_hours': 288.0,
            'clutches_data': json.dumps({
                'total': 21,
                '1v1': 12,
                '1v2': 7,
                '1v3': 2,
                '1v4': 0,
                '1v5': 0,
                'lost_total': 88,
                'lost_1v1': 10,
                'lost_1v2': 30,
                'lost_1v3': 11,
                'lost_1v4': 25,
                'lost_1v5': 12,
            })
        }

        metrics = calculator.calculate_all(snapshot)

        assert metrics['clutch_attempt_rate'] == pytest.approx(109 / 702, rel=0.001)
        assert metrics['overall_clutch_success'] == pytest.approx(21 / 109, rel=0.001)
        assert metrics['clutch_1v1_success'] == pytest.approx(12 / 22, rel=0.001)
        assert metrics['clutch_1v2_success'] == pytest.approx(7 / 37, rel=0.001)

        assert metrics['high_pressure_attempt_rate'] == pytest.approx(50 / 702, rel=0.001)
        assert metrics['high_pressure_success'] == pytest.approx(2 / 50, rel=0.001)
        assert metrics['high_pressure_attempts'] == 50
        assert metrics['high_pressure_wins'] == 2

        assert metrics['disadv_attempt_share'] == pytest.approx(87 / 109, rel=0.001)
        assert metrics['extreme_attempts'] == 37


class TestCalculatorEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_division_by_zero_handling(self):
        """Test that division by zero is handled gracefully."""
        calculator = MetricsCalculator()

        # All methods should return 0 when denominators are 0
        assert calculator.calc_entry_efficiency(0, 0) == 0.0
        assert calculator.calc_aggression_score(10, 10, 0) == 0.0
        assert calculator.calc_clutch_success(0, 0) == 0.0
        assert calculator.calc_teamplay_index(0, 0) == 0.0

    def test_perfect_stats(self):
        """Test calculation with perfect stats."""
        calculator = MetricsCalculator()

        # Perfect entry (all first bloods, no first deaths)
        assert calculator.calc_entry_efficiency(100, 0) == 1.0

        # Perfect clutch (all wins, no losses)
        assert calculator.calc_clutch_success(50, 0) == 1.0

    def test_zero_stats(self):
        """Test calculation with all zero stats."""
        calculator = MetricsCalculator()

        empty_clutches = {
            'total': 0,
            '1v1': 0, '1v2': 0, '1v3': 0, '1v4': 0, '1v5': 0,
            'lost_total': 0,
            'lost_1v1': 0, 'lost_1v2': 0, 'lost_1v3': 0, 'lost_1v4': 0, 'lost_1v5': 0
        }

        snapshot = {
            'matches': 0,
            'rounds_played': 0,
            'kills': 0,
            'deaths': 0,
            'assists': 0,
            'kd': 0,
            'kills_per_round': 0,
            'deaths_per_round': 0,
            'assists_per_round': 0,
            'first_bloods': 0,
            'first_deaths': 0,
            'match_win_pct': 0,
            'clutches_data': json.dumps(empty_clutches)
        }

        metrics = calculator.calculate_all(snapshot)

        # Should not crash and should return valid structure
        assert 'primary_role' in metrics
        assert 'primary_confidence' in metrics

    def test_calculate_all_with_partial_clutch_data(self):
        """Test calculate_all handles missing clutch keys gracefully."""
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 100,
            'kills': 50,
            'deaths': 40,
            'assists': 20,
            'kd': 1.25,
            'kills_per_round': 0.5,
            'deaths_per_round': 0.4,
            'assists_per_round': 0.2,
            'first_bloods': 10,
            'first_deaths': 8,
            'match_win_pct': 52.0,
            'wins': 20,
            'time_played_hours': 40.0,
            'clutches_data': json.dumps({'1v1': 3, 'lost_1v1': 2})
        }

        metrics = calculator.calculate_all(snapshot)
        assert metrics['clutch_attempt_rate'] == pytest.approx(0.05, rel=0.01)
        assert metrics['clutch_1v1_success'] == pytest.approx(0.6, rel=0.01)

    def test_clutch_total_mismatch_logs_warning_and_uses_computed_sum(self, caplog):
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 100,
            'kills': 10,
            'deaths': 10,
            'assists': 2,
            'first_bloods': 1,
            'first_deaths': 1,
            'wins': 5,
            'time_played_hours': 20.0,
            'clutches_data': json.dumps({
                'total': 99,
                '1v1': 1,
                'lost_total': 88,
                'lost_1v1': 2,
            })
        }

        with caplog.at_level('WARNING'):
            metrics = calculator.calculate_all(snapshot)

        assert metrics['clutch_attempt_rate'] == pytest.approx(0.03, rel=0.01)
        assert metrics['clutch_totals_mismatch'] is True
        assert metrics['clutch_lost_totals_mismatch'] is True
        assert metrics['clutch_totals_unreliable'] is True
        assert metrics['overall_clutch_success'] == pytest.approx(1 / 3, rel=0.01)
        assert "Clutch total mismatch" in caplog.text
        assert "Clutch lost_total mismatch" in caplog.text

    def test_time_unreliable_suppresses_per_hour_metrics(self):
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 120,
            'kills': 80,
            'deaths': 60,
            'assists': 20,
            'wins': 30,
            'kd': 1.33,
            'kills_per_round': 0.67,
            'deaths_per_round': 0.5,
            'assists_per_round': 0.17,
            'first_bloods': 10,
            'first_deaths': 8,
            'time_played_hours': 60.0,
            'clutches_data': json.dumps({'1v1': 2, 'lost_1v1': 2}),
        }

        metrics = calculator.calculate_all(snapshot)

        assert metrics['rounds_per_hour'] == pytest.approx(2.0, rel=0.01)
        assert metrics['time_played_unreliable'] is True
        assert metrics['wins_per_hour'] is None
        assert metrics['kills_per_hour'] is None
        assert metrics['tk_per_hour'] is None

    def test_time_reliable_keeps_per_hour_metrics(self):
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 300,
            'kills': 210,
            'deaths': 180,
            'assists': 60,
            'wins': 120,
            'kd': 1.16,
            'kills_per_round': 0.7,
            'deaths_per_round': 0.6,
            'assists_per_round': 0.2,
            'first_bloods': 20,
            'first_deaths': 15,
            'time_played_hours': 40.0,
            'clutches_data': json.dumps({'1v1': 2, 'lost_1v1': 2}),
        }

        metrics = calculator.calculate_all(snapshot)

        assert metrics['rounds_per_hour'] == pytest.approx(7.5, rel=0.01)
        assert metrics['time_played_unreliable'] is False
        assert metrics['wins_per_hour'] == pytest.approx(3.0, rel=0.01)
        assert metrics['kills_per_hour'] == pytest.approx(5.25, rel=0.01)

    def test_expansion_pack_metrics(self):
        """Test new v1 expansion metrics and confidence weighting."""
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 200,
            'kills': 220,
            'deaths': 180,
            'assists': 80,
            'kd': 1.22,
            'kills_per_round': 1.10,
            'deaths_per_round': 0.90,
            'assists_per_round': 0.40,
            'first_bloods': 40,
            'first_deaths': 25,
            'match_win_pct': 54.0,
            'wins': 80,
            'time_played_hours': 100.0,
            'teamkills': 4,
            'clutches_data': json.dumps({
                'total': 20,
                '1v1': 10,
                '1v2': 6,
                '1v3': 3,
                '1v4': 1,
                '1v5': 0,
                'lost_total': 40,
                'lost_1v1': 5,
                'lost_1v2': 10,
                'lost_1v3': 12,
                'lost_1v4': 8,
                'lost_1v5': 5
            })
        }

        metrics = calculator.calculate_all(snapshot)

        assert metrics['engagement_rate'] == pytest.approx(2.0, rel=0.01)
        assert metrics['wcontrib_per_round'] == pytest.approx(1.30, rel=0.01)
        assert metrics['opening_net_per_round'] == pytest.approx(0.075, rel=0.01)
        assert metrics['clutch_attempts'] == 60
        assert metrics['clutch_attempts_per_100'] == pytest.approx(30.0, rel=0.01)
        assert metrics['high_pressure_attempts'] == 29
        assert metrics['high_pressure_wins'] == 4
        assert metrics['survival_rate'] == pytest.approx(0.10, rel=0.01)
        assert metrics['rounds_per_hour'] == pytest.approx(2.0, rel=0.01)
        assert metrics['time_played_unreliable'] is True
        assert metrics['wins_per_hour'] is None
        assert metrics['tk_per_kill'] == pytest.approx(4 / 220, rel=0.01)
        assert metrics['clean_play_index'] == pytest.approx(0.0, rel=0.01)
        assert metrics['rounds_conf'] == pytest.approx(200 / 300, rel=0.01)
        assert metrics['time_conf'] == pytest.approx(0.0, rel=0.01)
        assert metrics['clutch_conf'] == pytest.approx(1.0, rel=0.01)
        assert metrics['overall_conf'] == pytest.approx(0.55, rel=0.01)

    def test_clean_play_index_at_normalization_rate(self):
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 100,
            'kills': 50,
            'deaths': 50,
            'assists': 10,
            'wins': 20,
            'kd': 1.0,
            'kills_per_round': 0.5,
            'deaths_per_round': 0.5,
            'assists_per_round': 0.1,
            'first_bloods': 5,
            'first_deaths': 5,
            'time_played_hours': 10.0,
            'teamkills': 2,
            'clutches_data': json.dumps({'1v1': 1, 'lost_1v1': 1}),
        }

        metrics = calculator.calculate_all(snapshot)
        assert metrics['clean_play_index'] == pytest.approx(0.0, rel=0.01)

    def test_clean_play_index_at_half_normalization_rate(self):
        calculator = MetricsCalculator()
        snapshot = {
            'rounds_played': 100,
            'kills': 50,
            'deaths': 50,
            'assists': 10,
            'wins': 20,
            'kd': 1.0,
            'kills_per_round': 0.5,
            'deaths_per_round': 0.5,
            'assists_per_round': 0.1,
            'first_bloods': 5,
            'first_deaths': 5,
            'time_played_hours': 10.0,
            'teamkills': 1,
            'clutches_data': json.dumps({'1v1': 1, 'lost_1v1': 1}),
        }

        metrics = calculator.calculate_all(snapshot)
        assert metrics['clean_play_index'] == pytest.approx(0.5, rel=0.01)
