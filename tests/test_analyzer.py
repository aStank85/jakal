import json

from src.analyzer import InsightAnalyzer


class TestInsightAnalyzer:
    def test_generates_expected_schema(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 300,
            "match_win_pct": 49.0,
            "kd": 1.30,
            "assists_per_round": 0.15,
            "teamkills": 4,
            "time_played_hours": 120.0,
            "clutches_data": json.dumps({"total": 20, "lost_total": 80}),
        }
        metrics = {
            "entry_efficiency": 0.40,
            "aggression_score": 0.25,
            "clutch_attempt_rate": 0.33,
            "overall_clutch_success": 0.20,
            "teamplay_index": 0.10,
            "wins_per_hour": 0.10,
            "impact_rating": 1.20,
            "primary_role": "Fragger",
        }

        insights = analyzer.generate_insights(snapshot, metrics)

        assert len(insights) > 0
        for insight in insights:
            assert "severity" in insight
            assert "category" in insight
            assert "message" in insight
            assert "evidence" in insight
            assert "action" in insight

    def test_high_kd_low_win_rule(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 250,
            "match_win_pct": 47.0,
            "kd": 1.35,
            "assists_per_round": 0.20,
            "teamkills": 1,
            "time_played_hours": 80.0,
            "clutches_data": json.dumps({"total": 10, "lost_total": 30}),
        }
        metrics = {
            "entry_efficiency": 0.55,
            "aggression_score": 0.19,
            "clutch_attempt_rate": 0.16,
            "overall_clutch_success": 0.25,
            "teamplay_index": 0.20,
            "wins_per_hour": 0.11,
            "impact_rating": 1.05,
            "primary_role": "Carry",
        }

        insights = analyzer.generate_insights(snapshot, metrics)
        messages = [insight["message"] for insight in insights]
        assert "Fragging output is not converting into wins consistently." in messages

    def test_small_sample_guard(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 60,
            "match_win_pct": 55.0,
            "kd": 1.00,
            "assists_per_round": 0.22,
            "teamkills": 0,
            "time_played_hours": 12.0,
            "clutches_data": "{}",
        }
        metrics = {
            "entry_efficiency": 0.50,
            "aggression_score": 0.15,
            "clutch_attempt_rate": 0.05,
            "overall_clutch_success": 0.20,
            "teamplay_index": 0.25,
            "wins_per_hour": 0.30,
            "impact_rating": 0.90,
            "primary_role": "Support",
        }

        insights = analyzer.generate_insights(snapshot, metrics)
        categories = [insight["category"] for insight in insights]
        assert "sample_size" in categories

    def test_baseline_insight_when_no_flags(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 250,
            "match_win_pct": 51.0,
            "kd": 1.05,
            "assists_per_round": 0.22,
            "teamkills": 0,
            "time_played_hours": 20.0,
            "clutches_data": json.dumps({"total": 2, "lost_total": 5}),
        }
        metrics = {
            "entry_efficiency": 0.52,
            "aggression_score": 0.14,
            "clutch_attempt_rate": 0.03,
            "overall_clutch_success": 0.29,
            "teamplay_index": 0.20,
            "wins_per_hour": 0.20,
            "impact_rating": 0.98,
            "primary_role": "Entry",
        }

        insights = analyzer.generate_insights(snapshot, metrics)
        assert len(insights) == 1
        assert insights[0]["category"] == "baseline"

    def test_teamkill_severity_low_between_one_and_two_percent(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 702,
            "match_win_pct": 52.0,
            "kd": 1.10,
            "assists_per_round": 0.22,
            "teamkills": 11,  # 1.57%
            "time_played_hours": 120.0,
            "clutches_data": json.dumps({"total": 20, "lost_total": 50}),
        }
        metrics = {
            "entry_efficiency": 0.63,
            "aggression_score": 0.22,
            "clutch_attempt_rate": 0.10,
            "overall_clutch_success": 0.29,
            "teamplay_index": 0.20,
            "wins_per_hour": 0.20,
            "impact_rating": 1.00,
            "primary_role": "Entry",
        }

        insights = analyzer.generate_insights(snapshot, metrics)
        discipline = next((item for item in insights if item["category"] == "discipline"), None)
        assert discipline is not None
        assert discipline["severity"] == "low"

    def test_teamkill_severity_medium_at_two_percent_or_higher(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 300,
            "match_win_pct": 49.0,
            "kd": 1.00,
            "assists_per_round": 0.20,
            "teamkills": 8,  # 2.66%
            "time_played_hours": 60.0,
            "clutches_data": json.dumps({"total": 10, "lost_total": 20}),
        }
        metrics = {
            "entry_efficiency": 0.50,
            "aggression_score": 0.15,
            "clutch_attempt_rate": 0.10,
            "overall_clutch_success": 0.25,
            "teamplay_index": 0.20,
            "wins_per_hour": 0.15,
            "impact_rating": 0.95,
            "primary_role": "Support",
        }

        insights = analyzer.generate_insights(snapshot, metrics)
        discipline = next((item for item in insights if item["category"] == "discipline"), None)
        assert discipline is not None
        assert discipline["severity"] == "medium"

    def test_low_overall_confidence_adds_confidence_insight(self):
        analyzer = InsightAnalyzer()
        snapshot = {
            "rounds_played": 80,
            "match_win_pct": 50.0,
            "kd": 1.0,
            "assists_per_round": 0.2,
            "teamkills": 0,
            "time_played_hours": 10.0,
            "clutches_data": json.dumps({"total": 2, "lost_total": 3}),
        }
        metrics = {
            "entry_efficiency": 0.5,
            "aggression_score": 0.1,
            "clutch_attempt_rate": 0.06,
            "overall_clutch_success": 0.4,
            "teamplay_index": 0.2,
            "wins_per_hour": 0.2,
            "impact_rating": 0.8,
            "primary_role": "Support",
            "overall_conf": 0.30,
            "opening_net_per_round": 0.0,
            "high_pressure_success": 0.0,
            "high_pressure_attempt_rate": 0.01,
            "risk_index": 0.2,
            "clean_play_index": 0.9,
        }

        insights = analyzer.generate_insights(snapshot, metrics)
        confidence = next((item for item in insights if item["category"] == "confidence"), None)
        assert confidence is not None
        assert confidence["severity"] == "info"
