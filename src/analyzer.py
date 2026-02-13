"""Rule-based insight generation for player snapshots and derived metrics."""

from __future__ import annotations

import json
from typing import Any, Dict, List

from .thresholds import (
    CLUTCH_BURDEN_ATTEMPT_RATE,
    CLUTCH_BURDEN_DISADV_SHARE,
    CLUTCH_BURDEN_MIN_ATTEMPTS,
    CLUTCH_LOW_SUCCESS_ATTEMPT_RATE,
    CLUTCH_LOW_SUCCESS_THRESHOLD,
    CLUTCH_MIN_STRONG_SAMPLE,
    CLUTCH_STRONG_SUCCESS_THRESHOLD,
    EFFICIENCY_LOW_WINS_PER_HOUR,
    EFFICIENCY_MIN_HOURS,
    EXTREME_CLUTCH_MIN_ATTEMPTS,
    EXTREME_CLUTCH_MIN_TOTAL_ATTEMPTS,
    HIGH_PRESSURE_LOW_SUCCESS,
    HIGH_PRESSURE_MIN_ATTEMPT_RATE,
    HIGH_PRESSURE_MIN_ATTEMPTS,
    HIGH_PRESSURE_STRONG_SUCCESS,
    LOW_CONFIDENCE_OVERALL,
    SMALL_SAMPLE_ROUNDS,
    TEAMKILL_SEVERITY_BANDS,
)


class InsightAnalyzer:
    """Generate deterministic insights from snapshot + metric data."""

    def generate_insights(self, snapshot: Dict[str, Any], metrics: Dict[str, Any]) -> List[Dict[str, str]]:
        insights: List[Dict[str, str]] = []

        rounds_played = self._as_float(snapshot.get("rounds_played"))
        match_win_pct = self._as_float(snapshot.get("match_win_pct"))
        kd = self._as_float(snapshot.get("kd"))
        assists_per_round = self._as_float(snapshot.get("assists_per_round"))
        teamkills = self._as_float(snapshot.get("teamkills"))
        time_played_hours = self._as_float(snapshot.get("time_played_hours"))

        entry_efficiency = self._as_float(metrics.get("entry_efficiency"))
        aggression_score = self._as_float(metrics.get("aggression_score"))
        clutch_attempt_rate = self._as_float(metrics.get("clutch_attempt_rate"))
        overall_clutch_success = self._as_float(metrics.get("overall_clutch_success"))
        teamplay_index = self._as_float(metrics.get("teamplay_index"))
        wins_per_hour = metrics.get("wins_per_hour")
        impact_rating = self._as_float(metrics.get("impact_rating"))

        clutch_attempts = int(self._as_float(metrics.get("clutch_attempts")))
        disadv_attempts = int(self._as_float(metrics.get("disadv_attempts")))
        disadv_attempt_share = self._as_float(metrics.get("disadv_attempt_share"))
        extreme_attempts = int(self._as_float(metrics.get("extreme_attempts")))

        hp_attempt_rate = self._as_float(metrics.get("high_pressure_attempt_rate"))
        hp_success = self._as_float(metrics.get("high_pressure_success"))
        hp_attempts = int(self._as_float(metrics.get("high_pressure_attempts")))
        hp_wins = int(self._as_float(metrics.get("high_pressure_wins")))

        # Fallback derivation from clutches JSON when partial high-pressure data is missing.
        if hp_attempts <= 0:
            clutch_json = self._parse_clutches(snapshot.get("clutches_data"))
            hp_wins = int(clutch_json.get("1v3", 0)) + int(clutch_json.get("1v4", 0)) + int(clutch_json.get("1v5", 0))
            hp_losses = int(clutch_json.get("lost_1v3", 0)) + int(clutch_json.get("lost_1v4", 0)) + int(clutch_json.get("lost_1v5", 0))
            hp_attempts = hp_wins + hp_losses
            hp_success = (hp_wins / hp_attempts) if hp_attempts else hp_success
            hp_attempt_rate = (hp_attempts / rounds_played) if rounds_played > 0 else hp_attempt_rate

        if rounds_played < SMALL_SAMPLE_ROUNDS:
            insights.append(
                self._insight(
                    "low",
                    "sample_size",
                    "Sample size is still small; treat trend signals as preliminary.",
                    f"Rounds played: {int(rounds_played)} (< {SMALL_SAMPLE_ROUNDS})",
                    "Collect more rounds before making major role/strategy changes.",
                )
            )

        if kd >= 1.2 and match_win_pct < 50.0:
            insights.append(
                self._insight(
                    "medium",
                    "conversion",
                    "Fragging output is not converting into wins consistently.",
                    f"K/D: {kd:.2f}, Win%: {match_win_pct:.1f}",
                    "Review objective/utility timing and post-plant conversion discipline.",
                )
            )

        if kd < 0.9 and match_win_pct >= 52.0:
            insights.append(
                self._insight(
                    "low",
                    "conversion",
                    "Team outcomes are strong despite lower K/D; value impact is likely non-frag focused.",
                    f"K/D: {kd:.2f}, Win%: {match_win_pct:.1f}",
                    "Keep enabling roles and utility value while gradually improving duel efficiency.",
                )
            )

        if entry_efficiency < 0.45 and aggression_score >= 0.20:
            insights.append(
                self._insight(
                    "medium",
                    "opening",
                    "Opening duel volume is high but conversion is weak.",
                    f"Entry efficiency: {entry_efficiency:.2f}, Aggression: {aggression_score:.2f}",
                    "Tighten entry pathing/trade setups or shift first-contact burden.",
                )
            )
        elif entry_efficiency >= 0.58 and aggression_score >= 0.18:
            insights.append(
                self._insight(
                    "low",
                    "opening",
                    "Opening duel profile is a team asset.",
                    f"Entry efficiency: {entry_efficiency:.2f}, Aggression: {aggression_score:.2f}",
                    "Build executes around early-map control generated by entries.",
                )
            )

        if clutch_attempt_rate >= CLUTCH_LOW_SUCCESS_ATTEMPT_RATE and overall_clutch_success < CLUTCH_LOW_SUCCESS_THRESHOLD:
            insights.append(
                self._insight(
                    "medium",
                    "clutch",
                    "Clutch workload is high, but conversion is currently low.",
                    f"Clutch success: {overall_clutch_success:.2f}, Attempt rate: {clutch_attempt_rate:.2f}",
                    "Prioritize comm clarity and isolate 1vX decision trees.",
                )
            )
        elif overall_clutch_success >= CLUTCH_STRONG_SUCCESS_THRESHOLD and clutch_attempts >= CLUTCH_MIN_STRONG_SAMPLE:
            insights.append(
                self._insight(
                    "low",
                    "clutch",
                    "Clutch conversion profile is reliably strong.",
                    f"Clutch wins/attempts: {int(round(overall_clutch_success * clutch_attempts))}/{clutch_attempts}",
                    "Leverage this player in late-round resource planning.",
                )
            )

        if teamplay_index < 0.12 and assists_per_round < 0.18:
            insights.append(
                self._insight(
                    "low",
                    "teamplay",
                    "Assist contribution is low relative to frag share.",
                    f"Teamplay index: {teamplay_index:.2f}, Assists/Round: {assists_per_round:.2f}",
                    "Increase trade spacing and utility-enabled setups.",
                )
            )

        tk_rate = (teamkills / rounds_played) if rounds_played > 0 else 0.0
        for threshold, severity in TEAMKILL_SEVERITY_BANDS:
            if tk_rate >= threshold:
                insights.append(
                    self._insight(
                        severity,
                        "discipline",
                        "Teamkill rate is creating avoidable round risk.",
                        f"Teamkills/Rounds: {int(teamkills)}/{int(rounds_played)} ({tk_rate:.2%})",
                        "Reinforce crossfire spacing and utility callouts before contact.",
                    )
                )
                break

        if not metrics.get("time_played_unreliable", False):
            wins_per_hour_value = self._as_float(wins_per_hour) if wins_per_hour is not None else 0.0
            if time_played_hours >= EFFICIENCY_MIN_HOURS and wins_per_hour_value < EFFICIENCY_LOW_WINS_PER_HOUR:
                insights.append(
                    self._insight(
                        "low",
                        "efficiency",
                        "Long-duration sample shows low win conversion per hour.",
                        f"Wins/Hour: {wins_per_hour_value:.2f}, Hours: {time_played_hours:.1f}",
                        "Review queue quality and objective-close discipline in late rounds.",
                    )
                )

        if impact_rating >= 1.10 and match_win_pct < 50.0:
            insights.append(
                self._insight(
                    "low",
                    "impact",
                    "Individual impact is high, but match conversion is lagging.",
                    f"Impact rating: {impact_rating:.2f}, Win%: {match_win_pct:.1f}",
                    "Prioritize round-winning decision points over stat padding.",
                )
            )

        overall_conf = metrics.get("overall_conf")
        if overall_conf is not None and self._as_float(overall_conf) < LOW_CONFIDENCE_OVERALL:
            insights.append(
                self._insight(
                    "info",
                    "confidence",
                    "Role-confidence signal is currently weak.",
                    f"Overall confidence: {self._as_float(overall_conf):.2f}",
                    "Gather more representative matches before locking role conclusions.",
                )
            )

        if (
            clutch_attempt_rate >= CLUTCH_BURDEN_ATTEMPT_RATE
            and clutch_attempts >= CLUTCH_BURDEN_MIN_ATTEMPTS
            and disadv_attempt_share >= CLUTCH_BURDEN_DISADV_SHARE
        ):
            insights.append(
                self._insight(
                    "medium",
                    "clutch_burden",
                    "Player is absorbing an unusually high disadvantaged-clutch burden.",
                    f"Disadvantaged attempts: {disadv_attempts}/{clutch_attempts}",
                    "Reduce isolated late-round scenarios through earlier support timing.",
                )
            )

        if extreme_attempts >= EXTREME_CLUTCH_MIN_ATTEMPTS and clutch_attempts >= EXTREME_CLUTCH_MIN_TOTAL_ATTEMPTS:
            insights.append(
                self._insight(
                    "medium",
                    "extreme_clutch",
                    "Frequent 1v4/1v5 states indicate high late-round pressure exposure.",
                    f"1v4/1v5 attempts: {extreme_attempts}",
                    "Stabilize mid-round numbers to avoid repeated extreme clutch states.",
                )
            )

        if hp_attempt_rate >= HIGH_PRESSURE_MIN_ATTEMPT_RATE and hp_attempts >= HIGH_PRESSURE_MIN_ATTEMPTS:
            if hp_success <= HIGH_PRESSURE_LOW_SUCCESS:
                insights.append(
                    self._insight(
                        "medium",
                        "high_pressure",
                        "High-pressure clutch conversion is below target.",
                        f"High-pressure wins/attempts: {hp_wins}/{hp_attempts}; attempts/rounds: {hp_attempts}/{int(rounds_played)}",
                        "Improve man-disadvantage protocols and retake timing.",
                    )
                )
            elif hp_success >= HIGH_PRESSURE_STRONG_SUCCESS:
                insights.append(
                    self._insight(
                        "low",
                        "high_pressure",
                        "High-pressure clutch conversion is a reliable strength.",
                        f"High-pressure wins/attempts: {hp_wins}/{hp_attempts}; attempts/rounds: {hp_attempts}/{int(rounds_played)}",
                        "Preserve this advantage with supportive utility in late rounds.",
                    )
                )

        if not insights:
            insights.append(
                self._insight(
                    "info",
                    "baseline",
                    "No major risk flags detected in current sample.",
                    f"Rounds: {int(rounds_played)}, K/D: {kd:.2f}, Win%: {match_win_pct:.1f}",
                    "Maintain current profile and monitor for trend changes.",
                )
            )

        return insights

    @staticmethod
    def _insight(severity: str, category: str, message: str, evidence: str, action: str) -> Dict[str, str]:
        return {
            "severity": severity,
            "category": category,
            "message": message,
            "evidence": evidence,
            "action": action,
        }

    @staticmethod
    def _as_float(value: Any) -> float:
        try:
            if value is None:
                return 0.0
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _parse_clutches(raw: Any) -> Dict[str, int]:
        if isinstance(raw, dict):
            return raw
        if not raw:
            return {}
        try:
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, TypeError):
            return {}
