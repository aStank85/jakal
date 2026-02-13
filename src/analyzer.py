import json
from typing import Dict, Any, List

from src.thresholds import (
    SMALL_SAMPLE_ROUNDS,
    CLUTCH_LOW_SUCCESS_ATTEMPT_RATE,
    CLUTCH_LOW_SUCCESS_THRESHOLD,
    CLUTCH_STRONG_SUCCESS_THRESHOLD,
    CLUTCH_MIN_STRONG_SAMPLE,
    TEAMKILL_SEVERITY_BANDS,
    EFFICIENCY_MIN_HOURS,
    EFFICIENCY_LOW_WINS_PER_HOUR,
    HIGH_PRESSURE_MIN_ATTEMPT_RATE,
    HIGH_PRESSURE_MIN_ATTEMPTS,
    HIGH_PRESSURE_LOW_SUCCESS,
    HIGH_PRESSURE_STRONG_SUCCESS,
    LOW_CONFIDENCE_OVERALL,
    CLUTCH_BURDEN_ATTEMPT_RATE,
    CLUTCH_BURDEN_DISADV_SHARE,
    CLUTCH_BURDEN_MIN_ATTEMPTS,
    EXTREME_CLUTCH_MIN_ATTEMPTS,
    EXTREME_CLUTCH_MIN_TOTAL_ATTEMPTS,
)


class InsightAnalyzer:
    """Generate deterministic, rule-based insights from snapshot + metrics."""

    SEVERITY_ORDER = {
        "high": 0,
        "medium": 1,
        "low": 2,
        "info": 3,
    }

    @staticmethod
    def _safe_get(container: Dict[str, Any], key: str, default: Any = 0) -> Any:
        value = container.get(key, default)
        return value if value is not None else default

    @staticmethod
    def _parse_clutches(snapshot: Dict[str, Any]) -> Dict[str, int]:
        raw = snapshot.get("clutches_data", "{}")
        if not raw:
            parsed: Dict[str, Any] = {}
        else:
            try:
                parsed = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                parsed = {}

        if not isinstance(parsed, dict):
            parsed = {}

        keys = (
            "total", "1v1", "1v2", "1v3", "1v4", "1v5",
            "lost_total", "lost_1v1", "lost_1v2", "lost_1v3", "lost_1v4", "lost_1v5",
        )
        result: Dict[str, int] = {}
        for key in keys:
            try:
                result[key] = int(parsed.get(key, 0) or 0)
            except (TypeError, ValueError):
                result[key] = 0

        # Keep analyzer math aligned with calculator's computed-sum behavior.
        result["total"] = result["1v1"] + result["1v2"] + result["1v3"] + result["1v4"] + result["1v5"]
        result["lost_total"] = (
            result["lost_1v1"] + result["lost_1v2"] + result["lost_1v3"] + result["lost_1v4"] + result["lost_1v5"]
        )
        return result

    def _insight(
        self,
        severity: str,
        category: str,
        message: str,
        evidence: str,
        action: str,
    ) -> Dict[str, str]:
        return {
            "severity": severity,
            "category": category,
            "message": message,
            "evidence": evidence,
            "action": action,
        }

    def _teamkill_severity(self, teamkill_rate: float) -> str:
        for threshold, severity in TEAMKILL_SEVERITY_BANDS:
            if teamkill_rate >= threshold:
                return severity
        return ""

    def generate_insights(self, snapshot: Dict[str, Any], metrics: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Return ordered, deterministic insights.

        Insight schema:
        - severity: high|medium|low|info
        - category: domain bucket
        - message: short summary
        - evidence: concrete metric evidence
        - action: practical next step
        """
        insights: List[Dict[str, str]] = []

        rounds = self._safe_get(snapshot, "rounds_played")
        match_win_pct = float(self._safe_get(snapshot, "match_win_pct", 0.0))
        kd = float(self._safe_get(snapshot, "kd", 0.0))
        assists_per_round = float(self._safe_get(snapshot, "assists_per_round", 0.0))
        teamkills = int(self._safe_get(snapshot, "teamkills", 0))
        time_played_hours = float(self._safe_get(snapshot, "time_played_hours", 0.0))

        entry_eff = float(self._safe_get(metrics, "entry_efficiency", 0.0))
        aggression = float(self._safe_get(metrics, "aggression_score", 0.0))
        clutch_attempt_rate = float(self._safe_get(metrics, "clutch_attempt_rate", 0.0))
        overall_clutch_success = float(self._safe_get(metrics, "overall_clutch_success", 0.0))
        teamplay_index = float(self._safe_get(metrics, "teamplay_index", 0.0))
        wins_per_hour = float(self._safe_get(metrics, "wins_per_hour", 0.0))
        impact_rating = float(self._safe_get(metrics, "impact_rating", 0.0))
        primary_role = str(self._safe_get(metrics, "primary_role", "Unknown"))
        overall_conf = float(self._safe_get(metrics, "overall_conf", 0.0))
        opening_net_per_round = float(self._safe_get(metrics, "opening_net_per_round", 0.0))
        high_pressure_success = float(self._safe_get(metrics, "high_pressure_success", 0.0))
        high_pressure_attempt_rate = float(self._safe_get(metrics, "high_pressure_attempt_rate", 0.0))
        high_pressure_attempts = int(self._safe_get(metrics, "high_pressure_attempts", 0))
        high_pressure_wins = int(self._safe_get(metrics, "high_pressure_wins", 0))
        risk_index = float(self._safe_get(metrics, "risk_index", 0.0))
        clean_play_index = float(self._safe_get(metrics, "clean_play_index", 1.0))
        clutch_attempts = int(self._safe_get(metrics, "clutch_attempts", 0))
        disadv_attempts = int(self._safe_get(metrics, "disadv_attempts", 0))
        disadv_attempt_share = float(self._safe_get(metrics, "disadv_attempt_share", 0.0))
        extreme_attempts = int(self._safe_get(metrics, "extreme_attempts", 0))
        time_played_unreliable = bool(self._safe_get(metrics, "time_played_unreliable", False))

        clutches = self._parse_clutches(snapshot)
        clutch_wins = int(clutches.get("total", 0))
        clutch_losses = int(clutches.get("lost_total", 0))
        extreme_wins = int(clutches.get("1v4", 0)) + int(clutches.get("1v5", 0))

        if rounds < SMALL_SAMPLE_ROUNDS:
            insights.append(
                self._insight(
                    severity="info",
                    category="sample_size",
                    message="Small sample size can distort role and trend signals.",
                    evidence=f"Rounds played: {rounds}",
                    action="Collect more snapshots before making major playstyle changes.",
                )
            )

        if kd >= 1.2 and match_win_pct < 50.0:
            insights.append(
                self._insight(
                    severity="high",
                    category="winning",
                    message="Fragging output is not converting into wins consistently.",
                    evidence=f"K/D: {kd:.2f}, Match Win %: {match_win_pct:.1f}",
                    action="Play closer to objective timing and trade paths rather than isolated picks.",
                )
            )
        elif kd < 0.9 and match_win_pct >= 52.0:
            insights.append(
                self._insight(
                    severity="low",
                    category="winning",
                    message="You are winning despite lower K/D, likely through utility and team timing.",
                    evidence=f"K/D: {kd:.2f}, Match Win %: {match_win_pct:.1f}",
                    action="Keep role discipline; focus on survivability and late-round utility value.",
                )
            )

        if entry_eff < 0.45 and aggression >= 0.20:
            insights.append(
                self._insight(
                    severity="high",
                    category="entry",
                    message="Opening duel conversion is too low for your aggression level.",
                    evidence=f"Entry Efficiency: {entry_eff:.2f}, Aggression: {aggression:.2f}",
                    action="Reduce first-contact frequency until crosshair placement and trade support stabilize.",
                )
            )
        elif entry_eff >= 0.58 and aggression >= 0.18:
            insights.append(
                self._insight(
                    severity="low",
                    category="entry",
                    message="Strong opening duel profile.",
                    evidence=f"Entry Efficiency: {entry_eff:.2f}, Aggression: {aggression:.2f}",
                    action="Keep taking first contacts with a dedicated trade partner.",
                )
            )

        if clutch_attempt_rate >= CLUTCH_LOW_SUCCESS_ATTEMPT_RATE and overall_clutch_success < CLUTCH_LOW_SUCCESS_THRESHOLD:
            insights.append(
                self._insight(
                    severity="medium",
                    category="clutch",
                    message="You face many clutches but conversion is low.",
                    evidence=(
                        f"Overall Clutches: {clutch_wins}/{clutch_wins + clutch_losses} wins ({overall_clutch_success:.2f}), "
                        f"Rate: {clutch_wins + clutch_losses}/{rounds} ({clutch_attempt_rate:.2f})"
                    ),
                    action="Prioritize early-round survival and repositioning to avoid repeated disadvantage states.",
                )
            )
        elif overall_clutch_success >= CLUTCH_STRONG_SUCCESS_THRESHOLD and (clutch_wins + clutch_losses) >= CLUTCH_MIN_STRONG_SAMPLE:
            insights.append(
                self._insight(
                    severity="low",
                    category="clutch",
                    message="Above-average clutch conversion.",
                    evidence=f"Clutch wins: {clutch_wins}/{clutch_wins + clutch_losses} ({overall_clutch_success:.2f})",
                    action="Lean into late-round decision making and information denial setups.",
                )
            )

        if (
            clutch_attempt_rate >= CLUTCH_BURDEN_ATTEMPT_RATE
            and disadv_attempt_share >= CLUTCH_BURDEN_DISADV_SHARE
            and clutch_attempts >= CLUTCH_BURDEN_MIN_ATTEMPTS
        ):
            insights.append(
                self._insight(
                    severity="high",
                    category="clutch_burden",
                    message="Clutch burden is high.",
                    evidence=f"Disadvantaged clutches: {disadv_attempts}/{clutch_attempts} ({disadv_attempt_share:.2f})",
                    action="Play closer to trade support; avoid isolated late-round positions; preserve a teammate for crossfires.",
                )
            )

        if extreme_attempts >= EXTREME_CLUTCH_MIN_ATTEMPTS and clutch_attempts >= EXTREME_CLUTCH_MIN_TOTAL_ATTEMPTS:
            severity = "high" if extreme_wins == 0 else "medium"
            insights.append(
                self._insight(
                    severity=severity,
                    category="extreme_clutch",
                    message="Extreme clutches (1v4/1v5) are frequent.",
                    evidence=f"1v4/1v5 attempts: {extreme_attempts}, wins: {extreme_wins}",
                    action="Prioritize early-man-count discipline and trade spacing to reduce impossible end-round states.",
                )
            )

        if teamplay_index < 0.12 and assists_per_round < 0.18:
            insights.append(
                self._insight(
                    severity="medium",
                    category="teamplay",
                    message="Support contribution is low relative to elimination profile.",
                    evidence=f"Teamplay Index: {teamplay_index:.2f}, Assists/Round: {assists_per_round:.2f}",
                    action="Increase utility-enabled fights and intentional trade positioning.",
                )
            )

        discipline_emitted = False
        if rounds > 0:
            teamkill_rate = teamkills / rounds
            teamkill_severity = self._teamkill_severity(teamkill_rate)
            if teamkill_severity:
                discipline_emitted = True
                message = "Teamkill rate is high enough to affect round outcomes." if teamkill_severity == "medium" else "Teamkill rate is elevated and worth monitoring."
                action = (
                    "Tighten crossfire comms and avoid swinging through teammate lines."
                    if teamkill_severity == "medium"
                    else "Improve crossfire spacing and call teammate pathing before swings."
                )
                insights.append(
                    self._insight(
                        severity=teamkill_severity,
                        category="discipline",
                        message=message,
                        evidence=f"Teamkills: {teamkills}/{rounds} ({teamkill_rate:.2%}), Clean Play Index: {clean_play_index:.2f}",
                        action=action,
                    )
                )

        if not discipline_emitted and clean_play_index < 0.5:
            insights.append(
                self._insight(
                    severity="medium",
                    category="discipline",
                    message="Clean play score is low and may be costing rounds.",
                    evidence=f"Clean Play Index: {clean_play_index:.2f}",
                    action="Tighten crosshair discipline around teammate pathing and utility execution timing.",
                )
            )

        if (not time_played_unreliable) and time_played_hours >= EFFICIENCY_MIN_HOURS and wins_per_hour < EFFICIENCY_LOW_WINS_PER_HOUR:
            insights.append(
                self._insight(
                    severity="low",
                    category="efficiency",
                    message="Match efficiency is low for current playtime volume.",
                    evidence=f"Wins/Hour: {wins_per_hour:.2f}, Time Played: {time_played_hours:.0f}h",
                    action="Use shorter, focused sessions with role-specific goals instead of long grind blocks.",
                )
            )

        if impact_rating >= 1.10 and match_win_pct < 50.0:
            insights.append(
                self._insight(
                    severity="medium",
                    category="impact",
                    message="Per-round impact is high but not reflected in match outcomes.",
                    evidence=f"Impact Rating: {impact_rating:.2f}, Match Win %: {match_win_pct:.1f}",
                    action="Convert impact into objective control and post-plant discipline.",
                )
            )

        if opening_net_per_round < -0.03:
            insights.append(
                self._insight(
                    severity="medium",
                    category="opening_control",
                    message="Opening duel outcomes are currently net negative.",
                    evidence=f"Opening Net/Round: {opening_net_per_round:.2f}",
                    action="Reduce unsupported early peeks and prioritize info utility before first contact.",
                )
            )
        elif opening_net_per_round > 0.03:
            insights.append(
                self._insight(
                    severity="low",
                    category="opening_control",
                    message="You are creating positive opening-round momentum.",
                    evidence=f"Opening Net/Round: {opening_net_per_round:.2f}",
                    action="Keep pairing first-contact pressure with immediate trade setups.",
                )
            )

        if (high_pressure_attempts >= HIGH_PRESSURE_MIN_ATTEMPTS and high_pressure_attempt_rate >= HIGH_PRESSURE_MIN_ATTEMPT_RATE and high_pressure_success < HIGH_PRESSURE_LOW_SUCCESS):
            insights.append(
                self._insight(
                    severity="medium",
                    category="high_pressure",
                    message="High-pressure clutch conversion is low.",
                    evidence=(
                        f"High Pressure: {high_pressure_wins}/{high_pressure_attempts} wins ({high_pressure_success:.2f}), "
                        f"Rate: {high_pressure_attempts}/{rounds} ({high_pressure_attempt_rate:.2f})"
                    ),
                    action="Prioritize isolating 1v1s and preserving utility for final 20 seconds.",
                )
            )
        elif (high_pressure_attempts >= HIGH_PRESSURE_MIN_ATTEMPTS and high_pressure_attempt_rate >= HIGH_PRESSURE_MIN_ATTEMPT_RATE and high_pressure_success >= HIGH_PRESSURE_STRONG_SUCCESS):
            insights.append(
                self._insight(
                    severity="low",
                    category="high_pressure",
                    message="Strong high-pressure clutch profile.",
                    evidence=(
                        f"High Pressure: {high_pressure_wins}/{high_pressure_attempts} wins ({high_pressure_success:.2f}), "
                        f"Rate: {high_pressure_attempts}/{rounds} ({high_pressure_attempt_rate:.2f})"
                    ),
                    action="Lean into late-round reads and force isolated duels.",
                )
            )

        if risk_index > 0.45 and kd < 1.0:
            insights.append(
                self._insight(
                    severity="medium",
                    category="risk",
                    message="Current fight selection profile is high risk for your conversion rate.",
                    evidence=f"Risk Index: {risk_index:.2f}, K/D: {kd:.2f}",
                    action="Cut low-value re-peeks and play more refrag-ready positions.",
                )
            )

        if "overall_conf" in metrics and overall_conf < LOW_CONFIDENCE_OVERALL:
            insights.append(
                self._insight(
                    severity="info",
                    category="confidence",
                    message="Insight confidence is currently limited by sample size/volume.",
                    evidence=f"Overall Confidence: {overall_conf:.2f}",
                    action="Add more snapshots and clutch volume before making major strategic adjustments.",
                )
            )

        if not insights:
            insights.append(
                self._insight(
                    severity="info",
                    category="baseline",
                    message="No major risk flags from current snapshot.",
                    evidence=f"Primary Role: {primary_role}",
                    action="Keep collecting snapshots for trend-based insights.",
                )
            )

        insights.sort(key=lambda insight: (self.SEVERITY_ORDER.get(insight["severity"], 99), insight["category"]))
        return insights

