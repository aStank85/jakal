import json
from typing import Dict, Any, List


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
    def _parse_clutches(snapshot: Dict[str, Any]) -> Dict[str, Any]:
        raw = snapshot.get("clutches_data", "{}")
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {}

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
        risk_index = float(self._safe_get(metrics, "risk_index", 0.0))
        clean_play_index = float(self._safe_get(metrics, "clean_play_index", 1.0))

        clutches = self._parse_clutches(snapshot)
        clutch_wins = int(clutches.get("total", 0))
        clutch_losses = int(clutches.get("lost_total", 0))

        if rounds < 120:
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

        if clutch_attempt_rate >= 0.12 and overall_clutch_success < 0.22:
            insights.append(
                self._insight(
                    severity="medium",
                    category="clutch",
                    message="You face many clutches but conversion is low.",
                    evidence=f"Attempt Rate: {clutch_attempt_rate:.2f}, Success: {overall_clutch_success:.2f}",
                    action="Prioritize early-round survival and repositioning to avoid repeated disadvantage states.",
                )
            )
        elif overall_clutch_success >= 0.30 and (clutch_wins + clutch_losses) >= 20:
            insights.append(
                self._insight(
                    severity="low",
                    category="clutch",
                    message="Above-average clutch conversion.",
                    evidence=f"Clutch wins/losses: {clutch_wins}/{clutch_losses}, Success: {overall_clutch_success:.2f}",
                    action="Lean into late-round decision making and information denial setups.",
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

        if rounds > 0:
            teamkill_rate = teamkills / rounds
            if teamkill_rate >= 0.02:
                insights.append(
                    self._insight(
                        severity="medium",
                        category="discipline",
                        message="Teamkill rate is high enough to affect round outcomes.",
                        evidence=f"Teamkills: {teamkills}, Rounds: {rounds}",
                        action="Tighten crossfire comms and avoid swinging through teammate lines.",
                    )
                )
            elif teamkill_rate >= 0.01:
                insights.append(
                    self._insight(
                        severity="low",
                        category="discipline",
                        message="Teamkill rate is elevated and worth monitoring.",
                        evidence=f"Teamkills: {teamkills}, Rounds: {rounds}",
                        action="Improve crossfire spacing and call teammate pathing before swings.",
                    )
                )

        if time_played_hours >= 50 and wins_per_hour < 0.12:
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

        if high_pressure_attempt_rate >= 0.03 and high_pressure_success < 0.12:
            insights.append(
                self._insight(
                    severity="medium",
                    category="high_pressure",
                    message="High-pressure clutch conversion is low.",
                    evidence=f"High Pressure Attempt Rate: {high_pressure_attempt_rate:.2f}, Success: {high_pressure_success:.2f}",
                    action="Prioritize isolating 1v1s and preserving utility for final 20 seconds.",
                )
            )
        elif high_pressure_attempt_rate >= 0.03 and high_pressure_success >= 0.20:
            insights.append(
                self._insight(
                    severity="low",
                    category="high_pressure",
                    message="Strong high-pressure clutch profile.",
                    evidence=f"High Pressure Attempt Rate: {high_pressure_attempt_rate:.2f}, Success: {high_pressure_success:.2f}",
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

        if clean_play_index < 0.5:
            insights.append(
                self._insight(
                    severity="medium",
                    category="discipline",
                    message="Clean play score is low and may be costing rounds.",
                    evidence=f"Clean Play Index: {clean_play_index:.2f}",
                    action="Tighten crosshair discipline around teammate pathing and utility execution timing.",
                )
            )

        if "overall_conf" in metrics and overall_conf < 0.45:
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
