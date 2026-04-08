from __future__ import annotations


def pp(value: float) -> str:
    sign = "+" if float(value) >= 0 else ""
    return f"{sign}{float(value):.1f}pp"


def pair_label(a: str, b: str) -> str:
    return f"{a}+{b}"


def defense_skewed_message(a: str, b: str, overall_pp: float, atk_pp: float) -> str:
    return (
        f"{pair_label(a, b)} is {pp(overall_pp)} overall, but on Attack it is {pp(atk_pp)}. "
        "This suggests the edge may come from defensive setups/holds, not executes."
    )


def map_concentrated_message(a: str, b: str, map_name: str, share_pct: float) -> str:
    return (
        f"{pair_label(a, b)} shows positive results, but {share_pct:.1f}% of sample is on {map_name}. "
        "This suggests the edge may be map-dependent."
    )


def consistent_risk_message(a: str, b: str, rounds_n: int, delta_pp: float, volatility: float) -> str:
    return (
        f"Across {int(rounds_n)} rounds, {pair_label(a, b)} is {pp(delta_pp)} vs baseline with volatility {volatility:.2f}. "
        "This may be a comp risk unless roles change."
    )


def volatile_edge_message(a: str, b: str) -> str:
    return (
        f"{pair_label(a, b)} looks positive overall, but win rate swings match-to-match. "
        "This may indicate opponent-quality variance or inconsistent coordination."
    )


def side_imbalance_message(atk_wr: float, def_wr: float, weak_side: str) -> str:
    return (
        f"Your Attack win rate is {atk_wr:.1f}% vs Defense {def_wr:.1f}%. "
        f"Focus practice on {weak_side} to close the phase gap."
    )


def first_death_drop_message(teammate: str, delta_pp: float) -> str:
    return (
        f"When queued with {teammate}, your Attack first-death rate drops {abs(delta_pp):.1f}pp. "
        "This suggests better spacing/trading or safer entry pacing."
    )


def over_aggression_message(teammate: str, entry_delta_pp: float, wr_delta_pp: float) -> str:
    return (
        f"With {teammate}, your Attack entry involvement rises {entry_delta_pp:.1f}pp while win rate is {pp(wr_delta_pp)}. "
        "This may indicate over-aggressive pacing or role mismatch."
    )
