from __future__ import annotations

import math
from collections import defaultdict
from itertools import combinations
from typing import Any


def _iter_chunks(values: list[str], size: int = 800):
    step = max(1, int(size))
    for i in range(0, len(values), step):
        yield values[i : i + step]


def _norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(float(z) / math.sqrt(2.0)))


def _prob_and_pvalue(wins: int, n: int, p0: float) -> tuple[float, float]:
    n_safe = max(1, int(n))
    p0_safe = min(0.999, max(0.001, float(p0)))
    phat = max(0.0, min(1.0, float(wins) / float(n_safe)))
    se = math.sqrt(max(1e-9, p0_safe * (1.0 - p0_safe) / n_safe))
    z = (phat - p0_safe) / se
    prob_above = _norm_cdf(z)
    p_value = max(0.0, min(1.0, 2.0 * (1.0 - _norm_cdf(abs(z)))))
    return prob_above, p_value


def _stddev01(values: list[int]) -> float:
    if not values:
        return 0.0
    n = len(values)
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    return math.sqrt(max(0.0, var))


def build_insight_features(
    *,
    cur: Any,
    username: str,
    player_id: int,
    match_ids: list[str],
    team_pairs_overall: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    username_l = str(username or "").strip().lower()
    ids = [str(m or "").strip() for m in (match_ids or []) if str(m or "").strip()]
    if not username_l or int(player_id or 0) <= 0 or not ids:
        return {
            "baseline": {"p0": 0.0, "wins": 0, "rounds": 0},
            "pairs_overall": [],
            "pairs_by_side": [],
            "pairs_by_map": [],
            "player_side_profile": {},
            "pair_entry_effect": [],
            "scope": {"match_ids": 0},
        }

    me_match_rows: list[dict[str, Any]] = []
    teammate_rows: list[dict[str, Any]] = []
    side_rows: list[dict[str, Any]] = []
    map_by_match: dict[str, str] = {}

    for chunk in _iter_chunks(ids):
        ph = ",".join("?" for _ in chunk)
        cur.execute(
            f"""
            SELECT me.match_id, LOWER(TRIM(COALESCE(me.result, ''))) AS result
            FROM match_detail_players me
            WHERE LOWER(TRIM(me.username)) = LOWER(TRIM(?))
              AND me.match_id IN ({ph})
            """,
            (username_l, *chunk),
        )
        me_match_rows.extend(dict(r) for r in cur.fetchall())

        cur.execute(
            f"""
            SELECT me.match_id, LOWER(TRIM(tm.username)) AS teammate
            FROM match_detail_players me
            JOIN match_detail_players tm
              ON tm.match_id = me.match_id
             AND tm.team_id = me.team_id
             AND LOWER(TRIM(tm.username)) != LOWER(TRIM(me.username))
            WHERE LOWER(TRIM(me.username)) = LOWER(TRIM(?))
              AND me.match_id IN ({ph})
            """,
            (username_l, *chunk),
        )
        teammate_rows.extend(dict(r) for r in cur.fetchall())

        cur.execute(
            f"""
            SELECT
                pr.match_id,
                LOWER(TRIM(COALESCE(pr.side, ''))) AS side,
                COUNT(*) AS rounds_n,
                SUM(CASE WHEN LOWER(TRIM(COALESCE(pr.result, ''))) IN ('win', 'victory') THEN 1 ELSE 0 END) AS wins_n,
                SUM(COALESCE(pr.first_blood, 0)) AS fb_n,
                SUM(COALESCE(pr.first_death, 0)) AS fd_n
            FROM player_rounds pr
            WHERE pr.player_id = ?
              AND pr.match_id IN ({ph})
            GROUP BY pr.match_id, LOWER(TRIM(COALESCE(pr.side, '')))
            """,
            (player_id, *chunk),
        )
        side_rows.extend(dict(r) for r in cur.fetchall())

        cur.execute(
            f"""
            WITH latest AS (
                SELECT match_id, MAX(scraped_at) AS scraped_at
                FROM scraped_match_cards
                WHERE match_id IN ({ph})
                GROUP BY match_id
            )
            SELECT smc.match_id, COALESCE(NULLIF(TRIM(smc.map_name), ''), 'unknown') AS map_name
            FROM scraped_match_cards smc
            JOIN latest l
              ON l.match_id = smc.match_id
             AND l.scraped_at = smc.scraped_at
            """,
            (*chunk,),
        )
        for r in cur.fetchall():
            map_by_match[str(r["match_id"])] = str(r["map_name"] or "unknown")

    did_win_by_match: dict[str, int] = {}
    for r in me_match_rows:
        mid = str(r.get("match_id") or "").strip()
        if not mid:
            continue
        did_win_by_match[mid] = 1 if str(r.get("result") or "").lower() in {"win", "victory"} else 0

    rounds_by_match: dict[str, int] = defaultdict(int)
    atk_stats_by_match: dict[str, dict[str, int]] = defaultdict(lambda: {"rounds": 0, "wins": 0, "fb": 0, "fd": 0})
    def_stats_by_match: dict[str, dict[str, int]] = defaultdict(lambda: {"rounds": 0, "wins": 0})
    total_rounds = 0
    total_wins = 0
    atk_rounds = 0
    atk_wins = 0
    atk_fb = 0
    atk_fd = 0
    def_rounds = 0
    def_wins = 0

    for r in side_rows:
        mid = str(r.get("match_id") or "").strip()
        side = str(r.get("side") or "").lower()
        n = int(r.get("rounds_n") or 0)
        w = int(r.get("wins_n") or 0)
        fb = int(r.get("fb_n") or 0)
        fd = int(r.get("fd_n") or 0)
        rounds_by_match[mid] += n
        total_rounds += n
        total_wins += w
        if side in {"attacker", "atk"}:
            atk_stats_by_match[mid]["rounds"] += n
            atk_stats_by_match[mid]["wins"] += w
            atk_stats_by_match[mid]["fb"] += fb
            atk_stats_by_match[mid]["fd"] += fd
            atk_rounds += n
            atk_wins += w
            atk_fb += fb
            atk_fd += fd
        if side in {"defender", "def"}:
            def_stats_by_match[mid]["rounds"] += n
            def_stats_by_match[mid]["wins"] += w
            def_rounds += n
            def_wins += w

    p0 = (total_wins / total_rounds) if total_rounds > 0 else 0.0
    p0_atk = (atk_wins / atk_rounds) if atk_rounds > 0 else 0.0
    p0_def = (def_wins / def_rounds) if def_rounds > 0 else 0.0

    mates_by_match: dict[str, set[str]] = defaultdict(set)
    for r in teammate_rows:
        mid = str(r.get("match_id") or "").strip()
        tm = str(r.get("teammate") or "").strip().lower()
        if mid and tm:
            mates_by_match[mid].add(tm)

    pair_match_outcomes: dict[tuple[str, str], list[int]] = defaultdict(list)
    pair_agg: dict[tuple[str, str], dict[str, Any]] = {}
    pair_side_agg: dict[tuple[str, str, str], dict[str, Any]] = {}
    pair_map_agg: dict[tuple[str, str, str], dict[str, Any]] = {}
    map_baseline: dict[str, dict[str, int]] = defaultdict(lambda: {"wins": 0, "rounds": 0})

    for mid, mates in mates_by_match.items():
        if len(mates) < 2:
            continue
        did_win = int(did_win_by_match.get(mid, 0))
        rounds_n = int(rounds_by_match.get(mid, 0))
        map_name = str(map_by_match.get(mid, "unknown") or "unknown")
        map_baseline[map_name]["wins"] += int(
            atk_stats_by_match.get(mid, {}).get("wins", 0) + def_stats_by_match.get(mid, {}).get("wins", 0)
        )
        map_baseline[map_name]["rounds"] += int(
            atk_stats_by_match.get(mid, {}).get("rounds", 0) + def_stats_by_match.get(mid, {}).get("rounds", 0)
        )
        atk = atk_stats_by_match.get(mid, {})
        deff = def_stats_by_match.get(mid, {})
        for a, b in combinations(sorted(mates), 2):
            key = (a, b)
            pair_match_outcomes[key].append(did_win)
            rec = pair_agg.setdefault(
                key,
                {"a": a, "b": b, "pair": f"{a} + {b}", "matches_n": 0, "wins_n": 0, "rounds_n": 0},
            )
            rec["matches_n"] += 1
            rec["wins_n"] += did_win
            rec["rounds_n"] += rounds_n

            for side_name, side_block in (("attack", atk), ("defense", deff)):
                skey = (a, b, side_name)
                srec = pair_side_agg.setdefault(
                    skey,
                    {"a": a, "b": b, "pair": f"{a} + {b}", "side": side_name, "matches_n": 0, "wins_n": 0, "rounds_n": 0},
                )
                srec["matches_n"] += 1
                srec["wins_n"] += int(side_block.get("wins", 0))
                srec["rounds_n"] += int(side_block.get("rounds", 0))

            mkey = (a, b, map_name)
            mrec = pair_map_agg.setdefault(
                mkey,
                {"a": a, "b": b, "pair": f"{a} + {b}", "map_name": map_name, "matches_n": 0, "wins_n": 0, "rounds_n": 0},
            )
            mrec["matches_n"] += 1
            mrec["wins_n"] += int(atk.get("wins", 0) + deff.get("wins", 0))
            mrec["rounds_n"] += int(atk.get("rounds", 0) + deff.get("rounds", 0))

    base_pairs: list[dict[str, Any]]
    if team_pairs_overall:
        base_pairs = []
        for r in team_pairs_overall:
            a = str(r.get("teammate_a") or "").strip().lower()
            b = str(r.get("teammate_b") or "").strip().lower()
            key = (a, b) if a <= b else (b, a)
            wins_n = int(r.get("wins_n") or 0)
            matches_n = int(r.get("matches_n") or 0)
            rounds_n = int(r.get("rounds_n") or 0)
            wr = float(r.get("win_rate") or 0.0)
            prob, pval = _prob_and_pvalue(wins_n, max(1, matches_n), p0)
            vol = _stddev01(pair_match_outcomes.get(key, []))
            base_pairs.append(
                {
                    "a": key[0],
                    "b": key[1],
                    "pair": str(r.get("pair") or f"{key[0]} + {key[1]}"),
                    "matches_n": matches_n,
                    "wins_n": wins_n,
                    "rounds_n": rounds_n,
                    "win_rate": wr,
                    "delta_pp": wr - (p0 * 100.0),
                    "prob_above_baseline": prob,
                    "p_value": pval,
                    "volatility": vol,
                }
            )
    else:
        base_pairs = []
        for key, rec in pair_agg.items():
            matches_n = int(rec["matches_n"])
            wins_n = int(rec["wins_n"])
            rounds_n = int(rec["rounds_n"])
            wr = (wins_n / matches_n) * 100.0 if matches_n > 0 else 0.0
            prob, pval = _prob_and_pvalue(wins_n, max(1, matches_n), p0)
            base_pairs.append(
                {
                    **rec,
                    "win_rate": wr,
                    "delta_pp": wr - (p0 * 100.0),
                    "prob_above_baseline": prob,
                    "p_value": pval,
                    "volatility": _stddev01(pair_match_outcomes.get(key, [])),
                }
            )

    base_pairs.sort(key=lambda x: (-int(x["rounds_n"]), -abs(float(x["delta_pp"])), -int(x["matches_n"]), str(x["pair"])))
    base_pairs = base_pairs[:80]
    pair_keep = {(str(r["a"]), str(r["b"])) for r in base_pairs}

    pairs_by_side: list[dict[str, Any]] = []
    for (_a, _b, side_name), rec in pair_side_agg.items():
        a = str(rec["a"])
        b = str(rec["b"])
        if (a, b) not in pair_keep:
            continue
        rounds_n = int(rec["rounds_n"])
        wins_n = int(rec["wins_n"])
        wr = (wins_n / rounds_n) * 100.0 if rounds_n > 0 else 0.0
        base = p0_atk if side_name == "attack" else p0_def
        prob, pval = _prob_and_pvalue(wins_n, max(1, rounds_n), base)
        pairs_by_side.append(
            {
                **rec,
                "win_rate": wr,
                "baseline_p0": base,
                "delta_pp": wr - (base * 100.0),
                "prob_above_baseline": prob,
                "p_value": pval,
            }
        )

    map_rounds = sorted(
        ((name, int(v["rounds"])) for name, v in map_baseline.items() if int(v["rounds"]) > 0),
        key=lambda x: x[1],
        reverse=True,
    )
    keep_maps = {name for name, _ in map_rounds[:12]}
    pairs_by_map: list[dict[str, Any]] = []
    for (_a, _b, map_name), rec in pair_map_agg.items():
        a = str(rec["a"])
        b = str(rec["b"])
        if (a, b) not in pair_keep or map_name not in keep_maps:
            continue
        rounds_n = int(rec["rounds_n"])
        wins_n = int(rec["wins_n"])
        wr = (wins_n / rounds_n) * 100.0 if rounds_n > 0 else 0.0
        mb = map_baseline.get(map_name, {"wins": 0, "rounds": 0})
        p0_map = (int(mb["wins"]) / int(mb["rounds"])) if int(mb["rounds"]) > 0 else 0.0
        prob, pval = _prob_and_pvalue(wins_n, max(1, rounds_n), p0_map)
        pairs_by_map.append(
            {
                **rec,
                "win_rate": wr,
                "baseline_p0": p0_map,
                "delta_pp": wr - (p0_map * 100.0),
                "prob_above_baseline": prob,
                "p_value": pval,
            }
        )

    atk_profile = {
        "attack_rounds": atk_rounds,
        "defense_rounds": def_rounds,
        "attack_win_rate": (atk_wins / atk_rounds) * 100.0 if atk_rounds > 0 else 0.0,
        "defense_win_rate": (def_wins / def_rounds) * 100.0 if def_rounds > 0 else 0.0,
        "first_blood_rate": (atk_fb / atk_rounds) * 100.0 if atk_rounds > 0 else 0.0,
        "first_death_rate": (atk_fd / atk_rounds) * 100.0 if atk_rounds > 0 else 0.0,
        "entry_rate": ((atk_fb + atk_fd) / atk_rounds) * 100.0 if atk_rounds > 0 else 0.0,
    }

    all_mates = sorted({tm for v in mates_by_match.values() for tm in v})
    pair_entry_effect: list[dict[str, Any]] = []
    for tm in all_mates:
        paired_mids = {mid for mid, mset in mates_by_match.items() if tm in mset}
        paired = {"rounds": 0, "wins": 0, "fb": 0, "fd": 0}
        unpaired = {"rounds": 0, "wins": 0, "fb": 0, "fd": 0}
        for mid in ids:
            at = atk_stats_by_match.get(mid, {"rounds": 0, "wins": 0, "fb": 0, "fd": 0})
            tgt = paired if mid in paired_mids else unpaired
            tgt["rounds"] += int(at.get("rounds", 0))
            tgt["wins"] += int(at.get("wins", 0))
            tgt["fb"] += int(at.get("fb", 0))
            tgt["fd"] += int(at.get("fd", 0))
        pr = max(1, paired["rounds"])
        ur = max(1, unpaired["rounds"])
        paired_fd = (paired["fd"] / pr) * 100.0
        unpaired_fd = (unpaired["fd"] / ur) * 100.0
        paired_fb = (paired["fb"] / pr) * 100.0
        unpaired_fb = (unpaired["fb"] / ur) * 100.0
        paired_entry = ((paired["fb"] + paired["fd"]) / pr) * 100.0
        unpaired_entry = ((unpaired["fb"] + unpaired["fd"]) / ur) * 100.0
        paired_wr = (paired["wins"] / pr) * 100.0
        unpaired_wr = (unpaired["wins"] / ur) * 100.0
        pair_entry_effect.append(
            {
                "teammate": tm,
                "paired_rounds": paired["rounds"],
                "unpaired_rounds": unpaired["rounds"],
                "paired_first_death_rate": paired_fd,
                "unpaired_first_death_rate": unpaired_fd,
                "delta_first_death_rate_pp": paired_fd - unpaired_fd,
                "paired_first_blood_rate": paired_fb,
                "unpaired_first_blood_rate": unpaired_fb,
                "delta_first_blood_rate_pp": paired_fb - unpaired_fb,
                "paired_entry_rate": paired_entry,
                "unpaired_entry_rate": unpaired_entry,
                "delta_entry_rate_pp": paired_entry - unpaired_entry,
                "paired_attack_win_rate": paired_wr,
                "unpaired_attack_win_rate": unpaired_wr,
                "delta_attack_win_rate_pp": paired_wr - unpaired_wr,
            }
        )
    pair_entry_effect.sort(key=lambda x: (-int(x["paired_rounds"]), x["teammate"]))

    return {
        "baseline": {"p0": p0, "wins": total_wins, "rounds": total_rounds, "p0_attack": p0_atk, "p0_defense": p0_def},
        "pairs_overall": base_pairs,
        "pairs_by_side": pairs_by_side,
        "pairs_by_map": pairs_by_map,
        "player_side_profile": atk_profile,
        "pair_entry_effect": pair_entry_effect,
        "scope": {"match_ids": len(ids)},
    }
