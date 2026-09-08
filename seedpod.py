"""SEED POD gacha: rare imbues, shop items, Tree Rings, and Auto-Bloom."""

from __future__ import annotations

import random

SEEDPOD_ALL_RARITIES = (
    "COMMON",
    "UNCOMMON",
    "RARE",
    "SUPER RARE",
    "LEGENDARY",
    "NETHERITE",
    "LUMINITE",
    "CELESTIAL",
    "SECRET",
)
SEEDPOD_LOOT_RARITIES = SEEDPOD_ALL_RARITIES[1:]  # Uncommon and above
SEEDPOD_RARITIES = SEEDPOD_ALL_RARITIES[2:]  # imbues from RARE up
SEEDPOD_RARITY_RANK = {name: index for index, name in enumerate(SEEDPOD_ALL_RARITIES)}
SEEDPOD_IMBUE_CHANCE = 0.75
SEEDPOD_FALLBACK_ITEM_ID = "nether_star"
SEEDPOD_MAX_BLOOM_COUNT = 18
SEEDPOD_TREE_RING_PRIZES = (
    {"amount": 50, "rarity": "UNCOMMON"},
    {"amount": 150, "rarity": "LEGENDARY"},
    {"amount": 300, "rarity": "CELESTIAL"},
)
SEEDPOD_AUTO_BLOOM_RARITY = "SECRET"


def seedpod_rarity_rank(rarity: str | None) -> int:
    return SEEDPOD_RARITY_RANK.get((rarity or "").strip(), -1)


def seedpod_rarity_for_shop_cost(cost: int | float) -> str:
    value = float(cost or 0)
    if value < 50:
        return "COMMON"
    if value < 100:
        return "UNCOMMON"
    if value < 150:
        return "RARE"
    if value < 250:
        return "SUPER RARE"
    if value < 350:
        return "LEGENDARY"
    if value < 500:
        return "NETHERITE"
    if value < 600:
        return "LUMINITE"
    return "CELESTIAL"


def seedpod_prize_rarity(prize: dict) -> str:
    if prize.get("kind") == "imbue":
        return str((prize.get("enchant") or {}).get("rarity") or "RARE")
    rarity = str(prize.get("rarity") or "").strip()
    return rarity if rarity in SEEDPOD_RARITY_RANK else "COMMON"


def seedpod_promote_rarity(rolled: str, valid_rarities: set[str]) -> str | None:
    """If the rolled rank has nothing left, walk up COMMON → SECRET until one does."""
    start = seedpod_rarity_rank(rolled)
    if start < 0:
        start = 0
    for rarity in SEEDPOD_ALL_RARITIES[start:]:
        if rarity in valid_rarities:
            return rarity
    return None


def guaranteed_imbue_rarity(equipped: dict | None) -> str | None:
    """Rarity to grant for a Battle Pass Netherite+ imbue, or None if they already have SECRET."""
    current = (equipped or {}).get("rarity")
    if current == "SECRET":
        return None
    rank = seedpod_rarity_rank(current)
    netherite_rank = SEEDPOD_RARITY_RANK["NETHERITE"]
    if rank < netherite_rank:
        return "NETHERITE"
    if rank >= SEEDPOD_RARITY_RANK["SECRET"]:
        return None
    return SEEDPOD_ALL_RARITIES[rank + 1]


def seedpod_fallback_prize() -> dict:
    """Always-valid consolation so a SEED POD never refuses to open."""
    rarity = seedpod_rarity_for_shop_cost(250)
    return {
        "kind": "shop",
        "item_id": SEEDPOD_FALLBACK_ITEM_ID,
        "rarity": rarity,
        "weight": 1.0,
    }


def seedpod_grants_immediately(prize: dict) -> bool:
    """Shop and Tree Rings land instantly; imbues and Auto-Bloom wait for a button."""
    return prize.get("kind") not in {"imbue", "auto_bloom"}


def seedpod_opened_title(prize: dict, rarity_emoji: dict[str, str] | None = None) -> str:
    """Final embed title always shows rank + matching IMBUE emoji."""
    rarity = seedpod_prize_rarity(prize)
    emoji = (rarity_emoji or {}).get(rarity, "")
    extra = f" {emoji}" if emoji else ""
    return f"🫛 SEED POD OPENED: {rarity}!{extra}"


def seedpod_normalized_loot_weights(rarity_weights: dict[str, float] | None = None) -> dict[str, float]:
    """Drop COMMON and stretch the remaining ranks so they still sum to 100."""
    source = rarity_weights or {}
    loot = {
        rarity: max(0.0, float(source.get(rarity, 0.0) or 0.0))
        for rarity in SEEDPOD_LOOT_RARITIES
    }
    total = sum(loot.values())
    if total <= 0:
        even = 100.0 / len(SEEDPOD_LOOT_RARITIES)
        return {rarity: even for rarity in SEEDPOD_LOOT_RARITIES}
    return {rarity: weight * 100.0 / total for rarity, weight in loot.items()}


def _weight_for(rarity: str, rarity_weights: dict[str, float]) -> float:
    return float(rarity_weights.get(rarity, 1.0))


def seedpod_build_pool(
    *,
    hoe_by_rarity: dict,
    tractor_by_rarity: dict,
    shop_items: dict,
    rarity_weights: dict[str, float],
) -> list[dict]:
    weights = seedpod_normalized_loot_weights(rarity_weights)
    pool: list[dict] = []
    for tool, catalog in (("hoe", hoe_by_rarity), ("tractor", tractor_by_rarity)):
        for rarity in SEEDPOD_RARITIES:
            weight = _weight_for(rarity, weights)
            for enchant in catalog.get(rarity) or []:
                pool.append({
                    "kind": "imbue",
                    "tool": tool,
                    "enchant": dict(enchant),
                    "rarity": rarity,
                    "weight": weight,
                })
    for item_id, info in (shop_items or {}).items():
        rarity = seedpod_rarity_for_shop_cost((info or {}).get("cost", 0))
        if rarity not in SEEDPOD_LOOT_RARITIES:
            continue
        pool.append({
            "kind": "shop",
            "item_id": str(item_id),
            "rarity": rarity,
            "weight": _weight_for(rarity, weights),
        })
    for row in SEEDPOD_TREE_RING_PRIZES:
        rarity = row["rarity"]
        pool.append({
            "kind": "tree_rings",
            "amount": int(row["amount"]),
            "rarity": rarity,
            "weight": _weight_for(rarity, weights),
        })
    pool.append({
        "kind": "auto_bloom",
        "rarity": SEEDPOD_AUTO_BLOOM_RARITY,
        "weight": _weight_for(SEEDPOD_AUTO_BLOOM_RARITY, weights),
    })
    return pool


def seedpod_prize_valid(
    prize: dict,
    *,
    hoe: dict | None,
    tractor: dict | None,
    inventory: dict,
    stackable_items,
    bloom_count: int = 0,
) -> bool:
    kind = prize.get("kind")
    if kind == "shop":
        item_id = str(prize.get("item_id") or "")
        if not item_id:
            return False
        if item_id in stackable_items:
            return True
        return int(inventory.get(item_id, 0) or 0) <= 0
    if kind == "tree_rings":
        return int(prize.get("amount") or 0) > 0
    if kind == "auto_bloom":
        return int(bloom_count or 0) < SEEDPOD_MAX_BLOOM_COUNT
    if kind != "imbue":
        return False
    enchant = prize.get("enchant") or {}
    equipped = hoe if prize.get("tool") == "hoe" else tractor
    if not equipped:
        return True
    if enchant.get("name") == equipped.get("name"):
        return False
    return seedpod_rarity_rank(enchant.get("rarity")) >= seedpod_rarity_rank(equipped.get("rarity"))


def _pick_by_rarity(candidates: list[dict], valid: list[dict], rng: random.Random) -> dict | None:
    """Roll a rarity (weighted like imbues), then bump up if that rank is empty."""
    if not valid:
        return None
    present: list[str] = []
    weights: list[float] = []
    for rarity in SEEDPOD_ALL_RARITIES:
        sample = next((prize for prize in candidates if seedpod_prize_rarity(prize) == rarity), None)
        if sample is None:
            continue
        present.append(rarity)
        weights.append(max(0.0, float(sample.get("weight", 1.0))))
    if not present:
        return rng.choice(valid)
    rolled = rng.choices(present, weights=weights, k=1)[0]
    valid_rarities = {seedpod_prize_rarity(prize) for prize in valid}
    rarity = seedpod_promote_rarity(rolled, valid_rarities)
    if rarity is None:
        return rng.choice(valid)
    options = [prize for prize in valid if seedpod_prize_rarity(prize) == rarity]
    return rng.choice(options)


def seedpod_pick_prize(
    pool: list[dict],
    *,
    hoe: dict | None,
    tractor: dict | None,
    inventory: dict,
    stackable_items,
    bloom_count: int = 0,
    rng: random.Random | None = None,
) -> dict | None:
    rng = rng or random.Random()
    valid = [
        prize
        for prize in pool
        if seedpod_prize_valid(
            prize,
            hoe=hoe,
            tractor=tractor,
            inventory=inventory,
            stackable_items=stackable_items,
            bloom_count=bloom_count,
        )
    ]
    if not valid:
        return None
    imbues = [prize for prize in valid if prize.get("kind") == "imbue"]
    others = [prize for prize in valid if prize.get("kind") != "imbue"]
    imbue_candidates = [prize for prize in pool if prize.get("kind") == "imbue"]
    other_candidates = [prize for prize in pool if prize.get("kind") != "imbue"]
    if imbues and others:
        if rng.random() < SEEDPOD_IMBUE_CHANCE:
            return _pick_by_rarity(imbue_candidates, imbues, rng)
        return _pick_by_rarity(other_candidates, others, rng)
    if imbues:
        return _pick_by_rarity(imbue_candidates, imbues, rng)
    return _pick_by_rarity(other_candidates, others, rng)
