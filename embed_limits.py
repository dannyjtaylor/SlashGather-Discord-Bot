"""Discord embed size helpers.

A single message may include up to 10 embeds, but the combined characters of
every title, description, field name/value, footer, and author across those
embeds cannot exceed 6000. /user previously sent 6 embeds in one followup and
Discord rejected it with HTTP 400.
"""

from typing import Iterator, Sequence

DISCORD_EMBED_CHAR_LIMIT = 6000
DISCORD_MESSAGE_EMBED_CHAR_SAFE = 5500
DISCORD_MAX_EMBEDS_PER_MESSAGE = 10


def discord_embed_char_count(embed) -> int:
    """Return the character count Discord uses toward the 6000-char embed cap."""
    data = embed.to_dict()
    total = len(data.get("title") or "") + len(data.get("description") or "")
    total += len((data.get("footer") or {}).get("text") or "")
    total += len((data.get("author") or {}).get("name") or "")
    for field in data.get("fields") or []:
        total += len(field.get("name") or "") + len(field.get("value") or "")
    return total


def pack_embeds_within_limits(
    embeds: Sequence,
    max_chars: int = DISCORD_MESSAGE_EMBED_CHAR_SAFE,
    max_count: int = DISCORD_MAX_EMBEDS_PER_MESSAGE,
) -> Iterator[list]:
    """Yield embed batches that each fit in one Discord message.

    A single embed larger than *max_chars* (but still under Discord's 6000-char
    per-embed cap) is sent alone rather than dropped.
    """
    batch: list = []
    batch_len = 0
    for embed in embeds:
        size = discord_embed_char_count(embed)
        if batch and (len(batch) >= max_count or batch_len + size > max_chars):
            yield batch
            batch = []
            batch_len = 0
        batch.append(embed)
        batch_len += size
    if batch:
        yield batch
