"""Legacy dashboard preview for recovery batches."""


def preview(rows, profiles, parallel):
    ordered = sorted(rows, key=lambda r: r['shard_id'])
    for index, row in enumerate(ordered):
        profile = profiles[row['profile_id']]
        duration = int(profile['startup_seconds'])
        wave = index // parallel + 1
        row.update(wave=wave, warmup_start_seconds=(wave-1)*duration,
                   cutover_seconds=wave*duration)
    return max((r['cutover_seconds'] for r in ordered), default=0)
