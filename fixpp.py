# -*- coding: utf-8 -*-

# a simple program to calculate every users total pp
# asynchronously on ripple-based servers, made for akatsuki.
# NOTE: this is only broken because of a few cases (such as
# a maps status being changed; i just don't really care to
# continue working on the ripple base when gulag has already
# come so far :P. it will be handled correctly there).

__all__ = ()

import asyncio
from cmyui import AsyncSQLPool, printc, Ansi
import config
from enum import unique, IntEnum

db = AsyncSQLPool()
scores_tables = ('scores', 'scores_relax')
stats_tables = ('users_stats', 'rx_stats')

@unique
class GameMode(IntEnum):
    vn_std = 0
    vn_taiko = 1
    vn_ctb = 2
    vn_mania = 3
    rx_std = 4
    rx_taiko = 5
    rx_ctb = 6

    @property
    def dbvalue(self) -> int:
        return self.value - (4 if self.value >= 4 else 0)

    def __str__(self) -> str:
        return (
            'std',
            'taiko',
            'ctb',
            'mania',

            'std',
            'taiko',
            'ctb'
        )[self.value]

    def __repr__(self) -> str:
        return (
            'vn!std',
            'vn!taiko',
            'vn!catch',
            'vn!mania',

            'rx!std',
            'rx!taiko',
            'rx!catch'
        )[self.value]

async def update_user_pp(user) -> None:
    # check if they have scores, if they do,
    # recalc their total pp (for each mode)
    for gm in GameMode:
        # get the appropriate
        # tables for this mode.
        rx = gm > GameMode.vn_mania
        scores_table = scores_tables[rx]
        stats_table = stats_tables[rx]

        # fetch the user's top 125 plays
        pp_vals = [x[0] for x in await db.fetchall(
            f'SELECT pp FROM {scores_table} s '
            'LEFT JOIN beatmaps b USING(beatmap_md5) '
            'WHERE s.completed = 3 AND s.play_mode = %s '
            'AND b.ranked = 2 AND s.userid = %s '
            'ORDER BY s.pp DESC LIMIT 125',
            [gm.dbvalue, user['id']], _dict=False
        )]

        if not pp_vals:
            # user has no scores in this mode.
            continue

        printc(f"Updating {user['name']}'s {gm!r} pp.", Ansi.LIGHT_BLUE)

        # calculate, and update their pp in db.
        total_pp = round(sum(
            v * 0.95 ** i
            for i, v in enumerate(pp_vals)
        ))

        await db.execute(
            f'UPDATE {stats_table} '
            f'SET pp_{gm!s} = %s '
            'WHERE id = %s',
            [total_pp, user['id']]
        )

async def run() -> None:
    await db.connect(**config.mysql)

    users = await db.fetchall('SELECT username name, id FROM users')
    if not users: return printc('No users found!', Ansi.LIGHT_RED)

    await asyncio.gather(*(update_user_pp(u) for u in users))

    db.pool.close()
    await db.pool.wait_closed()

asyncio.run(run())
