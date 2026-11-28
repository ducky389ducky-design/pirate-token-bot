import hashlib
import json
import os
import re
import secrets
import sqlite3
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Literal, Optional

import discord
from discord import app_commands
from dotenv import load_dotenv

# Ensure all print() logs appear immediately in Bothost Docker console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)

load_dotenv()

# =====================================================================
# DEFAULT CONFIGURATION
# =====================================================================

DEFAULT_CONFIG = {
    "odds_1day": 500,                  # 1 in 500 chance per message
    "odds_lifetime": 3000,             # 1 in 3,000 chance per message
    "max_stock_1day": 30,              # 30 keys stock cap for 1-Day
    "max_stock_lifetime": 15,          # 15 keys stock cap for Lifetime
    "roll_cooldown_seconds": 0,        # 0s = disabled (uses Discord channel slowmode)
    "min_message_length": 1,           # Any real text message rolls
    "one_lifetime_per_user": True,     # Max 1 Lifetime Pirate Token per user
    "oneday_user_cooldown_hours": 24,  # 24h before same user can win another 1-Day key
    "show_remaining_stock_publicly": True,
    "log_channel_id": None,
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _resolve_db_path() -> str:
    candidates = []
    if os.getenv("DATA_DIR"):
        candidates.append(os.getenv("DATA_DIR", ""))
    if os.path.isdir("/app"):
        candidates.append("/app/data")
    candidates.append(BASE_DIR)

    for folder in candidates:
        if not folder:
            continue
        try:
            os.makedirs(folder, exist_ok=True)
            test_file = os.path.join(folder, ".write_test")
            with open(test_file, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test_file)
            return os.path.join(folder, "pirate_bot.db")
        except Exception as exc:
            print(f"[WARN] Data folder {folder} not writable ({exc}), trying fallback...")
    return os.path.join(BASE_DIR, "pirate_bot.db")


DB_PATH = _resolve_db_path()
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
SEED_1DAY_PATH = os.path.join(BASE_DIR, "keys_1day.txt")
SEED_LIFETIME_PATH = os.path.join(BASE_DIR, "keys_lifetime.txt")


# =====================================================================
# SQLITE PERSISTENCE LAYER (FAST PERSISTENT CONNECTION)
# =====================================================================


class PirateDatabase:
    """Fast SQLite persistence layer safe for Docker network/bind volumes."""

    def __init__(self, db_path: str = DB_PATH, config_path: str = CONFIG_PATH) -> None:
        self.db_path = db_path
        self.config_path = config_path
        self._conn = sqlite3.connect(
            self.db_path,
            timeout=10.0,
            isolation_level=None,  # Autocommit unless explicit transaction
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=TRUNCATE;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._init_db()
        self._seed_default_settings()

    def _init_db(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tier TEXT NOT NULL CHECK(tier IN ('1day', 'lifetime')),
                key_text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'available' CHECK(status IN ('available', 'claimed')),
                added_at REAL NOT NULL,
                claimed_by_id INTEGER,
                claimed_by_name TEXT,
                claimed_at REAL,
                channel_id INTEGER,
                message_url TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_keys_tier_status ON keys(tier, status);
            CREATE INDEX IF NOT EXISTS idx_keys_claimed_by ON keys(claimed_by_id);

            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                total_rolls INTEGER NOT NULL DEFAULT 0,
                last_roll_at REAL NOT NULL DEFAULT 0,
                oneday_wins INTEGER NOT NULL DEFAULT 0,
                lifetime_wins INTEGER NOT NULL DEFAULT 0,
                last_oneday_win_at REAL NOT NULL DEFAULT 0,
                last_lifetime_win_at REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS ignored_channels (
                channel_id INTEGER PRIMARY KEY
            );
            """
        )

    def _seed_default_settings(self) -> None:
        merged = dict(DEFAULT_CONFIG)
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    file_cfg = json.load(f)
                if isinstance(file_cfg, dict):
                    merged.update(file_cfg)
            except Exception as exc:
                print(f"[WARN] Could not parse {self.config_path}: {exc}")

        merged.setdefault("power_hour_multiplier", 1.0)
        merged.setdefault("power_hour_ends_at", 0.0)

        self._conn.execute("BEGIN")
        try:
            for k, v in merged.items():
                self._conn.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (k, json.dumps(v)),
                )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def get_setting(self, key: str, default: Any = None) -> Any:
        row = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except Exception:
            return row["value"]

    def set_setting(self, key: str, value: Any) -> None:
        self._conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value)),
        )

    def get_all_settings(self) -> dict[str, Any]:
        rows = self._conn.execute("SELECT key, value FROM settings").fetchall()
        result: dict[str, Any] = {}
        for row in rows:
            try:
                result[row["key"]] = json.loads(row["value"])
            except Exception:
                result[row["key"]] = row["value"]
        return result

    def is_channel_ignored(self, channel_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM ignored_channels WHERE channel_id = ?", (channel_id,)
        ).fetchone()
        return row is not None

    def toggle_ignore_channel(self, channel_id: int) -> bool:
        exists = self._conn.execute(
            "SELECT 1 FROM ignored_channels WHERE channel_id = ?", (channel_id,)
        ).fetchone()
        if exists:
            self._conn.execute("DELETE FROM ignored_channels WHERE channel_id = ?", (channel_id,))
            return False
        self._conn.execute("INSERT INTO ignored_channels (channel_id) VALUES (?)", (channel_id,))
        return True

    def get_ignored_channels(self) -> list[int]:
        rows = self._conn.execute("SELECT channel_id FROM ignored_channels").fetchall()
        return [int(r["channel_id"]) for r in rows]

    def get_stock_counts(self) -> dict[str, dict[str, int]]:
        rows = self._conn.execute(
            """
            SELECT tier, status, COUNT(*) as cnt
            FROM keys
            GROUP BY tier, status
            """
        ).fetchall()

        stats = {
            "1day": {"available": 0, "claimed": 0, "cap": int(self.get_setting("max_stock_1day", 30))},
            "lifetime": {
                "available": 0,
                "claimed": 0,
                "cap": int(self.get_setting("max_stock_lifetime", 15)),
            },
        }
        for r in rows:
            tier = r["tier"]
            status = r["status"]
            if tier in stats and status in ("available", "claimed"):
                stats[tier][status] = int(r["cnt"])
        return stats

    def add_keys(self, tier: str, key_texts: list[str], enforce_cap: bool = True) -> tuple[int, int]:
        cleaned = [k.strip() for k in key_texts if k and k.strip()]
        if not cleaned:
            return 0, 0

        cap_key = "max_stock_lifetime" if tier == "lifetime" else "max_stock_1day"
        default_cap = 15 if tier == "lifetime" else 30
        cap = int(self.get_setting(cap_key, default_cap))

        now = time.time()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            cur_avail = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM keys WHERE tier = ? AND status = 'available'",
                (tier,),
            ).fetchone()["cnt"]

            if enforce_cap:
                room = max(0, cap - int(cur_avail))
                to_insert = cleaned[:room]
                skipped = len(cleaned) - len(to_insert)
            else:
                to_insert = cleaned
                skipped = 0

            if to_insert:
                self._conn.executemany(
                    "INSERT INTO keys (tier, key_text, status, added_at) VALUES (?, ?, 'available', ?)",
                    [(tier, k, now) for k in to_insert],
                )
            self._conn.execute("COMMIT")
            return len(to_insert), skipped
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def clear_available_keys(self, tier: str) -> int:
        cur = self._conn.execute(
            "DELETE FROM keys WHERE tier = ? AND status = 'available'", (tier,)
        )
        return cur.rowcount

    def import_seed_file(self, tier: str, filepath: str) -> int:
        path = Path(filepath)
        if not path.exists():
            return 0

        lines = path.read_text(encoding="utf-8").splitlines()
        valid_keys = [
            line.strip()
            for line in lines
            if line.strip() and not line.strip().startswith("#")
        ]
        if not valid_keys:
            return 0

        added, skipped = self.add_keys(tier, valid_keys, enforce_cap=True)
        label = "LIFETIME (Pirate Token)" if tier == "lifetime" else "1-DAY"
        leftover = valid_keys[added:]

        header = [
            f"# {label} KEY DROP FILE",
            "# Paste keys/messages below (1 per line) and restart the bot to load them,",
            "# OR use /addkeys and /addsharedkey directly inside Discord!",
            f"# Last startup imported {added} key(s) into pirate_bot.db.",
            "",
        ]
        if leftover:
            header.append(f"# NOTE: {skipped} key(s) below were kept here because max stock cap was reached:")
            header.extend(leftover)

        path.write_text("\n".join(header) + "\n", encoding="utf-8")
        return added

    def import_env_keys(self) -> tuple[int, int]:
        added_1d = 0
        added_lt = 0

        shared_1d = os.getenv("SHARED_KEY_1DAY", "").strip()
        if shared_1d:
            h = hashlib.sha256(shared_1d.encode("utf-8")).hexdigest()
            if self.get_setting("imported_hash_shared_1day") != h:
                cap = int(self.get_setting("max_stock_1day", 30))
                cnt, _ = self.add_keys("1day", [shared_1d] * cap, enforce_cap=True)
                self.set_setting("imported_hash_shared_1day", h)
                added_1d += cnt

        shared_lt = os.getenv("SHARED_KEY_LIFETIME", "").strip()
        if shared_lt:
            h = hashlib.sha256(shared_lt.encode("utf-8")).hexdigest()
            if self.get_setting("imported_hash_shared_lt") != h:
                cap = int(self.get_setting("max_stock_lifetime", 15))
                cnt, _ = self.add_keys("lifetime", [shared_lt] * cap, enforce_cap=True)
                self.set_setting("imported_hash_shared_lt", h)
                added_lt += cnt

        raw_1d = os.getenv("KEYS_1DAY", "").strip()
        if raw_1d:
            h = hashlib.sha256(raw_1d.encode("utf-8")).hexdigest()
            if self.get_setting("imported_hash_keys_1day") != h:
                items = [x.strip() for x in raw_1d.replace(",", "\n").splitlines() if x.strip()]
                cnt, _ = self.add_keys("1day", items, enforce_cap=True)
                self.set_setting("imported_hash_keys_1day", h)
                added_1d += cnt

        raw_lt = os.getenv("KEYS_LIFETIME", "").strip()
        if raw_lt:
            h = hashlib.sha256(raw_lt.encode("utf-8")).hexdigest()
            if self.get_setting("imported_hash_keys_lt") != h:
                items = [x.strip() for x in raw_lt.replace(",", "\n").splitlines() if x.strip()]
                cnt, _ = self.add_keys("lifetime", items, enforce_cap=True)
                self.set_setting("imported_hash_keys_lt", h)
                added_lt += cnt

        return added_1d, added_lt

    def record_roll_if_ready(self, user_id: int, username: str) -> tuple[bool, dict[str, Any]]:
        cooldown = float(self.get_setting("roll_cooldown_seconds", 0))
        now = time.time()

        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO users (user_id, username, total_rolls, last_roll_at)
                    VALUES (?, ?, 1, ?)
                    """,
                    (user_id, username, now),
                )
                new_row = self._conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
                self._conn.execute("COMMIT")
                return True, dict(new_row)

            if cooldown > 0 and (now - float(row["last_roll_at"])) < cooldown:
                self._conn.execute("ROLLBACK")
                return False, dict(row)

            self._conn.execute(
                """
                UPDATE users
                SET username = ?, total_rolls = total_rolls + 1, last_roll_at = ?
                WHERE user_id = ?
                """,
                (username, now, user_id),
            )
            updated = self._conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            self._conn.execute("COMMIT")
            return True, dict(updated)
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def can_user_win(self, user_row: dict[str, Any], tier: str) -> bool:
        now = time.time()
        if tier == "lifetime":
            if bool(self.get_setting("one_lifetime_per_user", True)):
                if int(user_row.get("lifetime_wins", 0)) >= 1:
                    return False
            return True

        if tier == "1day":
            cd_hours = float(self.get_setting("oneday_user_cooldown_hours", 24))
            if cd_hours > 0:
                last_win = float(user_row.get("last_oneday_win_at", 0))
                if (now - last_win) < (cd_hours * 3600.0):
                    return False
            return True

        return False

    def has_available_key_for_user(self, tier: str, user_id: int) -> bool:
        row = self._conn.execute(
            """
            SELECT 1 FROM keys
            WHERE tier = ?
              AND status = 'available'
              AND key_text NOT IN (
                  SELECT key_text FROM keys
                  WHERE status = 'claimed' AND claimed_by_id = ?
              )
            LIMIT 1
            """,
            (tier, user_id),
        ).fetchone()
        return row is not None

    def claim_key_atomic(
        self,
        tier: str,
        user_id: int,
        username: str,
        channel_id: int,
    ) -> Optional[dict[str, Any]]:
        now = time.time()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            row = self._conn.execute(
                """
                SELECT id, tier, key_text
                FROM keys
                WHERE tier = ?
                  AND status = 'available'
                  AND key_text NOT IN (
                      SELECT key_text FROM keys
                      WHERE status = 'claimed' AND claimed_by_id = ?
                  )
                ORDER BY id ASC
                LIMIT 1
                """,
                (tier, user_id),
            ).fetchone()

            if row is None:
                self._conn.execute("ROLLBACK")
                return None

            key_id = int(row["id"])
            self._conn.execute(
                """
                UPDATE keys
                SET status = 'claimed',
                    claimed_by_id = ?,
                    claimed_by_name = ?,
                    claimed_at = ?,
                    channel_id = ?
                WHERE id = ?
                """,
                (user_id, username, now, channel_id, key_id),
            )

            if tier == "lifetime":
                self._conn.execute(
                    """
                    UPDATE users
                    SET lifetime_wins = lifetime_wins + 1,
                        last_lifetime_win_at = ?
                    WHERE user_id = ?
                    """,
                    (now, user_id),
                )
            else:
                self._conn.execute(
                    """
                    UPDATE users
                    SET oneday_wins = oneday_wins + 1,
                        last_oneday_win_at = ?
                    WHERE user_id = ?
                    """,
                    (now, user_id),
                )

            remaining = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM keys WHERE tier = ? AND status = 'available'",
                (tier,),
            ).fetchone()["cnt"]

            self._conn.execute("COMMIT")
            return {
                "id": key_id,
                "tier": row["tier"],
                "key_text": row["key_text"],
                "claimed_by_id": user_id,
                "claimed_by_name": username,
                "claimed_at": now,
                "remaining": int(remaining),
            }
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def attach_message_url(self, key_id: int, message_url: str) -> None:
        self._conn.execute("UPDATE keys SET message_url = ? WHERE id = ?", (message_url, key_id))

    def get_key_by_id(self, key_id: int) -> Optional[dict[str, Any]]:
        row = self._conn.execute("SELECT * FROM keys WHERE id = ?", (key_id,)).fetchone()
        return dict(row) if row else None

    def get_user_stats(self, user_id: int) -> dict[str, Any]:
        row = self._conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row is None:
            return {
                "user_id": user_id,
                "total_rolls": 0,
                "oneday_wins": 0,
                "lifetime_wins": 0,
            }
        return dict(row)

    def get_user_claimed_keys(self, user_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT id, tier, key_text, claimed_at, message_url
            FROM keys
            WHERE status = 'claimed' AND claimed_by_id = ?
            ORDER BY claimed_at DESC
            """,
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_winners(self, limit: int = 10) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT id, tier, claimed_by_id, claimed_by_name, claimed_at, message_url
            FROM keys
            WHERE status = 'claimed'
            ORDER BY claimed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_multiplier(self) -> tuple[float, float]:
        mult = float(self.get_setting("power_hour_multiplier", 1.0))
        ends_at = float(self.get_setting("power_hour_ends_at", 0.0))
        if mult <= 1.0 or ends_at <= time.time():
            return 1.0, 0.0
        return mult, ends_at

    def set_power_hour(self, multiplier: float, duration_minutes: int) -> float:
        if multiplier <= 1.0 or duration_minutes <= 0:
            self.set_setting("power_hour_multiplier", 1.0)
            self.set_setting("power_hour_ends_at", 0.0)
            return 0.0
        ends_at = time.time() + (duration_minutes * 60.0)
        self.set_setting("power_hour_multiplier", multiplier)
        self.set_setting("power_hour_ends_at", ends_at)
        return ends_at


db = PirateDatabase()


# =====================================================================
# DISCORD BOT & UI COMPONENTS
# =====================================================================


def format_tier_name(tier: str) -> str:
    if tier == "lifetime":
        return "🏴‍☠️ LIFETIME PIRATE TOKEN"
    return "🗝️ 1-Day Script Key"


def build_private_key_embed(tier: str, key_text: str) -> discord.Embed:
    is_lifetime = tier == "lifetime"
    title = (
        "🏴‍☠️ PIRATE TOKEN UNLOCKED — LIFETIME KEY!"
        if is_lifetime
        else "🗝️ TREASURE CHEST UNLOCKED — 1-DAY KEY!"
    )
    color = discord.Color.gold() if is_lifetime else discord.Color.teal()
    desc = (
        "Congratulations! You hit the ultra-rare **Lifetime Pirate Token** drop just by chatting!\n\n"
        if is_lifetime
        else "Nice! You pulled a **1-Day Script Key** drop from chatting!\n\n"
    )
    desc += f"**Your Key / Reward:**\n```\n{key_text}\n```\n*Keep this private — do not share it in public chat!*"

    embed = discord.Embed(title=title, description=desc, color=color)
    embed.set_footer(text="Pirate Token Drop System • Use /mykeys anytime to view your won keys")
    return embed


class ClaimKeyButton(
    discord.ui.DynamicItem[discord.ui.Button],
    template=r"pirate_claim:(?P<key_id>[0-9]+)",
):
    """Persistent button allowing ONLY the drop winner to view their key privately."""

    def __init__(self, key_id: int) -> None:
        super().__init__(
            discord.ui.Button(
                label="🔒 Claim / View Your Key",
                style=discord.ButtonStyle.success,
                custom_id=f"pirate_claim:{key_id}",
            )
        )
        self.key_id = key_id

    @classmethod
    async def from_custom_id(
        cls,
        interaction: discord.Interaction,
        item: discord.ui.Button,
        match: re.Match[str],
        /,
    ) -> "ClaimKeyButton":
        return cls(int(match["key_id"]))

    async def callback(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        record = db.get_key_by_id(self.key_id)
        if not record or record.get("status") != "claimed":
            await interaction.followup.send("⚠️ Could not find this key record.", ephemeral=True)
            return

        winner_id = int(record["claimed_by_id"])
        if interaction.user.id != winner_id:
            await interaction.followup.send(
                f"🏴‍☠️ Hands off, matey! This treasure belongs to <@{winner_id}> — keep chatting to find your own!",
                ephemeral=True,
            )
            return

        embed = build_private_key_embed(record["tier"], record["key_text"])
        await interaction.followup.send(embed=embed, ephemeral=True)


class AddKeysModal(discord.ui.Modal):
    """Admin pop-up modal to paste multiple unique keys (one per line) privately."""

    keys_input = discord.ui.TextInput(
        label="Paste Keys / Messages (1 per line)",
        style=discord.TextStyle.paragraph,
        placeholder="KEY-XXXX-YYYY-1\nKEY-XXXX-YYYY-2\nKEY-XXXX-YYYY-3",
        required=True,
        max_length=4000,
    )

    def __init__(self, tier: Literal["1day", "lifetime"], ignore_cap: bool = False) -> None:
        title_label = "Add Lifetime Keys" if tier == "lifetime" else "Add 1-Day Keys"
        super().__init__(title=title_label)
        self.tier = tier
        self.ignore_cap = ignore_cap

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        lines = [line.strip() for line in self.keys_input.value.splitlines() if line.strip()]
        added, skipped = db.add_keys(self.tier, lines, enforce_cap=not self.ignore_cap)
        stock = db.get_stock_counts()[self.tier]

        msg = (
            f"✅ Added **{added}** key(s) to **{format_tier_name(self.tier)}**!\n"
            f"📦 Available Stock: **{stock['available']} / {stock['cap']}**"
        )
        if skipped > 0:
            msg += (
                f"\n⚠️ Skipped **{skipped}** key(s) because the stock cap (`{stock['cap']}`) was reached. "
                f"Use `/setstockcap` if you want to raise the limit."
            )
        await interaction.followup.send(msg, ephemeral=True)


class PirateBot(discord.Client):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self._synced_guilds: set[int] = set()

    async def setup_hook(self) -> None:
        imported_1d = db.import_seed_file("1day", SEED_1DAY_PATH)
        imported_lt = db.import_seed_file("lifetime", SEED_LIFETIME_PATH)
        env_1d, env_lt = db.import_env_keys()
        tot_1d = imported_1d + env_1d
        tot_lt = imported_lt + env_lt
        if tot_1d or tot_lt:
            print(f"[SEED] Imported {tot_1d} 1-Day key(s) and {tot_lt} Lifetime key(s) into {DB_PATH}.")

        self.add_dynamic_items(ClaimKeyButton)
        await self.tree.sync()
        print("[SYNC] Synced global slash commands.")

    async def on_ready(self) -> None:
        stock = db.get_stock_counts()
        odds_1d = db.get_setting("odds_1day", 500)
        odds_lt = db.get_setting("odds_lifetime", 3000)
        cooldown = db.get_setting("roll_cooldown_seconds", 0)
        print("=" * 58)
        print(f"🏴‍☠️ Pirate Token Bot is ONLINE as {self.user} (ID: {self.user.id})")
        print(f"   • Guilds Connected  : {[g.name for g in self.guilds]}")
        print(f"   • 1-Day Key Odds    : 1 in {odds_1d} | Stock: {stock['1day']['available']}/{stock['1day']['cap']}")
        print(f"   • Lifetime Key Odds : 1 in {odds_lt} | Stock: {stock['lifetime']['available']}/{stock['lifetime']['cap']}")
        print(f"   • Roll Cooldown     : {cooldown}s")
        print(f"   • SQLite DB Path    : {DB_PATH}")
        print("=" * 58)
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="chat for Pirate Token drops 🏴‍☠️",
            )
        )

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or message.guild is None or message.webhook_id is not None:
            return
        if message.type not in (discord.MessageType.default, discord.MessageType.reply):
            return
        if db.is_channel_ignored(message.channel.id):
            return

        min_len = int(db.get_setting("min_message_length", 1))
        if len(message.content.strip()) < min_len:
            return

        ready, user_row = db.record_roll_if_ready(message.author.id, str(message.author))
        if not ready:
            return

        multiplier, _ = db.get_active_multiplier()

        # 1) Roll for LIFETIME Pirate Token first (Jackpot)
        if db.can_user_win(user_row, "lifetime") and db.has_available_key_for_user(
            "lifetime", message.author.id
        ):
            base_lt_odds = max(1, int(db.get_setting("odds_lifetime", 3000)))
            effective_lt_odds = max(1, round(base_lt_odds / multiplier))
            if secrets.randbelow(effective_lt_odds) == 0:
                claimed = db.claim_key_atomic(
                    tier="lifetime",
                    user_id=message.author.id,
                    username=str(message.author),
                    channel_id=message.channel.id,
                )
                if claimed:
                    await self._deliver_drop(message, claimed, effective_lt_odds, user_row["total_rolls"])
                    return

        # 2) Roll for 1-DAY Script Key second
        if db.can_user_win(user_row, "1day") and db.has_available_key_for_user(
            "1day", message.author.id
        ):
            base_1d_odds = max(1, int(db.get_setting("odds_1day", 500)))
            effective_1d_odds = max(1, round(base_1d_odds / multiplier))
            if secrets.randbelow(effective_1d_odds) == 0:
                claimed = db.claim_key_atomic(
                    tier="1day",
                    user_id=message.author.id,
                    username=str(message.author),
                    channel_id=message.channel.id,
                )
                if claimed:
                    await self._deliver_drop(message, claimed, effective_1d_odds, user_row["total_rolls"])
                    return

    async def _deliver_drop(
        self,
        trigger_message: discord.Message,
        claimed: dict,
        odds_hit: int,
        user_rolls: int,
    ) -> None:
        winner = trigger_message.author
        tier = claimed["tier"]
        key_id = int(claimed["id"])
        key_text = claimed["key_text"]
        remaining = int(claimed["remaining"])
        is_lifetime = tier == "lifetime"

        dm_sent = False
        try:
            dm_embed = build_private_key_embed(tier, key_text)
            await winner.send(embed=dm_embed)
            dm_sent = True
        except (discord.Forbidden, discord.HTTPException):
            dm_sent = False

        if is_lifetime:
            pub_title = "🏴‍☠️ MYTHIC DROP! PIRATE TOKEN FOUND! 🏴‍☠️"
            pub_color = discord.Color.gold()
            pub_desc = (
                f"🎉 {winner.mention} just pulled a **LIFETIME SCRIPT KEY (Pirate Token)** "
                f"at **`1 in {odds_hit:,}`** odds on message **#{user_rolls:,}**!"
            )
        else:
            pub_title = "🗝️ TREASURE DROP! 1-DAY KEY FOUND!"
            pub_color = discord.Color.teal()
            pub_desc = (
                f"✨ {winner.mention} just unlocked a **1-Day Script Key** "
                f"at **`1 in {odds_hit:,}`** odds on message **#{user_rolls:,}**!"
            )

        if dm_sent:
            pub_desc += "\n\n📩 **Sent to your DMs!** *(You can also click the button below to view it privately.)*"
        else:
            pub_desc += "\n\n🔒 **Your DMs are closed!** Click **Claim / View Your Key** below to reveal your key privately!"

        if bool(db.get_setting("show_remaining_stock_publicly", True)):
            cap_key = "max_stock_lifetime" if is_lifetime else "max_stock_1day"
            cap = int(db.get_setting(cap_key, 15 if is_lifetime else 30))
            pub_desc += f"\n📦 Remaining {('Lifetime' if is_lifetime else '1-Day')} Stock: **`{remaining} / {cap}`**"

        pub_embed = discord.Embed(title=pub_title, description=pub_desc, color=pub_color)
        pub_embed.set_thumbnail(url=winner.display_avatar.url)
        pub_embed.set_footer(text="Keep chatting anywhere for a chance to unlock keys!")

        view = discord.ui.View(timeout=None)
        view.add_item(ClaimKeyButton(key_id))

        sent_msg: Optional[discord.Message] = None
        try:
            sent_msg = await trigger_message.reply(
                content=winner.mention,
                embed=pub_embed,
                view=view,
                mention_author=True,
            )
        except discord.HTTPException:
            try:
                sent_msg = await trigger_message.channel.send(
                    content=winner.mention,
                    embed=pub_embed,
                    view=view,
                )
            except discord.HTTPException as exc:
                print(f"[WARN] Could not send public drop embed: {exc}")

        if sent_msg is not None:
            db.attach_message_url(key_id, sent_msg.jump_url)

        log_channel_id = db.get_setting("log_channel_id", None)
        if log_channel_id:
            log_ch = trigger_message.guild.get_channel(int(log_channel_id))
            if isinstance(log_ch, discord.TextChannel):
                log_embed = discord.Embed(
                    title=f"📋 Key Drop Log — {format_tier_name(tier)}",
                    color=pub_color,
                    timestamp=discord.utils.utcnow(),
                )
                log_embed.add_field(name="Winner", value=f"{winner.mention} (`{winner.id}`)", inline=True)
                log_embed.add_field(name="Channel", value=f"<#{trigger_message.channel.id}>", inline=True)
                log_embed.add_field(name="Remaining Stock", value=f"`{remaining}`", inline=True)
                log_embed.add_field(name="Key Delivered", value=f"```\n{key_text}\n```", inline=False)
                if sent_msg is not None:
                    log_embed.add_field(name="Jump Link", value=sent_msg.jump_url, inline=False)
                try:
                    await log_ch.send(embed=log_embed)
                except discord.HTTPException:
                    pass


bot = PirateBot()


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction, error: app_commands.AppCommandError
) -> None:
    print(f"[ERROR] Slash command /{interaction.command.name if interaction.command else '?'} failed:")
    traceback.print_exception(type(error), error, error.__traceback__)
    err_msg = f"⚠️ Command error: `{error}`"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(err_msg, ephemeral=True)
        else:
            await interaction.response.send_message(err_msg, ephemeral=True)
    except discord.HTTPException:
        pass


# =====================================================================
# SLASH COMMANDS (WITH INSTANT DEFER SO DISCORD NEVER TIMES OUT)
# =====================================================================


@bot.tree.command(name="pirate", description="Check Pirate Token drop odds, stock, and your message roll stats")
async def pirate_stats_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer()
    user_stats = db.get_user_stats(interaction.user.id)
    stock = db.get_stock_counts()
    mult, ends_at = db.get_active_multiplier()

    base_1d = max(1, int(db.get_setting("odds_1day", 500)))
    base_lt = max(1, int(db.get_setting("odds_lifetime", 3000)))
    eff_1d = max(1, round(base_1d / mult))
    eff_lt = max(1, round(base_lt / mult))

    embed = discord.Embed(
        title="🏴‍☠️ Pirate Token Drop Tracker",
        description="Chat in any text channel for a random chance to pull a script key!",
        color=discord.Color.gold() if mult > 1.0 else discord.Color.blurple(),
    )

    if mult > 1.0:
        embed.add_field(
            name="⚡ POWER HOUR ACTIVE!",
            value=f"**`{mult:g}x` Boosted Drop Odds** — ends <t:{int(ends_at)}:R>!",
            inline=False,
        )

    show_stock = bool(db.get_setting("show_remaining_stock_publicly", True))
    stock_1d_txt = f"\nStock: **`{stock['1day']['available']} / {stock['1day']['cap']}`**" if show_stock else ""
    stock_lt_txt = f"\nStock: **`{stock['lifetime']['available']} / {stock['lifetime']['cap']}`**" if show_stock else ""

    embed.add_field(
        name="🗝️ 1-Day Script Key",
        value=f"Odds: **`1 in {eff_1d:,}`**{stock_1d_txt}",
        inline=True,
    )
    embed.add_field(
        name="🏴‍☠️ Lifetime Pirate Token",
        value=f"Odds: **`1 in {eff_lt:,}`**{stock_lt_txt}",
        inline=True,
    )
    embed.add_field(
        name=f"📊 {interaction.user.display_name}'s Stats",
        value=(
            f"• Messages Rolled: **`{user_stats['total_rolls']:,}`**\n"
            f"• 1-Day Keys Won: **`{user_stats['oneday_wins']}`**\n"
            f"• Lifetime Tokens Won: **`{user_stats['lifetime_wins']}`**"
        ),
        inline=False,
    )
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="mykeys", description="Privately view any script keys you have won")
async def mykeys_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    won = db.get_user_claimed_keys(interaction.user.id)
    if not won:
        await interaction.followup.send(
            "🏴‍☠️ You haven't pulled any keys yet — keep chatting in text channels to roll!",
            ephemeral=True,
        )
        return

    lines = []
    for item in won[:15]:
        ts = int(item["claimed_at"] or 0)
        tier_label = "🏴‍☠️ **Lifetime Key**" if item["tier"] == "lifetime" else "🗝️ **1-Day Key**"
        lines.append(f"{tier_label} (<t:{ts}:R>):\n```\n{item['key_text']}\n```")

    embed = discord.Embed(
        title="🔒 Your Claimed Keys",
        description="\n".join(lines),
        color=discord.Color.gold(),
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="addkeys", description="[Admin] Open a private form to paste unique 1-Day or Lifetime keys")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    tier="Which key pool to add to",
    ignore_cap="Set True to allow adding beyond the current max stock cap",
)
async def addkeys_cmd(
    interaction: discord.Interaction,
    tier: Literal["1day", "lifetime"],
    ignore_cap: bool = False,
) -> None:
    await interaction.response.send_modal(AddKeysModal(tier=tier, ignore_cap=ignore_cap))


@bot.tree.command(
    name="addsharedkey",
    description="[Admin] Add multiple drops of the SAME key/message (e.g., 30x 1-Day or 15x Lifetime)",
)
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    tier="Which key pool to load",
    key_message="The key or message to send each winner",
    copies="How many people can win this key (e.g. 30 for 1day, 15 for lifetime)",
)
async def addsharedkey_cmd(
    interaction: discord.Interaction,
    tier: Literal["1day", "lifetime"],
    key_message: str,
    copies: app_commands.Range[int, 1, 500],
) -> None:
    await interaction.response.defer(ephemeral=True)
    added, skipped = db.add_keys(tier, [key_message] * copies, enforce_cap=True)
    stock = db.get_stock_counts()[tier]
    msg = (
        f"✅ Loaded **{added}** drop(s) into **{format_tier_name(tier)}**!\n"
        f"📦 Available Stock: **`{stock['available']} / {stock['cap']}`**\n"
        f"*(Note: Each winner can only claim this shared key once, guaranteeing unique winners!)*"
    )
    if skipped > 0:
        msg += f"\n⚠️ `{skipped}` copy/copies skipped due to stock cap (`{stock['cap']}`). Use `/setstockcap` to raise it."
    await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name="stock", description="[Admin] View remaining key stock, current settings, and recent winners")
@app_commands.default_permissions(administrator=True)
async def stock_cmd(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    stock = db.get_stock_counts()
    cfg = db.get_all_settings()
    mult, ends_at = db.get_active_multiplier()
    recent = db.get_recent_winners(limit=8)

    odds_1d = int(cfg.get("odds_1day", 500))
    odds_lt = int(cfg.get("odds_lifetime", 3000))

    embed = discord.Embed(title="🛠️ Admin Pirate Token Dashboard", color=discord.Color.gold())
    embed.add_field(
        name="🗝️ 1-Day Pool",
        value=(
            f"• Available: **`{stock['1day']['available']} / {stock['1day']['cap']}`**\n"
            f"• Claimed: **`{stock['1day']['claimed']}`**\n"
            f"• Base Odds: **`1 in {odds_1d:,}`**"
        ),
        inline=True,
    )
    embed.add_field(
        name="🏴‍☠️ Lifetime Pool",
        value=(
            f"• Available: **`{stock['lifetime']['available']} / {stock['lifetime']['cap']}`**\n"
            f"• Claimed: **`{stock['lifetime']['claimed']}`**\n"
            f"• Base Odds: **`1 in {odds_lt:,}`**"
        ),
        inline=True,
    )

    ph_status = f"Active (`{mult:g}x` until <t:{int(ends_at)}:t>)" if mult > 1.0 else "Off (`1x`)"
    ignored = db.get_ignored_channels()
    ignored_str = ", ".join(f"<#{cid}>" for cid in ignored) if ignored else "None"
    log_ch = f"<#{cfg['log_channel_id']}>" if cfg.get("log_channel_id") else "Not set"

    embed.add_field(
        name="⚙️ Active Rules",
        value=(
            f"• Roll Cooldown: **`{cfg.get('roll_cooldown_seconds', 0)}s`**\n"
            f"• 1 Lifetime Max / User: **`{cfg.get('one_lifetime_per_user', True)}`**\n"
            f"• 1-Day Win Cooldown: **`{cfg.get('oneday_user_cooldown_hours', 24)}h`**\n"
            f"• Power Hour: **{ph_status}**\n"
            f"• Log Channel: {log_ch}\n"
            f"• Ignored Channels: {ignored_str}"
        ),
        inline=False,
    )

    if recent:
        win_lines = []
        for w in recent:
            icon = "🏴‍☠️" if w["tier"] == "lifetime" else "🗝️"
            ts = int(w["claimed_at"] or 0)
            link = f" ([jump]({w['message_url']}))" if w.get("message_url") else ""
            win_lines.append(f"{icon} <@{w['claimed_by_id']}> — <t:{ts}:R>{link}")
        embed.add_field(name="🏆 Recent Winners", value="\n".join(win_lines), inline=False)
    else:
        embed.add_field(name="🏆 Recent Winners", value="*No keys claimed yet.*", inline=False)

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="setodds", description="[Admin] Update 1-in-N drop odds live")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    oneday_one_in="1 in N chance for 1-Day Key (default: 500)",
    lifetime_one_in="1 in N chance for Lifetime Key (default: 3000)",
)
async def setodds_cmd(
    interaction: discord.Interaction,
    oneday_one_in: app_commands.Range[int, 1, 1000000],
    lifetime_one_in: app_commands.Range[int, 1, 1000000],
) -> None:
    await interaction.response.defer(ephemeral=True)
    db.set_setting("odds_1day", oneday_one_in)
    db.set_setting("odds_lifetime", lifetime_one_in)
    await interaction.followup.send(
        f"✅ Drop odds updated!\n"
        f"• **1-Day Key:** `1 in {oneday_one_in:,}`\n"
        f"• **Lifetime Key:** `1 in {lifetime_one_in:,}`",
        ephemeral=True,
    )


@bot.tree.command(name="setstockcap", description="[Admin] Update max stock caps (default: 30 for 1-Day, 15 for Lifetime)")
@app_commands.default_permissions(administrator=True)
async def setstockcap_cmd(
    interaction: discord.Interaction,
    oneday_cap: app_commands.Range[int, 1, 10000],
    lifetime_cap: app_commands.Range[int, 1, 10000],
) -> None:
    await interaction.response.defer(ephemeral=True)
    db.set_setting("max_stock_1day", oneday_cap)
    db.set_setting("max_stock_lifetime", lifetime_cap)
    await interaction.followup.send(
        f"✅ Max stock caps updated!\n"
        f"• **1-Day Max Stock:** `{oneday_cap}`\n"
        f"• **Lifetime Max Stock:** `{lifetime_cap}`",
        ephemeral=True,
    )


@bot.tree.command(name="setcooldown", description="[Admin] Configure roll cooldown (0 = off) and 1-Day repeat win cooldown")
@app_commands.default_permissions(administrator=True)
async def setcooldown_cmd(
    interaction: discord.Interaction,
    roll_cooldown_seconds: app_commands.Range[float, 0.0, 3600.0],
    oneday_win_cooldown_hours: app_commands.Range[float, 0.0, 720.0] = 24.0,
) -> None:
    await interaction.response.defer(ephemeral=True)
    db.set_setting("roll_cooldown_seconds", roll_cooldown_seconds)
    db.set_setting("oneday_user_cooldown_hours", oneday_win_cooldown_hours)
    await interaction.followup.send(
        f"✅ Cooldowns updated!\n"
        f"• **Message Roll Cooldown:** `{roll_cooldown_seconds:g}s`\n"
        f"• **1-Day Repeat Win Cooldown:** `{oneday_win_cooldown_hours:g}h`",
        ephemeral=True,
    )


@bot.tree.command(name="clearstock", description="[Admin] Remove unclaimed keys from 1-Day or Lifetime stock")
@app_commands.default_permissions(administrator=True)
async def clearstock_cmd(
    interaction: discord.Interaction,
    tier: Literal["1day", "lifetime"],
) -> None:
    await interaction.response.defer(ephemeral=True)
    removed = db.clear_available_keys(tier)
    await interaction.followup.send(
        f"🗑️ Cleared **{removed}** unclaimed key(s) from **{format_tier_name(tier)}**.",
        ephemeral=True,
    )


@bot.tree.command(name="powerhour", description="[Admin] Toggle Fortnite-style Power Hour boosted drop odds!")
@app_commands.default_permissions(administrator=True)
async def powerhour_cmd(
    interaction: discord.Interaction,
    multiplier: app_commands.Range[float, 1.0, 20.0],
    duration_minutes: app_commands.Range[int, 1, 1440] = 60,
    announce_here: bool = True,
) -> None:
    await interaction.response.defer(ephemeral=True)
    if multiplier <= 1.0:
        db.set_power_hour(1.0, 0)
        await interaction.followup.send("⏹️ Power Hour turned **OFF** (back to normal odds).", ephemeral=True)
        return

    ends_at = db.set_power_hour(multiplier, duration_minutes)
    await interaction.followup.send(
        f"⚡ Power Hour started: **`{multiplier:g}x` odds** until <t:{int(ends_at)}:t>!",
        ephemeral=True,
    )
    if announce_here and isinstance(interaction.channel, discord.TextChannel):
        embed = discord.Embed(
            title="⚡🏴‍☠️ PIRATE TOKEN POWER HOUR IS LIVE! 🏴‍☠️⚡",
            description=(
                f"Drop chances in chat are boosted by **`{multiplier:g}x`** for the next **{duration_minutes} minutes**!\n"
                f"⏳ Ends <t:{int(ends_at)}:R> — start chatting to hunt for keys!"
            ),
            color=discord.Color.gold(),
        )
        await interaction.channel.send(embed=embed)


@bot.tree.command(name="ignorechannel", description="[Admin] Toggle ignoring a channel so messages there don't roll keys")
@app_commands.default_permissions(administrator=True)
async def ignorechannel_cmd(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
) -> None:
    await interaction.response.defer(ephemeral=True)
    target = channel or interaction.channel
    if target is None:
        await interaction.followup.send("⚠️ Invalid channel.", ephemeral=True)
        return
    now_ignored = db.toggle_ignore_channel(target.id)
    if now_ignored:
        await interaction.followup.send(
            f"🔇 <#{target.id}> is now **ignored** — messages there will not roll for keys.",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            f"🔊 <#{target.id}> is **enabled** again — messages there can roll for keys!",
            ephemeral=True,
        )


@bot.tree.command(name="setlogchannel", description="[Admin] Set a staff channel to log every key drop")
@app_commands.default_permissions(administrator=True)
async def setlogchannel_cmd(
    interaction: discord.Interaction,
    channel: Optional[discord.TextChannel] = None,
) -> None:
    await interaction.response.defer(ephemeral=True)
    if channel is None:
        db.set_setting("log_channel_id", None)
        await interaction.followup.send("📋 Drop log channel disabled.", ephemeral=True)
    else:
        db.set_setting("log_channel_id", channel.id)
        await interaction.followup.send(
            f"📋 Drop logs will now be sent to {channel.mention}.", ephemeral=True
        )


def run_bot() -> None:
    token = (
        os.getenv("DISCORD_TOKEN")
        or os.getenv("BOT_TOKEN")
        or os.getenv("TOKEN")
        or ""
    ).strip()
    if not token or token == "YOUR_BOT_TOKEN_HERE":
        print("=" * 65)
        print("[ERROR] Missing Discord Bot Token!")
        print("Set DISCORD_TOKEN (or BOT_TOKEN) in Bothost environment variables")
        print("or your .env file before starting the bot.")
        print("=" * 65)
        sys.exit(1)

    try:
        bot.run(token)
    except discord.PrivilegedIntentsRequired:
        print("=" * 70)
        print("[ERROR] 'Message Content Intent' is NOT enabled for your Discord Bot!")
        print("Fix this in 10 seconds:")
        print("  1. Open https://discord.com/developers/applications")
        print("  2. Select your Application -> click 'Bot' on the left sidebar")
        print("  3. Scroll down to 'Privileged Gateway Intents'")
        print("  4. Turn ON 'MESSAGE CONTENT INTENT' and click Save Changes!")
        print("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    run_bot()
