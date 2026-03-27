"""Application settings and Fernet key factory for bank-email-fetcher.

Settings are loaded from environment variables or a .env file via
pydantic-settings. All fields have defaults except EMAIL_SOURCE_MASTER_KEY,
which should be set to a Fernet key generated with:

    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

If EMAIL_SOURCE_MASTER_KEY is not set, get_fernet() generates an ephemeral
key and emits a warning. Credentials encrypted with an ephemeral key will
not survive a server restart.

get_fernet() is a factory function (not a module-level instance) so that it
can be called lazily from functions that need it. This avoids import-time
failures when the key is not yet set in the environment.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    db_url: str = "sqlite+aiosqlite:///./data/bank_email_fetcher.db"
    poll_interval_minutes: int = 15
    poll_fetch_limit_per_rule: int = 50

    email_source_master_key: str = ""  # Fernet key for encrypting credentials

    telegram_bot_token: str = ""
    telegram_chat_id: int = 0


settings = Settings()


def get_fernet():
    from cryptography.fernet import Fernet

    key = settings.email_source_master_key
    if not key:
        # Auto-generate and warn (for dev convenience)
        import warnings
        key = Fernet.generate_key().decode()
        warnings.warn(
            "No EMAIL_SOURCE_MASTER_KEY set. Generated ephemeral key. "
            "Set it in .env for persistence."
        )
    return Fernet(key.encode() if isinstance(key, str) else key)
