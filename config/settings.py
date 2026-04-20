"""
Application settings — loaded from environment / .env file.
"""
import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, Dict, Any
from dotenv import set_key


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    datahub_gms_url: str = "http://localhost:8080"
    datahub_token: Optional[str] = None

    ollama_base_url: str = "http://localhost:11434"
    anthropic_api_key: Optional[str] = None
    llm_model: str = "deepseek-r1:1.5b"
    llm_enabled: bool = True

    app_title: str = "Pipeline Intelligence Engine"
    app_version: str = "1.0.0"

    # Cloud Providers & Tools
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None
    azure_subscription_id: Optional[str] = None
    azure_redirect_uri: str = "http://localhost:5000/getAToken"
    app_secret_key: str = "super-secret-key-change-me" # For SessionMiddleware

    gcp_project_id: Optional[str] = None
    gcp_mock_enabled: bool = False

    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None

    snowflake_account: Optional[str] = None
    snowflake_user: Optional[str] = None
    snowflake_password: Optional[str] = None

    # Local workspace discovery (POST /discover/workspace): semicolon-separated roots; empty = only under cwd
    pipeline_workspace_roots: Optional[str] = None

    def update_keys(self, new_keys: Dict[str, Any]) -> None:
        """
        Dynamically internalize keys and persist to the .env file.
        """
        env_path = ".env"
        if not os.path.exists(env_path):
            with open(env_path, "a") as f:
                pass

        for key, value in new_keys.items():
            if hasattr(self, key):
                setattr(self, key, value)
                # Ensure we format Pydantic fields to upper case for env vars
                env_key = key.upper()
                if value is not None:
                    set_key(env_path, env_key, str(value))


settings = Settings()
