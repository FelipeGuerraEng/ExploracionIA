import os
import pdb
import logging
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

logger = logging.getLogger('logger_app.settings')

# load_dotenv(override=True)

class Config:
    """Base configuration class."""

    AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
    AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
    AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
    # Add other static or environment-based configurations here.


class SecretManager:
    """Manages secrets from Azure Key Vault or environment variables."""

    def __init__(self, use_key_vault=True):
        self.use_key_vault = use_key_vault
        self.key_vault_name = os.getenv("KEY_VAULT_NAME", "kv-igac-lakehouse")
        self.kv_uri = f"https://{self.key_vault_name}.vault.azure.net"
        self.credential = DefaultAzureCredential(
                                exclude_environment_credential=True,  # Include environment credential if needed
                                exclude_managed_identity_credential=True,  # Exclude Managed Identity
                                # exclude_cli_credential=True,  # Exclude Azure CLI authentication
                                exclude_visual_studio_code_credential=True,  # Exclude VS Code credential
                                exclude_powershell_credential=True,  # Exclude Azure PowerShell credential
                                exclude_interactive_browser_credential=True,  # Exclude browser-based authentication
                                # shared_cache_tenant_id="your-tenant-id",  # Specify your tenant ID
                            )
        self.client = (
            SecretClient(vault_url=self.kv_uri, credential=self.credential)
            if use_key_vault
            else None
        )

    def get_secret(self, name):
        """Retrieve secret from Azure Key Vault or environment."""
        if self.use_key_vault:
            try:
                secret = self.client.get_secret(name).value
                logger.info(f"Secret {name} fetched from Key Vault.")
                return secret
            except Exception as e:
                logger.exception(f"Failed to fetch {name} from Key Vault: {e}")
        # Fallback to environment variable if Key Vault fails or not used
        secret = os.getenv(name)
        if secret:
            logger.info(f"Secret {name} fetched from environment.")
        else:
            logger.error(f"Secret {name} not found in environment.")
        return secret
