"""
Created: Nov 4, 2025
By: Adam M.
Generalised: 2025-12-29
Objective: Authentication management for Azure and Service Principals.
"""
import os
import pyodbc
from azure.identity import ClientSecretCredential
from typing import Optional, Union

# Environment variable keys
ENV_TENANT = ["FABRIC_SP_TENANT", "FABRIC_TENANT_ID", "TENANT"]
ENV_CLIENT = ["FABRIC_SP_CLIENT_ID", "FABRIC_CLIENT_ID", "CLIENT"]
ENV_SECRET = ["FABRIC_SP_CLIENT_SECRET", "FABRIC_CLIENT_SECRET", "CLIENT_SECRET"]

def get_env_value(keys):
    for k in keys:
        if os.environ.get(k):
            return os.environ.get(k)
    return None

class AuthManager:
    def __init__(self, tenant_id: str = None, client_id: str = None, client_secret: str = None):
        self.tenant_id = tenant_id or get_env_value(ENV_TENANT)
        self.client_id = client_id or get_env_value(ENV_CLIENT)
        self.client_secret = client_secret or get_env_value(ENV_SECRET)
        self.credential = None
        
        if self.tenant_id and self.client_id and self.client_secret:
            try:
                self.credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
            except Exception as e:
                print(f"Failed to create ClientSecretCredential: {e}")

    def get_token_credential(self) -> Optional[ClientSecretCredential]:
        return self.credential
    
    def get_access_token(self, resource: str = "https://database.windows.net/.default") -> Optional[bytes]:
        if self.credential:
            token = self.credential.get_token(resource).token
            # ODBC expects UTF-16LE bytes for the token (attribute 1256)
            return token.encode("utf-16-le") 
        return None
    
    def has_sp_credentials(self) -> bool:
        return bool(self.tenant_id and self.client_id and self.client_secret)
