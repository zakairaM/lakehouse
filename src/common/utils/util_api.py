# Databricks notebook source
# MAGIC %md
# MAGIC # API Utility Functions
# MAGIC
# MAGIC This module provides utility functions for API interactions, including authentication and data retrieval.
# MAGIC

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

# COMMAND ----------

class APIClient:
    """
    Generic API Client for interacting with REST APIs.
    Handles authentication, token management, and data retrieval.
    """
    
    def __init__(self, base_url: str):
        """
        Initialize API Client with base URL.
        
        Args:
            base_url: The base URL of the API
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.token = None

    def get_token(self, payload: dict, login_url) -> str:
        """
        Authenticate with the Entersoft API and retrieve the access token.
        """

        # Correct login URL (double /login as per API spec)
        login_url = f"{self.base_url}{login_url}"

        try:
            response = self.session.post(login_url, json=payload)
            response.raise_for_status()

            auth_data = response.json()

            # ✅ The token is nested under Model.WebApiToken
            self.token = auth_data.get("Model", {}).get("WebApiToken")

            if not self.token:
                raise Exception(f"Failed to retrieve token from response: {auth_data}")

            print(f"✅ Successfully authenticated")
            return self.token

        except requests.exceptions.RequestException as e:
            raise Exception(f"❌ Authentication failed: {e}")

    def build_auth(self, auth_mode: str, token: str):
        """
        Return (auth, headers) based on auth_mode in a clean, extensible way.
        """
        # Define strategies as small, composable functions
        strategies = {
            "basic": lambda t: (HTTPBasicAuth(t, ""), {}),
            "header": lambda t: (None, {
                "Authorization": f"Bearer {t}",
                "Content-Type": "application/json"
            })
        }

        # Default fallback (header mode)
        strategy = strategies.get(auth_mode.lower(), strategies["header"])

        return strategy(token)


    
    def get_data_2(self, record_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retrieve data from API based on configuration.
        
        Args:
            record_config: Dictionary containing:
                - entity: Entity name (e.g., "Documents", "Collections")
                - category: Category name (e.g., "Web_Scrolls")
                - params: Dict of query parameters with datetime values
                - token: Authentication token
                
        Returns:
            Dictionary containing API response data
            
        Raises:
            Exception: If data retrieval fails
        """
        if not self.token:
            raise Exception("Not authenticated. Call login() first.")
        
        entity = record_config.get('entity')
        category = record_config.get('category')
        params = record_config.get('params', {})
        
        # Build API endpoint
        data_url = f"{self.base_url}/api/rpc/PublicQuery/{category}/{entity}"
        
        # Build query parameters
        query_params = []
        for param_name, param_value in params.items():
            if isinstance(param_value, datetime):
                # Format datetime as ISO string
                query_params = {param_name: param_value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}
                date_str = param_value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
                query_params.append(f"{param_name} ge datetime'{date_str}'")

        # Construct full URL with filters
        if query_params:
            filter_str = " and ".join(query_params)
            full_url = f"{data_url}?$filter={filter_str}"
        else:
            full_url = data_url
        
        print(f"📡 Fetching data from: {entity}/{category}")
        print(f"🔍 Filter: {query_params if query_params else 'None'}")
        
        try:
            headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': 'application/json'
            }
            
            response = self.session.get(full_url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            # Extract row count if available
            row_count = len(data.get('Rows', []))
            print(f"✅ Retrieved {row_count} records from {entity}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"❌ Failed to retrieve data from {entity}: {e}")

    def get_data(self, auth_mode, token, endpoint, record_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retrieve data from API based on configuration (similar to get_data).
        
        Args:
            record_config: Dictionary containing:
                - entity: Entity name (e.g., "Documents", "Collections")
                - category: Category name (e.g., "Web_Scrolls")
                - params: Dict of query parameters with datetime values
                - token: Authentication token
                    
        Returns:
            Dictionary containing API response data
                
        Raises:
            Exception: If data retrieval fails
        """
        if not self.token and 'token' not in record_config:
            raise Exception("Not authenticated. Call login() first or provide token in record_config.")
        
        entity = record_config.get('entity','') or ''
        category = record_config.get('category') or ''
        params = record_config.get('params', {}) or ''
        auth, headers = self.build_auth(auth_mode, token)

        # Build API endpoint
        #endpoint = f"{endpoint}/{category}/{entity}"

        path_segments = [self.base_url.rstrip('/'), endpoint.strip('/'), category.strip('/'), entity.strip('/')]

        url = '/'.join(segment for segment in path_segments if segment)

        # Convert datetime params to ISO format if needed
        query_params = {}

        if isinstance(params, dict):
            # Convert datetime values to ISO strings if necessary
            for k, v in params.items():
                if isinstance(v, datetime):
                    query_params[k] = v.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
                else:
                    query_params[k] = v
        elif isinstance(params, (list, tuple)):
            # Convert list of keys into dict with None or empty values
            query_params = {key: '' for key in params}
        elif isinstance(params, str) and params.strip():
            # Single string param — treat it as a key with empty value
            query_params = {params: ''}
        else:
            # No params or invalid type
            query_params = {}

        print(f"📡 Fetching data from: {entity}/{category}")
        print(f"🔍 Params: {query_params if query_params else 'None'}")

        try:
            #response = self.session.get(url, headers=headers, params=query_params, timeout=30)
            #response = requests.get(url, auth=HTTPBasicAuth(token, ''))
            response = requests.get(url, headers=headers or None, auth=auth, params=query_params, timeout=30)
            response.raise_for_status()
            data = response.json()
            row_count = len(data.get('Rows', []))
            print(f"✅ Retrievedrecords from {entity}")
            return data
        except requests.RequestException as e:
            print(f"❌ Failed to retrieve data from {entity}: {e}")
            return {
                "success": False,
                "error": str(e),
                "status_code": getattr(e.response, "status_code", None),
                "response": getattr(e.response, "text", None)
            }

    
    def close(self):
        """Close the session."""
        self.session.close()
        print("🔒 API session closed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def format_datetime_for_api(dt: datetime) -> str:
    """
    Format datetime for API query parameters.
    
    Args:
        dt: Datetime object
        
    Returns:
        Formatted datetime string (ISO format)
    """
    return dt.strftime('%Y-%m-%dT%H:%M:%S')

def get_date_range(days_back: int = 1) -> tuple:
    """
    Get date range for API queries.
    
    Args:
        days_back: Number of days to look back
        
    Returns:
        Tuple of (start_datetime, end_datetime)
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    return start_date, end_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage
# MAGIC
# MAGIC ```python
# MAGIC # Initialize client
# MAGIC client = APIClient("https://api.entersoft.gr")
# MAGIC
# MAGIC # Authenticate
# MAGIC token = client.login(
# MAGIC     subscription_id="enartia",
# MAGIC     subscription_password="!Enartia1",
# MAGIC     bridge_id="Ecom",
# MAGIC     user_id="webapi",
# MAGIC     password="En@rti@",
# MAGIC     branch_id="1"
# MAGIC )
# MAGIC
# MAGIC # Get data
# MAGIC record_config = {
# MAGIC     'entity': 'Documents',
# MAGIC     'category': 'Web_Scrolls',
# MAGIC     'params': {
# MAGIC         'ADRegistrationDate': datetime.now() - timedelta(days=1)
# MAGIC     },
# MAGIC     'token': token
# MAGIC }
# MAGIC
# MAGIC data = client.get_data(record_config)
# MAGIC
# MAGIC # Close session
# MAGIC client.close()
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC     def login2(
# MAGIC         self,
# MAGIC         subscription_id: str,
# MAGIC         subscription_password: str,
# MAGIC         bridge_id: str,
# MAGIC         user_id: str,
# MAGIC         password: str,
# MAGIC         branch_id: str
# MAGIC     ) -> str:
# MAGIC         """
# MAGIC         Authenticate with the API and retrieve access token.
# MAGIC         
# MAGIC         Args:
# MAGIC             subscription_id: Subscription identifier
# MAGIC             subscription_password: Subscription password
# MAGIC             bridge_id: Bridge identifier
# MAGIC             user_id: User identifier
# MAGIC             password: User password
# MAGIC             branch_id: Branch identifier
# MAGIC             
# MAGIC         Returns:
# MAGIC             Access token string
# MAGIC             
# MAGIC         Raises:
# MAGIC             Exception: If authentication fails
# MAGIC         """
# MAGIC         login_url = f"{self.base_url}/api/login"
# MAGIC         
# MAGIC         payload = {
# MAGIC             "SubscrId": subscription_id,
# MAGIC             "SubscrPassword": subscription_password,
# MAGIC             "BridgeId": bridge_id,
# MAGIC             "UserId": user_id,
# MAGIC             "Password": password,
# MAGIC             "BranchId": branch_id
# MAGIC         }
# MAGIC         
# MAGIC         try:
# MAGIC             response = self.session.post(login_url, json=payload)
# MAGIC             response.raise_for_status()
# MAGIC             
# MAGIC             auth_data = response.json()
# MAGIC             self.token = auth_data.get('Model', {}).get('WebApiToken')
# MAGIC             
# MAGIC             if not self.token:
# MAGIC                 raise Exception(f"Failed to retrieve token from response: {auth_data}")
# MAGIC             
# MAGIC             print(f"✅ Successfully authenticated")
# MAGIC             return self.token
# MAGIC             
# MAGIC         except requests.exceptions.RequestException as e:
# MAGIC             raise Exception(f"❌ Authentication failed: {e}")
# MAGIC     
# MAGIC
# MAGIC >>>>>>> Stashed changes
