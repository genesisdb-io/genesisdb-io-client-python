import os
import json
from typing import List, Dict, Any, Optional, AsyncGenerator
import httpx
from cloudevents.http import CloudEvent


class Client:
    """Genesis DB Python Client

    This client provides methods to interact with Genesis DB API.
    """

    def __init__(self, config: Optional[Dict[str, str]] = None):
        """Initialize the Genesis DB client

        Args:
            config: Optional configuration dictionary with keys:
                - api_url: The base URL of the Genesis DB API
                - api_version: The API version (e.g., 'v1')
                - auth_token: The authentication token

        If config is not provided or values are missing, environment variables will be used:
            - GENESISDB_API_URL
            - GENESISDB_API_VERSION
            - GENESISDB_AUTH_TOKEN
        """
        if config:
            self.api_url = config.get('api_url') or os.getenv('GENESISDB_API_URL', '')
            self.api_version = config.get('api_version') or os.getenv('GENESISDB_API_VERSION', '')
            self.auth_token = config.get('auth_token') or os.getenv('GENESISDB_AUTH_TOKEN', '')
        else:
            self.api_url = os.getenv('GENESISDB_API_URL', '')
            self.api_version = os.getenv('GENESISDB_API_VERSION', '')
            self.auth_token = os.getenv('GENESISDB_AUTH_TOKEN', '')

        if not self.api_url or not self.api_version or not self.auth_token:
            self._check_required_env()

    def _check_required_env(self):
        """Check that all required environment variables are set"""
        required_env_vars = [
            'GENESISDB_API_URL',
            'GENESISDB_API_VERSION',
            'GENESISDB_AUTH_TOKEN',
        ]

        missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]

        if missing_env_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_env_vars)}")

    async def stream_events(
        self,
        subject: str,
        options: Optional[Dict[str, Any]] = None
    ) -> List[CloudEvent]:
        """Stream events from Genesis DB

        Args:
            subject: The subject to stream events for
            options: Optional dictionary with keys:
                - lower_bound: UUID of the event to start from
                - include_lower_bound_event: Whether to include the lower bound event
                - latest_by_event_type: Only return latest events of this type

        Returns:
            List of CloudEvent objects
        """
        url = f"{self.api_url}/api/{self.api_version}/stream"

        request_body = {"subject": subject}
        if options:
            # Convert snake_case to camelCase for API
            api_options = {}
            if 'lower_bound' in options:
                api_options['lowerBound'] = options['lower_bound']
            if 'include_lower_bound_event' in options:
                api_options['includeLowerBoundEvent'] = options['include_lower_bound_event']
            if 'latest_by_event_type' in options:
                api_options['latestByEventType'] = options['latest_by_event_type']
            request_body['options'] = api_options

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/x-ndjson',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=request_body, headers=headers)
                response.raise_for_status()

                raw_text = response.text

                if not raw_text or raw_text.strip() == '':
                    return []

                events = []
                for line in raw_text.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        json_data = json.loads(line)
                        event = CloudEvent(json_data)
                        events.append(event)
                    except json.JSONDecodeError as err:
                        print(f"Error while parsing event: {err}")
                        print(f"Problem with JSON: {line}")

                return events

            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while streaming events: {error}")
                raise

    async def commit_events(
        self,
        events: List[Dict[str, Any]],
        preconditions: Optional[List[Dict[str, Any]]] = None
    ):
        """Commit events to Genesis DB

        Args:
            events: List of event dictionaries with keys:
                - source: Event source
                - subject: Event subject
                - type: Event type
                - data: Event data
                - options: Optional dictionary with:
                    - store_data_as_reference: Whether to store data as reference (for GDPR)
            preconditions: Optional list of precondition dictionaries with keys:
                - type: Precondition type ('isSubjectNew' or 'isQueryResultTrue')
                - payload: Precondition payload

        Example:
            await client.commit_events([
                {
                    'source': 'io.genesisdb.app',
                    'subject': '/user',
                    'type': 'io.genesisdb.app.user-added',
                    'data': {'name': 'John'}
                }
            ])
        """
        url = f"{self.api_url}/api/{self.api_version}/commit"

        # Convert snake_case to camelCase for API
        formatted_events = []
        for event in events:
            event_data = {
                'source': event['source'],
                'subject': event['subject'],
                'type': event['type'],
                'data': event['data']
            }
            if 'options' in event:
                api_options = {}
                if 'store_data_as_reference' in event['options']:
                    api_options['storeDataAsReference'] = event['options']['store_data_as_reference']
                event_data['options'] = api_options
            formatted_events.append(event_data)

        request_body = {'events': formatted_events}

        if preconditions and len(preconditions) > 0:
            request_body['preconditions'] = preconditions

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=request_body, headers=headers)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while committing events: {error}")
                raise

    async def erase_data(self, subject: str):
        """Erase data for a subject (GDPR compliance)

        Args:
            subject: The subject to erase data for

        Example:
            await client.erase_data('/user/456')
        """
        url = f"{self.api_url}/api/{self.api_version}/erase"

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json={'subject': subject}, headers=headers)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while erasing data: {error}")
                raise

    async def audit(self) -> str:
        """Run an audit to check event consistency

        Returns:
            Audit response text
        """
        url = f"{self.api_url}/api/{self.api_version}/status/audit"

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'text/plain',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return response.text
            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while running audit: {error}")
                raise

    async def ping(self) -> str:
        """Check API status

        Returns:
            Ping response text
        """
        url = f"{self.api_url}/api/{self.api_version}/status/ping"

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'text/plain',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                return response.text
            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while pinging: {error}")
                raise

    async def q(self, query: str) -> List[Any]:
        """Execute a GDBQL query

        Args:
            query: The GDBQL query string

        Returns:
            List of query results
        """
        url = f"{self.api_url}/api/{self.api_version}/q"

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/x-ndjson',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json={'query': query}, headers=headers)
                response.raise_for_status()

                raw_text = response.text

                if not raw_text or raw_text.strip() == '':
                    return []

                results = []
                for line in raw_text.strip().split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        json_data = json.loads(line)
                        results.append(json_data)
                    except json.JSONDecodeError as err:
                        print(f"Error while parsing result: {err}")
                        print(f"Problem with JSON: {line}")

                return results

            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while querying: {error}")
                raise

    async def query_events(self, query: str) -> List[Any]:
        """Query events using GDBQL

        Args:
            query: The GDBQL query string

        Returns:
            List of query results

        Example:
            results = await client.query_events(
                'STREAM e FROM events WHERE e.type == "io.genesisdb.app.customer-added" '
                'ORDER BY e.time DESC LIMIT 20 '
                'MAP { subject: e.subject, firstName: e.data.firstName }'
            )
        """
        return await self.q(query)

    async def observe_events(
        self,
        subject: str,
        options: Optional[Dict[str, Any]] = None
    ) -> AsyncGenerator[CloudEvent, None]:
        """Observe events in real-time (streaming with SSE-like behavior)

        Args:
            subject: The subject to observe events for
            options: Optional dictionary with keys:
                - lower_bound: UUID of the event to start from
                - include_lower_bound_event: Whether to include the lower bound event
                - latest_by_event_type: Only return latest events of this type

        Yields:
            CloudEvent objects as they arrive

        Example:
            async for event in client.observe_events('/customer'):
                print('Received event:', event)
        """
        url = f"{self.api_url}/api/{self.api_version}/observe"

        request_body = {"subject": subject}
        if options:
            # Convert snake_case to camelCase for API
            api_options = {}
            if 'lower_bound' in options:
                api_options['lowerBound'] = options['lower_bound']
            if 'include_lower_bound_event' in options:
                api_options['includeLowerBoundEvent'] = options['include_lower_bound_event']
            if 'latest_by_event_type' in options:
                api_options['latestByEventType'] = options['latest_by_event_type']
            request_body['options'] = api_options

        headers = {
            'Authorization': f'Bearer {self.auth_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/x-ndjson',
            'User-Agent': 'genesisdb-sdk',
        }

        async with httpx.AsyncClient() as client:
            try:
                async with client.stream('POST', url, json=request_body, headers=headers) as response:
                    response.raise_for_status()

                    buffer = ''
                    async for chunk in response.aiter_bytes():
                        buffer += chunk.decode('utf-8')

                        while '\n' in buffer:
                            line, buffer = buffer.split('\n', 1)
                            line = line.strip()

                            if not line:
                                continue

                            try:
                                # Handle SSE-like format
                                json_str = line[6:] if line.startswith('data: ') else line
                                json_data = json.loads(json_str)

                                # Check if this is an empty payload object with only one key
                                if json_data.get('payload') == '' and len(json_data) == 1:
                                    continue

                                event = CloudEvent(json_data)
                                yield event
                            except json.JSONDecodeError as err:
                                print(f"Error while parsing event: {err}")
                                print(f"Problem with JSON: {line}")
                            except Exception as err:
                                print(f"Error creating CloudEvent: {err}")

            except httpx.HTTPStatusError as error:
                print(f"API Error: {error.response.status_code} {error.response.reason_phrase}")
                raise
            except Exception as error:
                print(f"Error while observing events: {error}")
                raise
