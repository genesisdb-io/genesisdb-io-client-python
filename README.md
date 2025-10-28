# Python SDK

This is the official Python SDK for Genesis DB, an awesome and production ready event store database system for building event-driven apps.

## Genesis DB Advantages

* Incredibly fast when reading, fast when writing 🚀
* Easy backup creation and recovery
* [CloudEvents](https://cloudevents.io/) compatible
* GDPR-ready
* Easily accessible via the HTTP interface
* Auditable. Guarantee database consistency
* Logging and metrics for Prometheus
* SQL like query language called Genesis DB Query Language (GDBQL)
* ...

## Installation

```bash
pip install https://github.com/genesisdb-io/genesisdb-io-client-python.git
```

## Configuration

### Environment Variables
The following environment variables are required:
```
GENESISDB_AUTH_TOKEN=<secret>
GENESISDB_API_URL=http://localhost:8080
GENESISDB_API_VERSION=v1
```

### Basic Setup

```python
from genesisdb import Client

# Initialize the Genesis DB client
client = Client()
```

## Streaming Events

### Basic Event Streaming

```python
# Stream all events for a subject
events = await client.stream_events('/customer')
```

### Stream Events from Lower Bound

```python
events = await client.stream_events('/', {
    'lower_bound': '2d6d4141-6107-4fb2-905f-445730f4f2a9',
    'include_lower_bound_event': True
})
```


### Stream Latest Events by Event Type

```python
events = await client.stream_events('/', {
    'latest_by_event_type': 'io.genesisdb.app.customer-updated'
})
```

This feature allows you to stream only the latest event of a specific type for each subject. Useful for getting the current state of entities.

## Committing Events

### Basic Event Committing

```python
await client.commit_events([
    {
        'source': 'io.genesisdb.app',
        'subject': '/customer',
        'type': 'io.genesisdb.app.customer-added',
        'data': {
            'firstName': 'Bruce',
            'lastName': 'Wayne',
            'emailAddress': 'bruce.wayne@enterprise.wayne'
        }
    },
    {
        'source': 'io.genesisdb.app',
        'subject': '/customer',
        'type': 'io.genesisdb.app.customer-added',
        'data': {
            'firstName': 'Alfred',
            'lastName': 'Pennyworth',
            'emailAddress': 'alfred.pennyworth@enterprise.wayne'
        }
    },
    {
        'source': 'io.genesisdb.store',
        'subject': '/article',
        'type': 'io.genesisdb.store.article-added',
        'data': {
            'name': 'Tumbler',
            'color': 'black',
            'price': 2990000.00
        }
    },
    {
        'source': 'io.genesisdb.app',
        'subject': '/customer/fed2902d-0135-460d-8605-263a06308448',
        'type': 'io.genesisdb.app.customer-personaldata-changed',
        'data': {
            'firstName': 'Angus',
            'lastName': 'MacGyver',
            'emailAddress': 'angus.macgyer@phoenix.foundation'
        }
    }
])
```

## Preconditions

Preconditions allow you to enforce certain checks on the server before committing events. Genesis DB supports multiple precondition types:

### isSubjectNew
Ensures that a subject is new (has no existing events):

```python
await client.commit_events([
    {
        'source': 'io.genesisdb.app',
        'subject': '/user/456',
        'type': 'io.genesisdb.app.user-created',
        'data': {
            'firstName': 'John',
            'lastName': 'Doe',
            'email': 'john.doe@example.com'
        }
    }
], [
    {
        'type': 'isSubjectNew',
        'payload': {
            'subject': '/user/456'
        }
    }
])
```

### isSubjectExisting
Ensures that events exist for the specified subject:

```python
await client.commit_events([
    {
        'source': 'io.genesisdb.app',
        'subject': '/user/456',
        'type': 'io.genesisdb.app.user-created',
        'data': {
            'firstName': 'John',
            'lastName': 'Doe',
            'email': 'john.doe@example.com'
        }
    }
], [
    {
        'type': 'isSubjectExisting',
        'payload': {
            'subject': '/user/456'
        }
    }
])
```

### isQueryResultTrue
Evaluates a query and ensures the result is truthy. Supports the full GDBQL feature set including complex WHERE clauses, aggregations, and calculated fields.

**Basic uniqueness check:**
```python
await client.commit_events([
    {
        'source': 'io.genesisdb.app',
        'subject': '/user/456',
        'type': 'io.genesisdb.app.user-created',
        'data': {
            'firstName': 'John',
            'lastName': 'Doe',
            'email': 'john.doe@example.com'
        }
    }
], [
    {
        'type': 'isQueryResultTrue',
        'payload': {
            'query': "STREAM e FROM events WHERE e.data.email == 'john.doe@example.com' MAP COUNT() == 0"
        }
    }
])
```

**Business rule enforcement (transaction limits):**
```python
await client.commit_events([
    {
        'source': 'io.genesisdb.banking',
        'subject': '/user/123/transactions',
        'type': 'io.genesisdb.banking.transaction-processed',
        'data': {
            'amount': 500.00,
            'currency': 'EUR'
        }
    }
], [
    {
        'type': 'isQueryResultTrue',
        'payload': {
            'query': "STREAM e FROM events WHERE e.subject UNDER '/user/123' AND e.type == 'transaction-processed' AND e.time >= '2024-01-01T00:00:00Z' MAP SUM(e.data.amount) + 500 <= 10000"
        }
    }
])
```

**Complex validation with aggregations:**
```python
await client.commit_events([
    {
        'source': 'io.genesisdb.events',
        'subject': '/conference/2024/registrations',
        'type': 'io.genesisdb.events.registration-created',
        'data': {
            'attendeeId': 'att-789',
            'ticketType': 'premium'
        }
    }
], [
    {
        'type': 'isQueryResultTrue',
        'payload': {
            'query': "STREAM e FROM events WHERE e.subject UNDER '/conference/2024/registrations' AND e.type == 'registration-created' GROUP BY e.data.ticketType HAVING e.data.ticketType == 'premium' MAP COUNT() < 50"
        }
    }
])
```

**Supported GDBQL Features in Preconditions:**
- WHERE conditions with AND/OR/IN/BETWEEN operators
- Hierarchical subject queries (UNDER, DESCENDANTS)
- Aggregation functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY with HAVING clauses
- ORDER BY and LIMIT clauses
- Calculated fields and expressions
- Nested field access (e.data.address.city)
- String concatenation and arithmetic operations

If a precondition fails, the commit returns HTTP 412 (Precondition Failed) with details about which condition failed.

## GDPR Compliance

### Store Data as Reference

```python
await client.commit_events([
    {
        'source': 'io.genesisdb.app',
        'subject': '/user/456',
        'type': 'io.genesisdb.app.user-created',
        'data': {
            'firstName': 'John',
            'lastName': 'Doe',
            'email': 'john.doe@example.com'
        },
        'options': {
            'store_data_as_reference': True
        }
    }
])
```

### Delete Referenced Data

```python
await client.erase_data('/user/456')
```

## Observing Events

### Basic Event Observation

```python
async for event in client.observe_events('/customer'):
    print('Received event:', event)
```

### Observe Events from Lower Bound (Message Queue)

```python
async for event in client.observe_events('/customer', {
    'lower_bound': '2d6d4141-6107-4fb2-905f-445730f4f2a9',
    'include_lower_bound_event': True
}):
    print('Received event:', event)
```


### Observe Latest Events by Event Type (Message Queue)

```python
async for event in client.observe_events('/customer', {
    'latest_by_event_type': 'io.genesisdb.app.customer-updated'
}):
    print('Received latest event:', event)
```

## Querying Events

```python
results = await client.query_events(
    'STREAM e FROM events WHERE e.type == "io.genesisdb.app.customer-added" '
    'ORDER BY e.time DESC LIMIT 20 '
    'MAP { subject: e.subject, firstName: e.data.firstName }'
)
print('Query results:', results)
```

## Health Checks

```python
# Check API status
ping_response = await client.ping()
print('Ping response:', ping_response)

# Run audit to check event consistency
audit_response = await client.audit()
print('Audit response:', audit_response)
```

## License

MIT

## Author

* E-Mail: mail@genesisdb.io
* URL: https://www.genesisdb.io
* Docs: https://docs.genesisdb.io
