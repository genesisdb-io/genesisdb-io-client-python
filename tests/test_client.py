import pytest
import os
import uuid
from dotenv import load_dotenv
from genesisdb import Client

# Load environment variables
load_dotenv()


@pytest.fixture
def client():
    """Create a client instance for testing"""
    return Client()


@pytest.mark.asyncio
async def test_ping(client):
    """Test the ping endpoint"""
    response = await client.ping()
    assert response is not None
    print(f"Ping response: {response}")


@pytest.mark.asyncio
async def test_audit(client):
    """Test the audit endpoint"""
    response = await client.audit()
    assert response is not None
    print(f"Audit response: {response}")


@pytest.mark.asyncio
async def test_commit_and_stream_events(client):
    """Test committing events and streaming them back"""
    # Create a unique subject for this test
    test_subject = f"/test/user/{uuid.uuid4()}"

    # Commit test events
    await client.commit_events([
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.user-created',
            'data': {
                'firstName': 'John',
                'lastName': 'Doe',
                'email': 'john.doe@example.com'
            }
        },
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.user-updated',
            'data': {
                'firstName': 'John',
                'lastName': 'Smith',
                'email': 'john.smith@example.com'
            }
        }
    ])

    # Stream events back
    events = await client.stream_events(test_subject)

    assert len(events) == 2
    assert events[0]['type'] == 'io.genesisdb.test.user-created'
    assert events[0]['data']['firstName'] == 'John'
    assert events[0]['data']['lastName'] == 'Doe'
    assert events[1]['type'] == 'io.genesisdb.test.user-updated'
    assert events[1]['data']['lastName'] == 'Smith'

    print(f"Successfully committed and streamed {len(events)} events")


@pytest.mark.asyncio
async def test_commit_with_precondition_is_subject_new(client):
    """Test committing events with isSubjectNew precondition"""
    test_subject = f"/test/unique/{uuid.uuid4()}"

    # First commit should succeed
    await client.commit_events([
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.item-created',
            'data': {'value': 'First'}
        }
    ], [
        {
            'type': 'isSubjectNew',
            'payload': {
                'subject': test_subject
            }
        }
    ])

    # Second commit should fail due to precondition
    with pytest.raises(Exception) as exc_info:
        await client.commit_events([
            {
                'source': 'io.genesisdb.test',
                'subject': test_subject,
                'type': 'io.genesisdb.test.item-created',
                'data': {'value': 'Second'}
            }
        ], [
            {
                'type': 'isSubjectNew',
                'payload': {
                    'subject': test_subject
                }
            }
        ])

    print(f"Precondition correctly failed: {exc_info.value}")


@pytest.mark.asyncio
async def test_query_events(client):
    """Test querying events with GDBQL"""
    # First commit some events
    test_subject = f"/test/query/{uuid.uuid4()}"

    await client.commit_events([
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.query-test',
            'data': {'value': 'test1'}
        },
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.query-test',
            'data': {'value': 'test2'}
        }
    ])

    # Query the events
    results = await client.query_events(
        f"STREAM e FROM events WHERE e.subject == '{test_subject}' "
        f"MAP {{ subject: e.subject, value: e.data.value }}"
    )

    assert len(results) >= 2
    print(f"Query returned {len(results)} results")


@pytest.mark.asyncio
async def test_gdpr_store_and_erase(client):
    """Test GDPR functionality - store data as reference and erase"""
    test_subject = f"/test/gdpr/{uuid.uuid4()}"

    # Commit event with data stored as reference
    await client.commit_events([
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.user-data',
            'data': {
                'firstName': 'Jane',
                'lastName': 'Doe',
                'ssn': '123-45-6789'
            },
            'options': {
                'store_data_as_reference': True
            }
        }
    ])

    # Verify data exists
    events_before = await client.stream_events(test_subject)
    assert len(events_before) == 1
    assert events_before[0]['data']['firstName'] == 'Jane'

    # Erase the data
    await client.erase_data(test_subject)

    # Verify data is erased (event exists but data should be removed/nullified)
    events_after = await client.stream_events(test_subject)
    assert len(events_after) == 1
    # The event should still exist but data should be erased or reference removed
    print(f"Data after erasure: {events_after[0]['data']}")


@pytest.mark.asyncio
async def test_stream_with_lower_bound(client):
    """Test streaming events from a lower bound"""
    test_subject = f"/test/lowerbound/{uuid.uuid4()}"

    # Commit multiple events
    await client.commit_events([
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.event',
            'data': {'seq': 1}
        },
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.event',
            'data': {'seq': 2}
        },
        {
            'source': 'io.genesisdb.test',
            'subject': test_subject,
            'type': 'io.genesisdb.test.event',
            'data': {'seq': 3}
        }
    ])

    # Get all events
    all_events = await client.stream_events(test_subject)
    assert len(all_events) == 3

    # Get events from second event onwards
    second_event_id = all_events[1]['id']
    events_from_second = await client.stream_events(test_subject, {
        'lower_bound': second_event_id,
        'include_lower_bound_event': True
    })

    assert len(events_from_second) == 2
    assert events_from_second[0]['data']['seq'] == 2
    assert events_from_second[1]['data']['seq'] == 3

    print(f"Lower bound test: got {len(events_from_second)} events from bound")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
