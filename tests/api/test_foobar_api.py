"""Tests for the full Foobar CRUD API via /api/v2/foobars endpoints."""

import json
import pytest


class TestFoobarListAll:

    def test_list_foobars_empty(self, client, auth_headers):
        response = client.get('/api/v2/foobars', headers=auth_headers)
        assert response.status_code == 200

        data = response.get_json()
        assert data['size'] == 0
        assert data['total_count'] == 0
        assert data['values'] == []
        assert data['isLastPage'] is True

    def test_list_foobars_after_create(self, client, auth_headers, sample_foobar_data):
        # Create a foobar first
        client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )

        # List foobars
        response = client.get('/api/v2/foobars', headers=auth_headers)
        assert response.status_code == 200

        data = response.get_json()
        assert data['size'] >= 1
        assert data['total_count'] >= 1

    def test_list_foobars_with_pagination(self, client, auth_headers):
        # Create multiple foobars
        for i in range(3):
            foobar_data = {
                'name': f'Foobar {i}',
                'email': f'foobar{i}@example.com',
                'status': 'active'
            }
            client.post(
                '/api/v2/foobars',
                data=json.dumps(foobar_data),
                headers=auth_headers
            )

        # Get first page
        response = client.get('/api/v2/foobars?start=0&limit=2', headers=auth_headers)
        assert response.status_code == 200

        data = response.get_json()
        assert data['limit'] == 2
        assert data['start'] == 0
        assert data['size'] <= 2

    def test_list_foobars_with_status_filter(self, client, auth_headers):
        # Create foobars with different statuses
        for status in ['active', 'processing']:
            foobar_data = {
                'name': f'Foobar {status}',
                'email': f'foobar_{status}@example.com',
                'status': status
            }
            client.post(
                '/api/v2/foobars',
                data=json.dumps(foobar_data),
                headers=auth_headers
            )

        # Filter by status
        response = client.get('/api/v2/foobars?status=active', headers=auth_headers)
        assert response.status_code == 200

    def test_list_foobars_response_structure(self, client, auth_headers):
        response = client.get('/api/v2/foobars', headers=auth_headers)
        data = response.get_json()

        assert 'size' in data
        assert 'total_count' in data
        assert 'limit' in data
        assert 'isLastPage' in data
        assert 'values' in data
        assert 'start' in data
        assert 'filter' in data
        assert 'nextPageStart' in data


class TestFoobarCreate:

    def test_create_foobar(self, client, auth_headers, sample_foobar_data):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        assert response.status_code == 201

        data = response.get_json()
        assert data['name'] == 'Test Foobar'
        assert data['email'] == 'test@example.com'
        assert data['status'] == 'active'
        assert 'key' in data
        assert data['version'] == 0
        assert data['object_type'] == 'foobar'

    def test_create_foobar_sets_system_fields(self, client, auth_headers, sample_foobar_data):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        data = response.get_json()

        assert data['created_user'] == 'testuser'
        assert data['updated_user'] == 'testuser'
        assert isinstance(data['created_dt'], float)
        assert isinstance(data['updated_dt'], float)

    def test_create_foobar_generates_key(self, client, auth_headers, sample_foobar_data):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        data = response.get_json()

        assert 'key' in data
        assert len(data['key']) == 40  # SHA1 hex

    def test_create_foobar_drops_id(self, client, auth_headers, sample_foobar_data):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        data = response.get_json()

        assert '_id' not in data

    def test_create_foobar_missing_name(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'email': 'test@example.com'}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_missing_email(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'name': 'Test'}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_invalid_email(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'name': 'Test', 'email': 'invalid'}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_invalid_status(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({
                'name': 'Test',
                'email': 'test@example.com',
                'status': 'invalid_status'
            }),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_no_body(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_not_json(self, client):
        response = client.post(
            '/api/v2/foobars',
            data='not json',
            content_type='text/plain'
        )
        assert response.status_code == 400

    def test_create_foobar_with_default_status(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'name': 'Test', 'email': 'test@example.com'}),
            headers=auth_headers
        )
        data = response.get_json()
        assert data['status'] == 'active'

    def test_create_foobar_with_phone(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({
                'name': 'Test',
                'email': 'test@example.com',
                'phone': '(202) 555-1234'
            }),
            headers=auth_headers
        )
        assert response.status_code == 201

        data = response.get_json()
        assert data['phone'] == '+12025551234'

    def test_create_foobar_normalizes_email(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({
                'name': 'Test',
                'email': 'TEST@EXAMPLE.COM'
            }),
            headers=auth_headers
        )
        data = response.get_json()
        assert data['email'] == 'test@example.com'

    def test_create_foobar_without_auth_still_works(self, client, json_headers):
        # Auth is not enforced (require_auth is commented out)
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'name': 'Test', 'email': 'test@example.com'}),
            headers=json_headers
        )
        assert response.status_code == 201

    def test_create_foobar_duplicate_name(self, client, auth_headers, sample_foobar_data):
        # Create first
        client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )

        # Create duplicate
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_null_name_rejected(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'name': None, 'email': 'test@example.com'}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_create_foobar_wrong_type_name(self, client, auth_headers):
        response = client.post(
            '/api/v2/foobars',
            data=json.dumps({'name': 123, 'email': 'test@example.com'}),
            headers=auth_headers
        )
        assert response.status_code == 400


class TestFoobarGetByKey:

    def test_get_existing_foobar(self, client, auth_headers, sample_foobar_data):
        # Create a foobar
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        created = create_response.get_json()
        key = created['key']

        # Get by key
        response = client.get(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert response.status_code == 200

        data = response.get_json()
        assert data['key'] == key
        assert data['name'] == 'Test Foobar'
        assert data['email'] == 'test@example.com'

    def test_get_nonexistent_foobar(self, client, auth_headers):
        response = client.get('/api/v2/foobars/nonexistent-key', headers=auth_headers)
        assert response.status_code == 404

    def test_get_foobar_excludes_id(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.get(f'/api/v2/foobars/{key}', headers=auth_headers)
        data = response.get_json()
        assert '_id' not in data


class TestFoobarUpdate:

    def test_update_email(self, client, auth_headers, sample_foobar_data):
        # Create
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        created = create_response.get_json()
        key = created['key']

        # Update
        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'updated@example.com', 'version': 0}),
            headers=auth_headers
        )
        assert response.status_code == 200

        data = response.get_json()
        assert data['email'] == 'updated@example.com'
        assert data['version'] == 1

    def test_update_status(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'status': 'processing', 'version': 0}),
            headers=auth_headers
        )
        assert response.status_code == 200

        data = response.get_json()
        assert data['status'] == 'processing'

    def test_update_missing_version(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'new@example.com'}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_update_version_conflict(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        # First update succeeds
        client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'first@example.com', 'version': 0}),
            headers=auth_headers
        )

        # Second update with old version fails
        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'second@example.com', 'version': 0}),
            headers=auth_headers
        )
        assert response.status_code == 409

    def test_update_nonexistent_foobar(self, client, auth_headers):
        response = client.patch(
            '/api/v2/foobars/nonexistent-key',
            data=json.dumps({'email': 'test@example.com', 'version': 0}),
            headers=auth_headers
        )
        assert response.status_code == 404

    def test_update_invalid_email(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'invalid-email', 'version': 0}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_update_invalid_status(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'status': 'invalid', 'version': 0}),
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_update_no_body(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            headers=auth_headers
        )
        assert response.status_code == 400

    def test_update_preserves_name(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'updated@example.com', 'version': 0}),
            headers=auth_headers
        )
        data = response.get_json()
        assert data['name'] == 'Test Foobar'

    def test_update_increments_version(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'updated@example.com', 'version': 0}),
            headers=auth_headers
        )
        assert response.get_json()['version'] == 1

    def test_update_updated_user_set(self, client, auth_headers, sample_foobar_data):
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({'email': 'updated@example.com', 'version': 0}),
            headers=auth_headers
        )
        data = response.get_json()
        assert data['updated_user'] == 'testuser'


class TestFoobarDelete:

    def test_delete_existing_foobar(self, client, auth_headers, sample_foobar_data):
        # Create
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps(sample_foobar_data),
            headers=auth_headers
        )
        key = create_response.get_json()['key']

        # Delete
        response = client.delete(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert response.status_code == 204

        # Verify deleted
        get_response = client.get(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert get_response.status_code == 404

    def test_delete_nonexistent_foobar(self, client, auth_headers):
        response = client.delete('/api/v2/foobars/nonexistent-key', headers=auth_headers)
        assert response.status_code == 404


class TestUnsupportedObjectType:

    def test_get_unsupported_type(self, client, auth_headers):
        response = client.get('/api/v2/nonexistent_type', headers=auth_headers)
        assert response.status_code == 404

    def test_post_unsupported_type(self, client, auth_headers):
        response = client.post(
            '/api/v2/nonexistent_type',
            data=json.dumps({'name': 'test'}),
            headers=auth_headers
        )
        assert response.status_code == 404


class TestObjectsLoadClass:

    def test_plurals_mapping(self, client, auth_headers):
        # 'foobars' should map to 'foobar' via the plurals dict
        response = client.get('/api/v2/foobars', headers=auth_headers)
        assert response.status_code == 200

    def test_invalid_class_returns_404(self, client, auth_headers):
        response = client.get('/api/v2/widgets', headers=auth_headers)
        assert response.status_code == 404


class TestCRUDFullLifecycle:

    def test_create_read_update_delete(self, client, auth_headers):
        # CREATE
        create_response = client.post(
            '/api/v2/foobars',
            data=json.dumps({
                'name': 'Lifecycle Test',
                'email': 'lifecycle@example.com',
                'phone': '+12025551234',
                'status': 'active'
            }),
            headers=auth_headers
        )
        assert create_response.status_code == 201
        created = create_response.get_json()
        key = created['key']
        assert created['version'] == 0

        # READ
        get_response = client.get(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert get_response.status_code == 200
        item = get_response.get_json()
        assert item['name'] == 'Lifecycle Test'
        assert item['email'] == 'lifecycle@example.com'

        # UPDATE
        update_response = client.patch(
            f'/api/v2/foobars/{key}',
            data=json.dumps({
                'email': 'updated_lifecycle@example.com',
                'status': 'processing',
                'version': 0
            }),
            headers=auth_headers
        )
        assert update_response.status_code == 200
        updated = update_response.get_json()
        assert updated['email'] == 'updated_lifecycle@example.com'
        assert updated['status'] == 'processing'
        assert updated['version'] == 1

        # READ after update
        get_response2 = client.get(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert get_response2.status_code == 200
        item2 = get_response2.get_json()
        assert item2['email'] == 'updated_lifecycle@example.com'
        assert item2['version'] == 1

        # DELETE
        delete_response = client.delete(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert delete_response.status_code == 204

        # READ after delete
        get_response3 = client.get(f'/api/v2/foobars/{key}', headers=auth_headers)
        assert get_response3.status_code == 404
