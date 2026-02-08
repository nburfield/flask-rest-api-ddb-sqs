# Flask REST API

A Flask REST API that follows the asynchronous task processing architecture.
- AWS SQS for tasks
- S3 for object storage
- DynamoDB for database

# API Object
## Object Requirements
All objects need these apart of them.
- created_user
- created_dt
- updated_user
- updated_dt
- version
- _id
- key
- object_type

## Object Schema Description
- name: The name of the 'key' in this object for this key/value
- default: The default value to set to if not set in API call
- type: The type that this object should be. References a python object. Example str, int, float, dict, etc.
- null: A boolean if this value can be null.
- post_value: A boolean if the POST can set this key/value.
- patch_value: A boolean if the PATCH can update this key/value.
- drop_from_response: A boolean if the API return should send this key/value.
- index: A boolean if the database used should have this as an index.
- unique: A boolean if the database used should restrict this key/value to unique.
- allowed_values: A list of allowed values that this key/value can be.
- update_roles: A list of strings that reference an authenticated role. If this list is present only these set roles can set/update this key/value.
