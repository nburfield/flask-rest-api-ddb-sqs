"""Base repository interface for database operations."""

class BaseRepository:
    """Base repository class defining the interface for database operations.
    
    This abstract base class defines the standard interface that all repository
    implementations must follow for CRUD operations and other database tasks.
    """

    def __init__(self, key_field: str = "key"):
        """Initialize the repository with the specified key field.
        
        Args:
            key_field: The name of the field used as the primary key
        """
        self.key_field = key_field

    def create(self, item: dict):
        """Create a new item in the database.
        
        Args:
            item: The item data to create
            
        Returns:
            The created item
        """
        raise NotImplementedError

    def update(self, key: str, data: dict):
        """Update an existing item in the database.
        
        Args:
            key: The key of the item to update
            data: The data to update
            
        Returns:
            The updated item
        """
        raise NotImplementedError

    def update_by_version(self, key: str, data: dict, expected_version: int):
        """
        Update an item atomically with version checking

        Args:
            key: The key of the item to update
            data: The data to update (should include incremented version)
            expected_version: The version that must match for the update to succeed

        Returns:
            The updated item if successful, None if version doesn't match

        Raises:
            Exception: If the update fails for other reasons
        """
        raise NotImplementedError

    def update_append_only_fields(self, key: str, append_operations: dict):
        """
        Update append-only fields without version checking using MongoDB operators

        Args:
            key: The key of the item to update
            append_operations: Dictionary with field names as keys and lists of values to append

        Returns:
            The updated item if successful, None if item not found

        Raises:
            Exception: If the update fails for other reasons
        """
        raise NotImplementedError

    def delete(self, key: str):
        """Delete an item from the database.
        
        Args:
            key: The key of the item to delete
            
        Returns:
            True if successful, or the deleted item
        """
        raise NotImplementedError

    def get(self, key: str):
        """Get a single item by key.
        
        Args:
            key: The key of the item to retrieve
            
        Returns:
            The item if found, None otherwise
        """
        raise NotImplementedError

    def get_by_field(self, field_name: str, field_value: str):
        """
        Get a single item by a specific field value

        Args:
            field_name: Name of the field to search by
            field_value: Value to search for

        Returns:
            The item if found, None otherwise
        """
        raise NotImplementedError

    def list_all(self, object_type: str, filters: dict = None):
        """List all items with optional filtering.
        
        Args:
            object_type: Type of objects to retrieve
            filters: Optional filters to apply
            
        Returns:
            List of items matching the criteria
        """
        raise NotImplementedError

    def list_all_paginated(self, object_type: str, filters: dict = None, start: int = 0, limit: int = 50):
        """
        List items with pagination and return total count

        Args:
            object_type: Type of objects to retrieve
            filters: Parsed filters from QueryParser
            start: Starting index for pagination
            limit: Maximum number of items to return

        Returns:
            Tuple of (results_list, total_count)
        """
        raise NotImplementedError

    def check_unique_values(self, object_type: str, unique_fields: dict, exclude_key: str = None):
        """
        Check if any of the provided unique field values already exist in the database

        Args:
            object_type: Type of objects to check
            unique_fields: Dictionary of field_name: value pairs to check for uniqueness
            exclude_key: Optional key to exclude from the check (for updates)

        Returns:
            List of field names that already exist, empty list if all are unique
        """
        raise NotImplementedError
