"""Foobar helper module for foobar-specific operations and messaging."""

from flask import current_app

from app.base.base_helper import BaseHelper
from app.models.foobar import FoobarModel
from app.services.sqs_factory import SQSFactory


class FoobarHelper(BaseHelper):
    """Helper class for foobar-specific operations including SQS messaging.

    This class extends BaseHelper to provide foobar-specific functionality
    such as status change notifications and password reset messaging.
    """

    def __init__(self):
        super().__init__(FoobarModel)

    def update(self, key, data, user, *, mode: str = "patch"):
        """
        Update an existing foobar

        Args:
            key: The key of the foobar to update
            data: The data to update
            user: The user updating the item
        """
        # Get current foobar data before update
        current_foobar = self.model.get(key)
        if not current_foobar:
            return super().update(key, data, user)

        # Store old status for comparison
        old_status = current_foobar.get('status')

        # Perform the update
        result = super().update(key, data, user, mode=mode)

        if data.get('status') == 'active' and old_status != 'active':
            self.publish_foobar_request(current_campaign)

        return result


    def publish_foobar_request(self, foobar_data):
        """
        Publish password reset request message to SQS

        Args:
            foobar_data: Foobar data including reset token information
        """
        try:
            # Get SQS service and publish message
            sqs_service = SQSFactory.get_service()
            success = sqs_service.publish_message('foobar_requests', foobar_data)

            if success:
                current_app.logger.info(f"Published foobar request for foobar {foobar_data.get('name')}")
            else:
                current_app.logger.error(f"Failed to publish password reset request for foobar {foobar_data.get('name')}")

        except Exception as e:
            current_app.logger.error(f"Error publishing foobar request: {str(e)}")
