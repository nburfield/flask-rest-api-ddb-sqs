"""
Manager module for the Flask Application
"""


from flask.cli import FlaskGroup
import serverless_wsgi
import logging

from app import create_app

# Cleanup logging for proper lambda logs
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

# very important for the running of uwsgi to have app here
app = create_app()
cli = FlaskGroup(create_app=create_app)


@cli.command('test')
def test():
    """
    test A test function call as example to build out features
    """
    print("TEST CLI")


def handler(event, context):
    print("=== Starting Flask App ===")
    return serverless_wsgi.handle_request(app, event, context)


if __name__ == '__main__':
    cli()
