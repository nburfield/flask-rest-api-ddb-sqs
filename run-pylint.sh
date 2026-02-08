#!/bin/bash
pylint manage.py app/ app/api_v1/ app/api_v2/ app/base/ app/helpers/ app/models/ app/repositories/ app/schemas/ app/services/ --output-format=json
