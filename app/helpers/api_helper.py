"""
Helper functions to assist in the API feature of the applicaiont
"""

import json


def make_list_api_response(values: list, start: int, limit: int,
                           is_last: bool, filter_str: str, total_count: int) -> dict:
    """
    make_api_response A single location to make the response object
    that all multi object GETs should return

    :param values: List of values to be the payload of the dict
    :param start: That start value used to generate the values
    :param limit: The value the results were limited to
    :param is_last: Boolean to describe if the return data is the last avaiable
    :param filter_str: String containing the filters used to generate the data
    :param total_count: The total number of objects that can be returned
    :return A python dict that is proper API format
    """

    data = {
        "size": len(values),
        "total_count": total_count,
        "limit": limit,
        "isLastPage": is_last,
        "values": values,
        "start": start,
        "filter": filter_str,
        "nextPageStart": None if is_last else start + limit
    }

    return data


def make_api_message(status: str, message: str) -> dict:
    """
    make_error_api_response Makes the error return object

    :param status: The status of the message
    :param message: The error message to set
    :return The dict error object
    """

    data = {
        "status": status,
        "message": message
    }

    return data


def get_arg_list(request_args, search_key, default_list, current_filter):
    """
    get_arg_list Get the list data from the URL Args

    :param request_args: The request args
    :param search_key: The Key to look for in args
    :param default_list: The default return data
    :param current_filter: Current filter string values
    :return Tuple with the value and the filter string
    """

    # Check if the args has the key to search
    if search_key in request_args:
        # Get the list data from args
        search_list = request_args.getlist(search_key)

        # Build the filter string and preserve existing values
        current_filter = "" if current_filter is None else current_filter
        filter_str = f"{search_key}=" + f"&{search_key}=".join(search_list)
        filter_str = filter_str if len(current_filter) == 0 else current_filter + "&" + filter_str

        # Return tuple results
        return search_list, filter_str

    # Return default tuple results
    return default_list, current_filter


def get_arg_value(request_args, search_key, default_value, current_filter):
    """
    get_arg_value Get the value data from the URL Args

    :param request_args: The request args
    :param search_key: The Key to look for in args
    :param default_value: The default return data
    :param current_filter: Current filter string values
    :return Tuple with the value and the filter string
    """

    # Check if the args has the key to search
    if search_key in request_args:
        # Get the value data from args
        search_value = request_args[search_key]

        # Build the filter string and preserve existing values
        current_filter = "" if current_filter is None else current_filter
        filter_str = f'{search_key}={search_value}'
        filter_str = filter_str if len(current_filter) == 0 else current_filter + "&" + filter_str

        # Return tuple results
        return search_value, filter_str

    # Return default tuple results
    return default_value, current_filter


def get_arg_dict(request_args, search_key, default_value, current_filter):
    """
    get_arg_dict Get the dict data from the URL Args

    :param request_args: The request args
    :param search_key: The Key to look for in args
    :param default_value: The default return data
    :param current_filter: Current filter string values
    :return Tuple with the value and the filter string
    """

    # Check if the args has the key to search
    if search_key in request_args:
        # Get the value data from args
        search_value = request_args[search_key]

        # Build the filter string and preserve existing values
        current_filter = "" if current_filter is None else current_filter
        filter_str = f'{search_key}={search_value}'
        filter_str = filter_str if len(current_filter) == 0 else current_filter + "&" + filter_str

        try:
            # Return tuple results
            return json.loads(search_value), filter_str
        except Exception as exc:
            raise Exception(f"Could not convert '{search_key}' query parameters to dict.") from exc

    # Return default tuple results
    return default_value, current_filter


def get_start_limit(request_args, *, start_default, limit_default, current_filter):
    """
    get_start_limit Get the 'start' and 'limit' value from the request args

    :param request_args: The request args
    :param start_default: The default for 'start'
    :param limit_default: The default for 'limit'
    :param current_filter: Current filter string values
    :return Tuple with the start, limit, and filter values
    """

    # Setup default
    start = start_default
    limit = limit_default
    filter_str = current_filter

    # Load start int
    if "start" in request_args:
        try:
            # Get the value and convert to int
            start = int(request_args["start"])

            # Set the filter string
            filter_str = "" if filter_str is None else filter_str
            set_filter = f"start={start}"
            filter_str = set_filter if len(filter_str) == 0 else filter_str + "&" + set_filter
        except Exception as ex:
            raise Exception("Could not convert 'start' query parameters to int.") from ex

    # Load limit int
    if "limit" in request_args:
        try:
            # Get the value and convert to int
            limit = int(request_args["limit"])

            # Set the filter string
            filter_str = "" if filter_str is None else filter_str
            set_filter = f"limit={limit}"
            filter_str = set_filter if len(filter_str) == 0 else filter_str + "&" + set_filter
        except Exception as ex:
            raise Exception("Could not convert 'limit' query parameters to int.") from ex

    return start, limit, filter_str
