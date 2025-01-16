"""Custom Subjects API calls."""

from taegis_sdk_python import GraphQLService
from typing import Dict, Any


def lookup_federated_subject(service: GraphQLService) -> Dict[str, Any]:
    """Federated response for expanding the Subjects API.

    Parameters
    ----------
    service : GraphQLService
        Taegis SDK for Python GraphQLService object.

    Returns
    -------
    Dict[str, Any]
        _description_
    """

    output = service.output

    # waiting Union support in GraphQLService
    if not output:
        output = """
            id
            identity {
                __typename
            }
        """

    results = service.subjects.execute_query(endpoint="currentSubject", output=output)
    return results.get("currentSubject", {})
