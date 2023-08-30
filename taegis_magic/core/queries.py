"""Taegis Queries API."""

from typing import List, Dict, Any
import requests
from taegis_sdk_python import GraphQLService


def create_query(service: GraphQLService, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Submit a query to the Taegis UI.

    Parameters
    ----------
    service : GraphQLService
        GraphQL Service object
    data : Dict[str, any]
        Data to send to API

    Returns
    -------
    Dict[str, Any]
        API response
    """
    response = requests.post(
        f"{service.core.sync_url}/queries/v1/queries",
        json=data,
        timeout=30,
        headers=service.headers,
    )

    return response.json()


def update_query(
    service: GraphQLService, query_id: str, data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Submit a query to the Taegis UI.

    Parameters
    ----------
    service : GraphQLService
        GraphQL Service object
    query_id : str
        Query Identifier
    data : Dict[str, any]
        Data to send to API

    Returns
    -------
    Dict[str, Any]
        API response
    """
    response = requests.put(
        f"{service.core.sync_url}/queries/v1/queries/{query_id}",
        json=data,
        timeout=30,
        headers=service.headers,
    )

    return response.json()


def get_query(service: GraphQLService, query_id: str) -> Dict[str, Any]:
    """Get a search query by id.

    Parameters
    ----------
    service : GraphQLService
        GraphQL Service object
    query_id : str
        Query Identifier

    Returns
    -------
    Dict[str, Any]
        API response
    """
    response = requests.get(
        f"{service.core.sync_url}/queries/v1/queries/{query_id}",
        timeout=30,
        headers=service.headers,
    )

    return response.json()


def list_query(service: GraphQLService) -> List[Dict[str, Any]]:
    """Get all search queries.

    Parameters
    ----------
    service : GraphQLService
        GraphQL Service object
    query_id : str
        Query Identifier

    Returns
    -------
    Dict[str, Any]
        API response
    """
    response = requests.get(
        f"{service.core.sync_url}/queries/v1/queries",
        timeout=30,
        headers=service.headers,
    )

    return response.json()
