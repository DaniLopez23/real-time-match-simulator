"""LangGraph tool definitions for querying metrics and retrieving RAG context.

This module will expose callable tools for graph agents to combine structured
match analytics with unstructured document context.
"""

# from langchain_core.tools import tool


def query_match_metrics(query: str) -> str:
    """Tool 1: Query computed match metrics using a natural-language prompt.

    Args:
        query: Natural-language query about match/team/player metrics.

    Returns:
        Text response containing metric insights.
    """
    # TODO: Implement parser and lookup logic against computed analytics outputs.
    pass


def retrieve_document_context(query: str) -> str:
    """Tool 2: Retrieve contextual snippets from indexed project documents.

    Args:
        query: Natural-language query requiring supporting document context.

    Returns:
        Retrieved context string for use in graph reasoning.
    """
    # TODO: Implement RAG retrieval call and response formatting.
    pass
