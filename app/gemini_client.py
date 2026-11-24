"""Compatibility alias re-exporting MultiProviderLLMRouter and get_client."""

from llm_router import (
    MultiProviderLLMRouter,
    UpstreamHttpError,
    build_default_model_chain,
    get_client,
    parse_provider_model,
)

__all__ = [
    "MultiProviderLLMRouter",
    "UpstreamHttpError",
    "build_default_model_chain",
    "get_client",
    "parse_provider_model",
]
