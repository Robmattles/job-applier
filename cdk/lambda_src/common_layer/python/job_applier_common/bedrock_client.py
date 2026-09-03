"""Thin Bedrock invoke wrapper shared by every LLM-calling Lambda.

Requests strict JSON output and parses it defensively (models sometimes
wrap JSON in a markdown code fence despite being told not to)."""
import json
import os

import boto3

_client = None


def _get_client():
    global _client
    if _client is None:
        _client = boto3.client("bedrock-runtime")
    return _client


def invoke_json(model_id: str, system_prompt: str, user_prompt: str, max_tokens: int = 1500,
                cached_prefix: str = None) -> dict:
    """`cached_prefix` is stable text — the accomplishment inventory, say —
    placed ahead of `user_prompt` and marked with `cache_control`, so a run
    that makes many calls sharing it pays full price once and ~10% on every
    call after.

    Caching is prefix-based: everything from the start of the prompt through
    the marked block is cached, and any byte change before that point
    invalidates it. So the stable text has to come FIRST and the volatile
    text (the specific posting) after — which is why fit_scoring's prompt
    had to be reordered to use this. Measured 2026-09-03: the inventory is
    78% of every scoring prompt, and scoring input was the largest single
    line on the bill ($28.67 of $83.11).

    Usage is logged rather than assumed: `cache_read_input_tokens` is the
    only real evidence caching is working, and a silent invalidator (a
    timestamp, a reordered dict) would otherwise look identical to success
    while costing full price."""
    client = _get_client()
    if cached_prefix:
        content = [
            {"type": "text", "text": cached_prefix,
             "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": user_prompt},
        ]
    else:
        content = user_prompt
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": content}],
    }
    resp = client.invoke_model(modelId=model_id, body=json.dumps(body))
    payload = json.loads(resp["body"].read())
    if cached_prefix:
        u = payload.get("usage", {})
        print(f"bedrock usage: in={u.get('input_tokens')} out={u.get('output_tokens')} "
              f"cache_write={u.get('cache_creation_input_tokens')} "
              f"cache_read={u.get('cache_read_input_tokens')}")
    text = "".join(
        block.get("text", "")
        for block in payload.get("content", [])
        if block.get("type") == "text"
    )
    return _extract_json(text)


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"No JSON object found in model response: {text[:500]!r}")
    return json.loads(text[start : end + 1])
