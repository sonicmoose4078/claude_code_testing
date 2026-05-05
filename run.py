#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request


ENDPOINT = "https://micha-mmxlnyzz-swedencentral.services.ai.azure.com/anthropic/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
KEY = "FEmj3JEdJX7r2miFGO7E6O3NSajEBYUy6ATimqYj295JrscIAo8sJQQJ99CCACfhMk5XJ3w3AAAAACOGjbTB"
MODEL = "claude-opus-4-7"
MODEL2 = "claude-opus-4-6"
MODEL3 = "claude-haiku-4-6"
MODEL4 = "claude-haiku-4-5"
MESSAGE="HI CLADU!"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a message to an Azure-hosted Anthropic Messages API endpoint."
    )
    parser.add_argument(
        "--api-key",
        default="FEmj3JEdJX7r2miFGO7E6O3NSajEBYUy6ATimqYj295JrscIAo8sJQQJ99CCACfhMk5XJ3w3AAAAACOGjbTB",
        help="Azure API key. Defaults to the AZURE_API_KEY environment variable.",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="Your Azure Foundry deployment name, for example claude-sonnet-4-6.",
    )
    parser.add_argument(
        "--message",
        default=DEFAULT_MESSAGE,
        help="The user message to send. Defaults to a 5000-word story prompt.",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=50000,
        help="Maximum number of tokens to generate per request.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.7,
        help="Sampling temperature.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Request timeout in seconds.",
    )
    parser.add_argument(
        "--target-words",
        type=int,
        default=5000,
        help="Continue requesting more text until at least this many words are produced. Set 0 to disable.",
    )
    parser.add_argument(
        "--max-continuations",
        type=int,
        default=100,
        help="Maximum number of continuation requests after the initial call.",
    )
    parser.add_argument(
        "--continuation-prompt",
        default=DEFAULT_CONTINUATION_PROMPT,
        help="Prompt used when the script asks the model to continue writing.",
    )
    parser.add_argument(
        "--no-stream",
        action="store_true",
        help="Disable incremental CLI streaming and wait for each request to finish.",
    )
    parser.add_argument(
        "--no-auto-continue",
        action="store_true",
        help="Disable automatic continuation requests when the model stops early.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print the full JSON response for a single non-streaming request.",
    )
    args = parser.parse_args()

    if args.max_tokens <= 0:
        parser.error("--max-tokens must be greater than 0.")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0.")
    if args.target_words < 0:
        parser.error("--target-words cannot be negative.")
    if args.max_continuations < 0:
        parser.error("--max-continuations cannot be negative.")

    return args


def extract_text(response_json: dict) -> str:
    content_blocks = response_json.get("content", [])
    text_parts = [
        block.get("text", "")
        for block in content_blocks
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    return "".join(part for part in text_parts if part)


def count_words(text: str) -> int:
    return len(re.findall(r"\S+", text))


def build_payload(
    model: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    temperature: float,
    stream: bool,
) -> dict:
    return {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "stream": stream,
    }


def build_request(payload: dict, api_key: str) -> urllib.request.Request:
    return urllib.request.Request(
        ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": APIKEY,
            "anthropic-version": ANTHROPIC_VERSION,
        },
        method="POST",
    )


def send_non_stream_request(
    request: urllib.request.Request,
    timeout: int,
) -> dict:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP {exc.code} {exc.reason}", file=sys.stderr)
        print(error_body, file=sys.stderr)
        raise
    except urllib.error.URLError as exc:
        print(f"Request failed: {exc.reason}", file=sys.stderr)
        raise

    return json.loads(body)


def iter_sse_events(response):
    event_name = None
    data_lines = []

    for raw_line in response:
        line = raw_line.decode("utf-8")
        stripped = line.rstrip("\r\n")

        if not stripped:
            if data_lines:
                yield event_name, "\n".join(data_lines)
            event_name = None
            data_lines = []
            continue

        if stripped.startswith(":"):
            continue

        field, separator, value = stripped.partition(":")
        if not separator:
            continue

        if value.startswith(" "):
            value = value[1:]

        if field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)

    if data_lines:
        yield event_name, "\n".join(data_lines)


def ensure_content_block(blocks: list[dict | None], index: int) -> dict:
    while len(blocks) <= index:
        blocks.append(None)
    if blocks[index] is None:
        blocks[index] = {}
    return blocks[index]


def parse_stream_response(response) -> dict:
    blocks: list[dict | None] = []
    result = {
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {},
    }
    saw_message_stop = False

    for event_name, data in iter_sse_events(response):
        if not data or data == "[DONE]":
            continue

        event = json.loads(data)
        event_type = event.get("type") or event_name

        if event_type == "message_start":
            message = event.get("message", {})
            result["id"] = message.get("id")
            result["model"] = message.get("model")
            result["role"] = message.get("role")
            result["usage"] = message.get("usage", {})
            continue

        if event_type == "content_block_start":
            index = event.get("index", 0)
            block = event.get("content_block", {})
            current = ensure_content_block(blocks, index)
            current.clear()
            current.update(block)
            if current.get("type") == "text":
                current["text"] = current.get("text", "")
                if current["text"]:
                    print(current["text"], end="", flush=True)
            continue

        if event_type == "content_block_delta":
            index = event.get("index", 0)
            current = ensure_content_block(blocks, index)
            delta = event.get("delta", {})
            delta_type = delta.get("type")

            if delta_type == "text_delta":
                text = delta.get("text", "")
                current.setdefault("type", "text")
                current["text"] = current.get("text", "") + text
                if text:
                    print(text, end="", flush=True)
            elif delta_type == "input_json_delta":
                current["partial_json"] = current.get("partial_json", "") + delta.get(
                    "partial_json", ""
                )
            elif delta_type == "thinking_delta":
                current["thinking"] = current.get("thinking", "") + delta.get(
                    "thinking", ""
                )
            elif delta_type == "signature_delta":
                current["signature"] = delta.get("signature")
            continue

        if event_type == "message_delta":
            delta = event.get("delta", {})
            if "stop_reason" in delta:
                result["stop_reason"] = delta.get("stop_reason")
            if "stop_sequence" in delta:
                result["stop_sequence"] = delta.get("stop_sequence")
            if "usage" in event:
                result["usage"] = event.get("usage", {})
            continue

        if event_type == "error":
            error = event.get("error", {})
            error_type = error.get("type", "error")
            error_message = error.get("message", "Unknown streaming error")
            raise RuntimeError(f"{error_type}: {error_message}")

        if event_type == "message_stop":
            saw_message_stop = True

    result["content"] = [block for block in blocks if block]

    if not saw_message_stop:
        raise RuntimeError("The stream ended before a message_stop event was received.")

    return result


def send_stream_request(
    request: urllib.request.Request,
    timeout: int,
) -> dict:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return parse_stream_response(response)
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP {exc.code} {exc.reason}", file=sys.stderr)
        print(error_body, file=sys.stderr)
        raise
    except urllib.error.URLError as exc:
        print(f"Request failed: {exc.reason}", file=sys.stderr)
        raise


def build_continuation_message(args: argparse.Namespace, word_count: int) -> str:
    continuation = args.continuation_prompt.strip()
    if args.target_words > 0:
        continuation += (
            f" The story is about {word_count} words so far. Keep writing until it "
            f"reaches at least {args.target_words} words and ends cleanly."
        )
    return continuation


def should_continue(
    stop_reason: str | None,
    full_text: str,
    args: argparse.Namespace,
    auto_continue: bool,
) -> bool:
    if not auto_continue:
        return False

    if stop_reason == "max_tokens":
        return True

    if args.target_words > 0 and count_words(full_text) < args.target_words:
        return True

    return False


def print_completion_status(attempt_number: int, stop_reason: str | None, words: int) -> None:
    label = stop_reason or "unknown"
    print(
        f"\n[continuing with request {attempt_number}; stop_reason={label}; words={words}]",
        file=sys.stderr,
    )


def main() -> int:
    args = parse_args()

    stream = not args.no_stream and not args.raw
    auto_continue = not args.no_auto_continue and not args.raw
    messages = [{"role": "user", "content": args.message}]
    full_text = ""

    for attempt in range(args.max_continuations + 1):
        payload = build_payload(
            model=MODEL,
            messages=messages,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            stream=stream,
        )
        request = build_request(payload, args.api_key)

        try:
            if stream:
                response_json = send_stream_request(request, args.timeout)
            else:
                response_json = send_non_stream_request(request, args.timeout)
        except (RuntimeError, urllib.error.HTTPError, urllib.error.URLError):
            return 1

        if args.raw:
            print(json.dumps(response_json, indent=2))
            return 0

        text = extract_text(response_json)
        if text and not stream:
            print(text, end="", flush=True)

        full_text += text
        stop_reason = response_json.get("stop_reason")

        if not should_continue(stop_reason, full_text, args, auto_continue):
            if full_text and not full_text.endswith("\n"):
                print()
            return 0

        if attempt == args.max_continuations:
            print(
                "\nReached the continuation limit before the response looked complete.",
                file=sys.stderr,
            )
            if full_text and not full_text.endswith("\n"):
                print()
            return 1

        words = count_words(full_text)
        print_completion_status(attempt + 2, stop_reason, words)
        messages.append({"role": "assistant", "content": text})
        messages.append(
            {
                "role": "user",
                "content": build_continuation_message(args, words),
            }
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
message.txt
14 KB
