"""
AI Prompt Generation Service - Handles AI-powered trading prompt generation

Supports Function Calling for AI to query variables reference, validate prompts, and preview.
Aligned with ai_program_service.py architecture for consistency.
"""
import json
import logging
import os
import random
import re
import time
from typing import Dict, List, Optional, Any, Generator

import requests
from sqlalchemy.orm import Session

from database.models import Account, AiPromptConversation, AiPromptMessage
from services.ai_decision_service import (
    build_chat_completion_endpoints,
    detect_api_format,
    _extract_text_from_message,
    get_max_tokens
)

logger = logging.getLogger(__name__)

# Retry configuration for API calls
API_MAX_RETRIES = 5
API_BASE_DELAY = 1.0  # seconds
API_MAX_DELAY = 16.0  # seconds
RETRYABLE_STATUS_CODES = {502, 503, 504, 429}


def _should_retry_api(status_code: Optional[int], error: Optional[str]) -> bool:
    """Check if API error is retryable."""
    if status_code and status_code in RETRYABLE_STATUS_CODES:
        return True
    if error and any(x in error.lower() for x in ['timeout', 'connection', 'reset', 'eof']):
        return True
    return False


def _get_retry_delay(attempt: int) -> float:
    """Calculate retry delay with exponential backoff and jitter."""
    delay = min(API_BASE_DELAY * (2 ** attempt), API_MAX_DELAY)
    jitter = random.uniform(0, delay * 0.1)
    return delay + jitter


# Path to system prompt file
SYSTEM_PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "prompt_generation_system_prompt.md"
)

# Path to variables reference file
VARIABLES_REFERENCE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "PROMPT_VARIABLES_REFERENCE.md"
)


def load_system_prompt() -> str:
    """Load the system prompt from markdown file"""
    try:
        with open(SYSTEM_PROMPT_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to load system prompt: {e}")
        return "You are a trading strategy prompt generation assistant."


def load_variables_reference() -> str:
    """Load the variables reference document"""
    try:
        with open(VARIABLES_REFERENCE_PATH, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to load variables reference: {e}")
        return "Variables reference not available."


def extract_prompt_from_response(content: str) -> Optional[str]:
    """Extract prompt content from ```prompt code block"""
    pattern = r'```prompt\s*\n(.*?)\n```'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        return match.group(1).strip()
    return None


# ============================================================================
# Tool Definitions (OpenAI format)
# ============================================================================

PROMPT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_variables_reference",
            "description": "Get the complete list of available variables that can be used in trading prompts. Returns documentation for market data, K-line, technical indicators, flow indicators, position/account variables, etc.",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "validate_variables",
            "description": "Validate that all variables used in a prompt text are valid and available. Returns list of valid and invalid variables found.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt_text": {
                        "type": "string",
                        "description": "The prompt text to validate for variable usage"
                    }
                },
                "required": ["prompt_text"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "preview_prompt",
            "description": "Preview the prompt with real market data to verify variables work correctly. Returns rendered text and variable status. Note: Account-related variables (equity, positions, trades, trigger_context) will show placeholder values since the prompt is not yet bound to an AI Trader. Market data and technical indicators will show real values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt_text": {
                        "type": "string",
                        "description": "The prompt text to preview"
                    },
                    "asset": {
                        "type": "string",
                        "description": "Primary asset symbol for market data (default: BTC)",
                        "default": "BTC"
                    }
                },
                "required": ["prompt_text"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "suggest_apply_prompt",
            "description": "Suggest applying the generated prompt to a specific AI Trader. Call this when you have a complete, validated prompt ready for the user to apply.",
            "parameters": {
                "type": "object",
                "properties": {
                    "prompt_text": {
                        "type": "string",
                        "description": "The complete prompt text to apply"
                    },
                    "summary": {
                        "type": "string",
                        "description": "Brief summary of what this prompt does (1-2 sentences)"
                    }
                },
                "required": ["prompt_text", "summary"]
            }
        }
    }
]


def _convert_tools_to_anthropic(openai_tools: List[Dict]) -> List[Dict]:
    """Convert OpenAI format tools to Anthropic format."""
    anthropic_tools = []
    for tool in openai_tools:
        if tool.get("type") == "function":
            func = tool["function"]
            anthropic_tools.append({
                "name": func["name"],
                "description": func.get("description", ""),
                "input_schema": func.get("parameters", {"type": "object", "properties": {}})
            })
    return anthropic_tools


def _convert_messages_to_anthropic(openai_messages: List[Dict]) -> tuple:
    """Convert OpenAI format messages to Anthropic format.

    Returns: (system_prompt, anthropic_messages)
    """
    system_prompt = ""
    anthropic_messages = []
    pending_tool_results = []

    def flush_tool_results():
        nonlocal pending_tool_results
        if pending_tool_results:
            anthropic_messages.append({
                "role": "user",
                "content": pending_tool_results
            })
            pending_tool_results = []

    def clean_tool_use_blocks(blocks):
        if not isinstance(blocks, list):
            return blocks
        cleaned = []
        for block in blocks:
            if isinstance(block, dict):
                block_copy = block.copy()
                if block_copy.get("type") == "tool_use" and block_copy.get("input") == "":
                    block_copy["input"] = {}
                cleaned.append(block_copy)
            else:
                cleaned.append(block)
        return cleaned

    for msg in openai_messages:
        role = msg.get("role")
        content = msg.get("content", "")

        if role == "system":
            system_prompt = content
        elif role == "user":
            flush_tool_results()
            anthropic_messages.append({"role": "user", "content": content})
        elif role == "assistant":
            flush_tool_results()
            if "tool_use_blocks" in msg:
                cleaned_blocks = clean_tool_use_blocks(msg["tool_use_blocks"])
                anthropic_messages.append({
                    "role": "assistant",
                    "content": cleaned_blocks
                })
            else:
                anthropic_messages.append({"role": "assistant", "content": content})
        elif role == "tool":
            pending_tool_results.append({
                "type": "tool_result",
                "tool_use_id": msg.get("tool_call_id", ""),
                "content": content
            })

    flush_tool_results()
    return system_prompt, anthropic_messages


# Pre-convert tools for Anthropic format
PROMPT_TOOLS_ANTHROPIC = _convert_tools_to_anthropic(PROMPT_TOOLS)


# ============================================================================
# Valid Variables List (for validation)
# ============================================================================

# Base variable patterns that are always valid
VALID_VARIABLE_PATTERNS = [
    # Account/Position variables
    r"total_equity", r"available_balance", r"margin_usage_percent", r"maintenance_margin",
    r"positions_detail", r"recent_trades_summary", r"open_orders_detail",
    # Context variables
    r"runtime_minutes", r"current_time_utc", r"trading_environment", r"selected_symbols_detail",
    r"trigger_context", r"news_section", r"output_format",
    # Market regime
    r"market_regime_description", r"trigger_market_regime",
    r"market_regime(?:_(?:1m|5m|15m|1h))?",
    # Symbol-specific patterns (BTC, ETH, SOL, etc.)
    r"[A-Z]+_market_data",
    r"[A-Z]+_klines_(?:1m|3m|5m|15m|30m|1h|2h|4h|8h|12h|1d|3d|1w|1M)",
    r"[A-Z]+_market_regime(?:_(?:1m|5m|15m|1h))?",
    # Technical indicators
    r"[A-Z]+_(?:MA|EMA|RSI14|RSI7|MACD|BOLL|ATR14|VWAP|OBV|STOCH)_(?:1m|3m|5m|15m|30m|1h|2h|4h|8h|12h|1d)",
    # Flow indicators
    r"[A-Z]+_(?:CVD|OI|OI_DELTA|TAKER|FUNDING|DEPTH|IMBALANCE)_(?:1m|3m|5m|15m|30m|1h|2h|4h)",
]


def _validate_variable(var_name: str) -> bool:
    """Check if a variable name matches any valid pattern."""
    for pattern in VALID_VARIABLE_PATTERNS:
        if re.fullmatch(pattern, var_name):
            return True
    return False


def _extract_variables_from_text(text: str) -> List[str]:
    """Extract all {variable} placeholders from text."""
    # Match {variable_name} but not {{escaped}}
    pattern = r'\{([^{}]+)\}'
    matches = re.findall(pattern, text)
    # Filter out things that look like JSON or format strings
    variables = []
    for m in matches:
        # Skip if it looks like JSON key or has special chars
        if ':' in m or '"' in m or "'" in m:
            continue
        # Skip if it's a number (like array index)
        if m.isdigit():
            continue
        # Handle klines with count: {BTC_klines_1h}(100) -> BTC_klines_1h
        clean_var = m.split('}')[0].split('(')[0].strip()
        if clean_var:
            variables.append(clean_var)
    return list(set(variables))


# Account-related variables that need AI Trader binding (will show placeholders)
ACCOUNT_VARIABLES = {
    "total_equity", "available_balance", "margin_usage_percent", "maintenance_margin",
    "positions_detail", "recent_trades_summary", "open_orders_detail",
    "runtime_minutes", "trading_environment", "trigger_context", "trigger_market_regime",
}


def _execute_preview_prompt(args: Dict[str, Any], request_id: str) -> str:
    """Execute preview_prompt tool - render prompt with real market data."""
    from services.ai_decision_service import _build_prompt_context, SafeDict
    from services.market_data import get_last_price
    from datetime import datetime, timezone

    prompt_text = args.get("prompt_text", "")
    asset = args.get("asset", "BTC").upper()

    # Extract variables from prompt
    variables = _extract_variables_from_text(prompt_text)

    resolved_vars = []
    placeholder_vars = []
    failed_vars = []

    # Build minimal context for preview (no account data)
    context = {}

    # System variables
    context["current_time_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    context["output_format"] = '{"action": "hold|buy|sell|close", "symbol": "BTC", ...}'
    context["selected_symbols_detail"] = f"Primary: {asset}"
    context["news_section"] = "[News preview not available in prompt generation mode]"
    context["market_regime_description"] = "[Market regime definitions - available at runtime]"

    # Account placeholders
    context["total_equity"] = "[PLACEHOLDER: Will show actual equity when bound to AI Trader]"
    context["available_balance"] = "[PLACEHOLDER: Will show actual balance when bound to AI Trader]"
    context["margin_usage_percent"] = "[PLACEHOLDER: Will show actual margin usage when bound to AI Trader]"
    context["maintenance_margin"] = "[PLACEHOLDER: Will show actual maintenance margin when bound to AI Trader]"
    context["positions_detail"] = "[PLACEHOLDER: Will show actual positions when bound to AI Trader]"
    context["recent_trades_summary"] = "[PLACEHOLDER: Will show actual trades when bound to AI Trader]"
    context["open_orders_detail"] = "[PLACEHOLDER: Will show actual orders when bound to AI Trader]"
    context["runtime_minutes"] = "[PLACEHOLDER: Will show actual runtime when AI Trader is running]"
    context["trading_environment"] = "[PLACEHOLDER: testnet or mainnet when bound to AI Trader]"
    context["trigger_context"] = "[PLACEHOLDER: Will show signal/scheduled trigger info at runtime]"
    context["trigger_market_regime"] = "[PLACEHOLDER: Will show regime snapshot at trigger time]"

    # Try to get real market data
    try:
        from services.hyperliquid_symbol_service import get_available_symbol_map
        from services.sampling_pool import sampling_pool

        symbol_map = get_available_symbol_map()
        symbols_to_fetch = [asset]

        # Check if other symbols are referenced
        for var in variables:
            match = re.match(r'^([A-Z]+)_', var)
            if match:
                sym = match.group(1)
                if sym not in symbols_to_fetch and sym in symbol_map:
                    symbols_to_fetch.append(sym)

        # Fetch market data for each symbol
        for sym in symbols_to_fetch[:5]:  # Limit to 5 symbols
            try:
                price = get_last_price(sym, "CRYPTO", environment="mainnet")
                context[f"{sym}_market_data"] = f"Symbol: {sym}, Price: ${price:,.2f}"

                # Get sampling data if available
                sample = sampling_pool.get(sym)
                if sample:
                    context[f"{sym}_market_data"] = (
                        f"Symbol: {sym}\n"
                        f"Price: ${sample.get('price', price):,.2f}\n"
                        f"24h Change: {sample.get('change_24h', 'N/A')}%\n"
                        f"Volume: ${sample.get('volume_24h', 'N/A'):,.0f}" if isinstance(sample.get('volume_24h'), (int, float)) else f"Volume: {sample.get('volume_24h', 'N/A')}"
                    )
            except Exception as e:
                logger.warning(f"[Preview {request_id}] Failed to get price for {sym}: {e}")
                context[f"{sym}_market_data"] = f"[Failed to fetch {sym} market data: {e}]"
    except Exception as e:
        logger.warning(f"[Preview {request_id}] Market data fetch error: {e}")

    # Build context with indicators using _build_prompt_context
    try:
        # Create minimal account-like object for context building
        class MinimalAccount:
            id = 0
            name = "Preview"

        minimal_portfolio = {'cash': 0, 'positions': {}, 'total_assets': 0}
        prices = {asset: get_last_price(asset, "CRYPTO", environment="mainnet")}

        full_context = _build_prompt_context(
            MinimalAccount(),
            minimal_portfolio,
            prices,
            context.get("news_section", ""),
            None, None, None,
            db=None,
            symbol_metadata={asset: {"name": asset}},
            symbol_order=[asset],
            environment="mainnet",
            template_text=prompt_text,
        )
        # Merge indicator data into context
        for key, value in full_context.items():
            if key not in context or context[key].startswith("[PLACEHOLDER"):
                context[key] = value
    except Exception as e:
        logger.warning(f"[Preview {request_id}] Context building error: {e}")

    # Render the prompt
    try:
        rendered = prompt_text.format_map(SafeDict(context))
    except Exception as e:
        rendered = f"[Render error: {e}]"

    # Categorize variables
    for var in variables:
        if var in ACCOUNT_VARIABLES:
            placeholder_vars.append(var)
        elif f"{{{var}}}" in rendered or f"[PLACEHOLDER" in context.get(var, "") or "N/A" in str(context.get(var, "")):
            # Check if variable was not resolved
            if var not in context or context.get(var, "").startswith("["):
                failed_vars.append(var)
            else:
                placeholder_vars.append(var)
        else:
            resolved_vars.append(var)

    # Build result
    result = {
        "success": True,
        "rendered_preview": rendered[:2000] + ("..." if len(rendered) > 2000 else ""),
        "preview_length": len(rendered),
        "variables_status": {
            "total": len(variables),
            "resolved": len(resolved_vars),
            "placeholder": len(placeholder_vars),
            "failed": len(failed_vars),
            "resolved_list": sorted(resolved_vars)[:20],
            "placeholder_list": sorted(placeholder_vars),
            "failed_list": sorted(failed_vars),
        },
        "note": "Account-related variables show placeholders. Apply to AI Trader for full preview with real account data."
    }

    if failed_vars:
        result["warning"] = f"Found {len(failed_vars)} variable(s) that could not be resolved: {', '.join(failed_vars)}"

    return json.dumps(result, indent=2, ensure_ascii=False)


# ============================================================================
# Tool Execution Functions
# ============================================================================

def execute_tool(tool_name: str, args: Dict[str, Any], request_id: str) -> str:
    """Execute a tool and return the result as a string."""
    logger.info(f"[AI Prompt Gen {request_id}] Executing tool: {tool_name}")

    try:
        if tool_name == "get_variables_reference":
            return load_variables_reference()

        elif tool_name == "validate_variables":
            prompt_text = args.get("prompt_text", "")
            variables = _extract_variables_from_text(prompt_text)

            valid_vars = []
            invalid_vars = []
            for var in variables:
                if _validate_variable(var):
                    valid_vars.append(var)
                else:
                    invalid_vars.append(var)

            result = {
                "total_found": len(variables),
                "valid_count": len(valid_vars),
                "invalid_count": len(invalid_vars),
                "valid_variables": sorted(valid_vars),
                "invalid_variables": sorted(invalid_vars),
            }
            if invalid_vars:
                result["warning"] = f"Found {len(invalid_vars)} invalid variable(s). Please check spelling or refer to variables reference."
            else:
                result["status"] = "All variables are valid."
            return json.dumps(result, indent=2)

        elif tool_name == "preview_prompt":
            return _execute_preview_prompt(args, request_id)

        elif tool_name == "suggest_apply_prompt":
            prompt_text = args.get("prompt_text", "")
            summary = args.get("summary", "")
            # This is a special tool - return structured data for frontend
            return json.dumps({
                "action": "suggest_apply",
                "prompt_text": prompt_text,
                "summary": summary,
                "message": "Prompt is ready to apply. User can click 'Apply' to use this prompt."
            })

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    except Exception as e:
        logger.error(f"[AI Prompt Gen {request_id}] Tool execution error: {e}")
        return json.dumps({"error": str(e)})


# ============================================================================
# Main SSE Generation Function
# ============================================================================

def generate_prompt_with_ai_stream(
    db: Session,
    account: Account,
    user_message: str,
    conversation_id: Optional[int] = None,
    user_id: int = 1,
) -> Generator[str, None, None]:
    """
    Generate trading strategy prompt using AI with SSE streaming.

    Yields SSE events:
    - tool_round: {type: "tool_round", round: N, max: M}
    - tool_call: {type: "tool_call", name: "...", args: {...}}
    - tool_result: {type: "tool_result", name: "...", result: "..."}
    - content: {type: "content", content: "..."}
    - suggest_apply: {type: "suggest_apply", prompt_text: "...", summary: "..."}
    - done: {type: "done", conversation_id: N, message_id: N, prompt_result: "..."}
    - error: {type: "error", content: "..."}
    - retry: {type: "retry", attempt: N, max_retries: M}
    """
    start_time = time.time()
    request_id = f"prompt_gen_{int(start_time)}"

    logger.info(f"[AI Prompt Gen {request_id}] Starting: account={account.name}, "
                f"conversation_id={conversation_id}, user_message_length={len(user_message)}")

    try:
        # Load system prompt
        system_prompt = load_system_prompt()

        # Get or create conversation
        conversation = None
        if conversation_id:
            conversation = db.query(AiPromptConversation).filter(
                AiPromptConversation.id == conversation_id,
                AiPromptConversation.user_id == user_id
            ).first()

        if not conversation:
            title = user_message[:50] + "..." if len(user_message) > 50 else user_message
            conversation = AiPromptConversation(user_id=user_id, title=title)
            db.add(conversation)
            db.flush()
            logger.info(f"[AI Prompt Gen {request_id}] Created conversation: id={conversation.id}")

        # Save user message
        user_msg = AiPromptMessage(
            conversation_id=conversation.id,
            role="user",
            content=user_message
        )
        db.add(user_msg)
        db.flush()

        # Build message history
        messages = [{"role": "system", "content": system_prompt}]

        history = db.query(AiPromptMessage).filter(
            AiPromptMessage.conversation_id == conversation.id,
            AiPromptMessage.id != user_msg.id
        ).order_by(AiPromptMessage.created_at).limit(10).all()

        for msg in history:
            messages.append({"role": msg.role, "content": msg.content})

        messages.append({"role": "user", "content": user_message})

        # Detect API format and build endpoints
        endpoint, api_format = detect_api_format(account.base_url)
        if not endpoint:
            yield f"data: {json.dumps({'type': 'error', 'content': 'Invalid API configuration'})}\n\n"
            return

        if api_format == 'anthropic':
            endpoints = [endpoint]
            headers = {
                "Content-Type": "application/json",
                "x-api-key": account.api_key,
                "anthropic-version": "2023-06-01"
            }
        else:
            endpoints = build_chat_completion_endpoints(account.base_url, account.model)
            if not endpoints:
                yield f"data: {json.dumps({'type': 'error', 'content': 'Invalid API configuration'})}\n\n"
                return
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {account.api_key}"
            }

        # Tool calling loop
        max_rounds = 10
        tool_round = 0
        tool_calls_log = []
        final_content = ""
        reasoning_snapshot = ""
        prompt_result = None
        suggest_apply_data = None

        # Create assistant message upfront with is_complete=False
        assistant_msg = AiPromptMessage(
            conversation_id=conversation.id,
            role="assistant",
            content="",
            is_complete=False
        )
        db.add(assistant_msg)
        db.flush()

        while tool_round < max_rounds:
            tool_round += 1
            is_last = tool_round == max_rounds

            yield f"data: {json.dumps({'type': 'tool_round', 'round': tool_round, 'max': max_rounds})}\n\n"

            # Build payload based on API format
            if api_format == 'anthropic':
                sys_prompt, anthropic_messages = _convert_messages_to_anthropic(messages)
                payload = {
                    "model": account.model,
                    "max_tokens": get_max_tokens(account.model),
                    "system": sys_prompt,
                    "messages": anthropic_messages,
                }
                if not is_last:
                    payload["tools"] = PROMPT_TOOLS_ANTHROPIC
            else:
                payload = {
                    "model": account.model,
                    "messages": messages,
                    "temperature": 0.7,
                    "max_tokens": get_max_tokens(account.model),
                }
                if not is_last:
                    payload["tools"] = PROMPT_TOOLS
                    payload["tool_choice"] = "auto"

            # API call with retry logic
            response = None
            last_error = None
            last_status_code = None

            for retry_attempt in range(API_MAX_RETRIES):
                response = None
                last_error = None

                for ep in endpoints:
                    try:
                        logger.info(f"[AI Prompt Gen {request_id}] Round {tool_round}, trying: {ep}")
                        response = requests.post(ep, json=payload, headers=headers, timeout=120)

                        if response.status_code == 200:
                            break
                        else:
                            last_status_code = response.status_code
                            last_error = f"HTTP {response.status_code}"
                            logger.warning(f"[AI Prompt Gen {request_id}] Endpoint failed: {response.status_code}")

                    except requests.exceptions.Timeout:
                        last_error = "timeout"
                        logger.warning(f"[AI Prompt Gen {request_id}] Timeout on {ep}")
                    except Exception as e:
                        last_error = str(e)
                        logger.warning(f"[AI Prompt Gen {request_id}] Error: {e}")

                if response and response.status_code == 200:
                    break

                if not _should_retry_api(last_status_code, last_error):
                    break

                if retry_attempt < API_MAX_RETRIES - 1:
                    delay = _get_retry_delay(retry_attempt)
                    logger.warning(f"[AI Prompt Gen {request_id}] Retrying in {delay:.1f}s")
                    yield f"data: {json.dumps({'type': 'retry', 'attempt': retry_attempt + 2, 'max_retries': API_MAX_RETRIES})}\n\n"
                    time.sleep(delay)

            if not response or response.status_code != 200:
                error_detail = last_error or "Unknown error"
                if tool_calls_log:
                    assistant_msg.content = final_content
                    assistant_msg.tool_calls_log = json.dumps(tool_calls_log)
                    db.commit()
                    yield f"data: {json.dumps({'type': 'interrupted', 'message_id': assistant_msg.id, 'error': error_detail})}\n\n"
                else:
                    db.delete(assistant_msg)
                    db.commit()
                    yield f"data: {json.dumps({'type': 'error', 'content': f'API request failed: {error_detail}'})}\n\n"
                return

            resp_json = response.json()

            # Parse response based on API format
            if api_format == 'anthropic':
                content_blocks = resp_json.get("content", [])
                tool_uses = []
                content = ""

                for block in content_blocks:
                    if block.get("type") == "text":
                        content += block.get("text", "")
                    elif block.get("type") == "tool_use":
                        tool_uses.append(block)

                if tool_uses:
                    # Process tool calls
                    messages.append({
                        "role": "assistant",
                        "content": content,
                        "tool_use_blocks": content_blocks
                    })

                    for tool_use in tool_uses:
                        tool_name = tool_use.get("name", "")
                        tool_id = tool_use.get("id", "")
                        tool_args = tool_use.get("input", {})

                        yield f"data: {json.dumps({'type': 'tool_call', 'name': tool_name, 'args': tool_args})}\n\n"

                        result = execute_tool(tool_name, tool_args, request_id)
                        tool_calls_log.append({
                            "name": tool_name,
                            "args": tool_args,
                            "result": result[:500] if len(result) > 500 else result
                        })

                        # Check for suggest_apply
                        if tool_name == "suggest_apply_prompt":
                            try:
                                suggest_apply_data = json.loads(result)
                                yield f"data: {json.dumps({'type': 'suggest_apply', 'prompt_text': suggest_apply_data.get('prompt_text', ''), 'summary': suggest_apply_data.get('summary', '')})}\n\n"
                            except:
                                pass

                        yield f"data: {json.dumps({'type': 'tool_result', 'name': tool_name, 'result': result[:200] + '...' if len(result) > 200 else result})}\n\n"

                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "content": result
                        })

                    continue  # Next round

                # No tool calls - final response
                final_content = content
                break

            else:
                # OpenAI format
                choice = resp_json.get("choices", [{}])[0]
                message = choice.get("message", {})
                content = _extract_text_from_message(message.get("content", ""))
                tool_calls = message.get("tool_calls", [])

                if tool_calls:
                    # Process tool calls
                    messages.append({
                        "role": "assistant",
                        "content": content,
                        "tool_calls": tool_calls
                    })

                    for tc in tool_calls:
                        func = tc.get("function", {})
                        tool_name = func.get("name", "")
                        tool_id = tc.get("id", "")
                        try:
                            tool_args = json.loads(func.get("arguments", "{}"))
                        except:
                            tool_args = {}

                        yield f"data: {json.dumps({'type': 'tool_call', 'name': tool_name, 'args': tool_args})}\n\n"

                        result = execute_tool(tool_name, tool_args, request_id)
                        tool_calls_log.append({
                            "name": tool_name,
                            "args": tool_args,
                            "result": result[:500] if len(result) > 500 else result
                        })

                        # Check for suggest_apply
                        if tool_name == "suggest_apply_prompt":
                            try:
                                suggest_apply_data = json.loads(result)
                                yield f"data: {json.dumps({'type': 'suggest_apply', 'prompt_text': suggest_apply_data.get('prompt_text', ''), 'summary': suggest_apply_data.get('summary', '')})}\n\n"
                            except:
                                pass

                        yield f"data: {json.dumps({'type': 'tool_result', 'name': tool_name, 'result': result[:200] + '...' if len(result) > 200 else result})}\n\n"

                        messages.append({
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "content": result
                        })

                    continue  # Next round

                # No tool calls - final response
                final_content = content
                break

        # Extract prompt from final content
        prompt_result = extract_prompt_from_response(final_content)

        # Update and save assistant message
        assistant_msg.content = final_content
        assistant_msg.prompt_result = prompt_result
        assistant_msg.tool_calls_log = json.dumps(tool_calls_log) if tool_calls_log else None
        assistant_msg.reasoning_snapshot = reasoning_snapshot if reasoning_snapshot else None
        assistant_msg.is_complete = True
        db.commit()

        # Send final content and done event
        yield f"data: {json.dumps({'type': 'content', 'content': final_content})}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'conversation_id': conversation.id, 'message_id': assistant_msg.id, 'prompt_result': prompt_result})}\n\n"

        total_elapsed = time.time() - start_time
        logger.info(f"[AI Prompt Gen {request_id}] Completed in {total_elapsed:.2f}s")

    except Exception as e:
        logger.error(f"[AI Prompt Gen {request_id}] Unexpected error: {e}", exc_info=True)
        db.rollback()
        yield f"data: {json.dumps({'type': 'error', 'content': f'Internal error: {type(e).__name__}'})}\n\n"


# ============================================================================
# Legacy Synchronous Function (for backward compatibility)
# ============================================================================

def generate_prompt_with_ai(
    db: Session,
    account: Account,
    user_message: str,
    conversation_id: Optional[int] = None,
    user_id: int = 1,
) -> Dict:
    """
    Legacy synchronous version - wraps the streaming version.
    Kept for backward compatibility with existing code.
    """
    result = {
        "success": False,
        "error": "Unknown error"
    }

    try:
        for event in generate_prompt_with_ai_stream(db, account, user_message, conversation_id, user_id):
            if event.startswith("data: "):
                data = json.loads(event[6:].strip())
                event_type = data.get("type")

                if event_type == "done":
                    result = {
                        "success": True,
                        "conversation_id": data.get("conversation_id"),
                        "message_id": data.get("message_id"),
                        "content": "",  # Will be set from content event
                        "prompt_result": data.get("prompt_result"),
                    }
                elif event_type == "content":
                    result["content"] = data.get("content", "")
                elif event_type == "error":
                    result = {
                        "success": False,
                        "error": data.get("content", "Unknown error")
                    }
    except Exception as e:
        result = {
            "success": False,
            "error": str(e)
        }

    return result


# ============================================================================
# Conversation History Functions
# ============================================================================

def get_conversation_history(
    db: Session,
    user_id: int,
    limit: int = 20
) -> List[Dict]:
    """Get user's conversation history."""
    conversations = db.query(AiPromptConversation).filter(
        AiPromptConversation.user_id == user_id
    ).order_by(
        AiPromptConversation.updated_at.desc()
    ).limit(limit).all()

    result = []
    for conv in conversations:
        msg_count = db.query(AiPromptMessage).filter(
            AiPromptMessage.conversation_id == conv.id
        ).count()

        result.append({
            "id": conv.id,
            "title": conv.title,
            "messageCount": msg_count,
            "createdAt": conv.created_at.isoformat() if conv.created_at else None,
            "updatedAt": conv.updated_at.isoformat() if conv.updated_at else None,
        })

    return result


def get_conversation_messages(
    db: Session,
    conversation_id: int,
    user_id: int
) -> Optional[List[Dict]]:
    """Get all messages in a conversation."""
    conversation = db.query(AiPromptConversation).filter(
        AiPromptConversation.id == conversation_id,
        AiPromptConversation.user_id == user_id
    ).first()

    if not conversation:
        return None

    messages = db.query(AiPromptMessage).filter(
        AiPromptMessage.conversation_id == conversation_id
    ).order_by(AiPromptMessage.created_at).all()

    result = []
    for msg in messages:
        msg_data = {
            "id": msg.id,
            "role": msg.role,
            "content": msg.content,
            "promptResult": msg.prompt_result,
            "createdAt": msg.created_at.isoformat() if msg.created_at else None,
        }
        # Include tool_calls_log if present
        if msg.tool_calls_log:
            try:
                msg_data["toolCallsLog"] = json.loads(msg.tool_calls_log)
            except:
                pass
        result.append(msg_data)

    return result
