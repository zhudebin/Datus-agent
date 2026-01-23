# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

# -*- coding: utf-8 -*-
"""Interaction broker for async user interaction flow control."""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Tuple

from datus.schemas.action_history import ActionHistory, ActionRole, ActionStatus
from datus.utils.loggings import get_logger

logger = get_logger(__name__)


@dataclass
class PendingInteraction:
    """Pending interaction waiting for user response"""

    action_id: str
    future: asyncio.Future
    choices: List[str]
    created_at: datetime = field(default_factory=datetime.now)


class InteractionCancelled(Exception):
    """Raised when interaction is cancelled."""

    pass


class InteractionBroker:
    """
    Per-node broker for async user interactions.

    Provides:
    - request(): Async method for hooks to request user input (blocks until response),
                 returns (choice, callback) where callback generates SUCCESS action
    - fetch(): AsyncGenerator for node to consume interaction ActionHistory objects
    - submit(): For UI to submit responses
    - close()/reset(): Lifecycle management

    Usage in hooks:
        choice, callback = await broker.request(
            content="## Generated YAML\\n```yaml\\n...\\n```\\n\\nSync to Knowledge Base?",
            choices=["Yes - Save to KB", "No - Keep file only"],
            content_type="markdown",
            context={"file_path": "/path/to/file.yaml"}
        )
        if choice.startswith("Yes"):
            await sync_to_storage(...)
            await callback("**Successfully synced to Knowledge Base**")
        else:
            await callback("File saved locally only")

    Usage in node (merging with execute_stream):
        async for action in merge_interaction_stream(node.execute_stream(), broker):
            yield action

    Usage in UI:
        # CLI - distinguish by status (PROCESSING = waiting for input, SUCCESS = show result)
        for action in merged_stream:
            if action.role == ActionRole.INTERACTION and action.action_type == "request_choice":
                if action.status == ActionStatus.PROCESSING:
                    choice = display_and_get_user_choice(action)
                    broker.submit(action.action_id, choice)
                elif action.status == ActionStatus.SUCCESS:
                    display_success_content(action)
    """

    def __init__(self):
        """
        Initialize a new InteractionBroker instance and its internal state.
        
        Initializes:
        - _pending: mapping of active action_id to PendingInteraction awaiting responses.
        - _output_queue: queue for emitting ActionHistory objects related to interactions.
        - _closed: broker lifecycle flag; True when the broker is closed.
        - _lock: asyncio lock protecting access to the _pending mapping.
        """
        self._pending: Dict[str, PendingInteraction] = {}
        self._output_queue: asyncio.Queue[ActionHistory] = asyncio.Queue()
        self._closed: bool = False
        self._lock: asyncio.Lock = asyncio.Lock()

    async def request(
        self,
        content: str,
        choices: List[str],
        default_choice: int = 0,
        content_type: str = "markdown",
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, Callable[[str, str, Optional[Dict[str, Any]]], Awaitable[None]]]:
        """
        Request a choice from the user and await their selection.
        
        Parameters:
            content (str): Prompt or message shown to the user (supports markdown).
            choices (List[str]): Available choice strings presented to the user.
            default_choice (int): Index of the default choice to suggest to the user.
            content_type (str): Content type hint (e.g., "text", "yaml", "sql", "markdown").
            context (Optional[Dict[str, Any]]): Optional metadata for UI handling.
        
        Returns:
            Tuple[str, Callable[[str, str, Optional[Dict[str, Any]]], Awaitable[None]]]:
                - The selected choice string.
                - An async `success_callback(callback_content, callback_content_type="markdown", callback_context=None)` function that emits a linked SUCCESS interaction action containing the provided content, content type, context, and the user's choice.
        
        Raises:
            InteractionCancelled: If the broker is closed or the waiting request is cancelled before a response is received.
        """
        if self._closed:
            raise InteractionCancelled("Broker is closed")

        action_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        # Create pending interaction
        pending = PendingInteraction(
            action_id=action_id,
            future=future,
            choices=choices,
        )

        async with self._lock:
            self._pending[action_id] = pending

        # Create ActionHistory with INTERACTION role
        action = ActionHistory(
            action_id=action_id,
            role=ActionRole.INTERACTION,
            status=ActionStatus.PROCESSING,
            action_type="request_choice",
            messages=content,
            input={
                "content": content,
                "content_type": content_type,
                "choices": choices,
                "default_choice": default_choice,
                "context": context or {},
            },
            output=None,
        )

        await self._output_queue.put(action)
        logger.debug(f"InteractionBroker: request queued with action_id={action_id}")

        # Wait for user response
        try:
            result = await future
            logger.debug(f"InteractionBroker: received response for action_id={action_id}: {result}")

            # Create callback for generating SUCCESS action
            async def success_callback(
                callback_content: str,
                callback_content_type: str = "markdown",
                callback_context: Optional[Dict[str, Any]] = None,
            ) -> None:
                """
                Emit a SUCCESS interaction action linked to the original request and queue it for downstream consumption.
                
                If the broker is closed this is a no-op. Otherwise, the callback creates an interaction ActionHistory with the original request's action_id, input (content, content_type, choices, default_choice, and context), and output containing the provided `callback_content`, `callback_content_type`, `callback_context`, and the resolved user choice.
                
                Parameters:
                    callback_content (str): Content to include in the success action's messages and output.
                    callback_content_type (str): MIME-like identifier for `callback_content` (default "markdown").
                    callback_context (Optional[Dict[str, Any]]): Additional context to attach to the output (defaults to empty dict).
                
                Returns:
                    None
                """
                if self._closed:
                    return

                # Use same action_id and action_type, but status=SUCCESS to indicate completion
                success_action = ActionHistory(
                    action_id=action_id,  # Same action_id to link with the original request
                    role=ActionRole.INTERACTION,
                    status=ActionStatus.SUCCESS,  # SUCCESS indicates completion
                    action_type="request_choice",  # Same action_type, UI distinguishes by status
                    messages=callback_content,
                    input={
                        "content": content,  # Original request content
                        "content_type": content_type,
                        "choices": choices,
                        "default_choice": default_choice,
                        "context": context or {},
                    },
                    output={
                        "content": callback_content,
                        "content_type": callback_content_type,
                        "context": callback_context or {},
                        "user_choice": result,
                    },
                )

                await self._output_queue.put(success_action)
                logger.debug(f"InteractionBroker: success callback queued for action_id={action_id}")

            return result, success_callback
        except asyncio.CancelledError:
            async with self._lock:
                self._pending.pop(action_id, None)
            raise InteractionCancelled("Request cancelled")

    async def fetch(self) -> AsyncGenerator[ActionHistory, None]:
        """
        Yield interaction-related ActionHistory items emitted by the broker.
        
        Stops iteration when the broker is closed.
        
        Yields:
            ActionHistory: Interaction actions (request_choice and corresponding success entries) produced by the broker.
        """
        while not self._closed:
            try:
                # Use wait_for with timeout to allow checking closed state
                action = await asyncio.wait_for(self._output_queue.get(), timeout=0.1)
                yield action
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    def submit(self, action_id: str, user_choice: str) -> bool:
        """
        Deliver a user's choice for a pending interaction identified by action_id.
        
        Parameters:
            action_id (str): The identifier of the INTERACTION ActionHistory to respond to.
            user_choice (str): The user's selected choice string to deliver to the pending interaction.
        
        Returns:
            bool: `True` if a pending interaction with `action_id` was found and resolved, `False` otherwise.
        """
        if action_id not in self._pending:
            logger.warning(f"InteractionBroker: submit called with unknown action_id={action_id}")
            return False

        pending = self._pending.pop(action_id)

        # Resolve the future with the user's choice
        if not pending.future.done():
            pending.future.set_result(user_choice)
            logger.debug(f"InteractionBroker: submitted response for action_id={action_id}")

        return True

    def close(self) -> None:
        """
        Mark the broker as closed and cancel all unresolved pending interactions.
        
        This sets the broker to a closed state, cancels any pending interaction futures that are not completed, clears the pending interaction registry, and emits a debug log.
        """
        self._closed = True

        # Cancel all pending futures
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.cancel()

        self._pending.clear()
        logger.debug("InteractionBroker: closed")

    def reset(self) -> None:
        """
        Reset the broker to its initial reusable state.
        
        This closes the broker (cancelling and clearing any pending interactions), re-opens it for reuse, and drains the internal output queue.
        """
        self.close()
        self._closed = False
        # Clear the queue
        while not self._output_queue.empty():
            try:
                self._output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        logger.debug("InteractionBroker: reset")

    @property
    def has_pending(self) -> bool:
        """
        Return whether there are any pending interactions awaiting a response.
        
        Returns:
            True if there is at least one pending interaction, False otherwise.
        """
        return len(self._pending) > 0

    @property
    def is_closed(self) -> bool:
        """
        Report whether the broker is closed.
        
        Returns:
            bool: `true` if the broker is closed, `false` otherwise.
        """
        return self._closed


async def merge_interaction_stream(
    execute_stream: AsyncGenerator[ActionHistory, None],
    broker: InteractionBroker,
) -> AsyncGenerator[ActionHistory, None]:
    """
    Merge execute_stream output with interaction broker output.

    This allows the UI to receive both:
    1. Normal execution actions (TOOL, ASSISTANT, etc.)
    2. Interaction actions (INTERACTION role): request_choice and success

    Args:
        execute_stream: The node's execute_stream() generator
        broker: The InteractionBroker instance for this node

    Yields:
        ActionHistory objects from both streams, interleaved
    """
    execute_iter = execute_stream.__aiter__()
    fetch_iter = broker.fetch().__aiter__()

    execute_exhausted = False
    execute_task: Optional[asyncio.Task] = None
    fetch_task: Optional[asyncio.Task] = None

    _EXHAUSTED = object()  # Sentinel for exhausted iterator

    async def safe_anext(iterable, sentinel):
        """
        Retrieve the next item from an asynchronous iterator, returning the provided sentinel when the iterator is exhausted.
        
        Parameters:
            iterable (AsyncIterator): The asynchronous iterator to advance.
            sentinel: Value to return if the iterator is exhausted.
        
        Returns:
            The next item from `iterable`, or `sentinel` if the iterator is exhausted.
        """
        try:
            return await iterable.__anext__()
        except StopAsyncIteration:
            return sentinel

    try:
        while not execute_exhausted or broker.has_pending or not broker._output_queue.empty():
            tasks_to_wait = []

            # Create execute task if not exhausted and no pending task
            if not execute_exhausted and execute_task is None:
                execute_task = asyncio.create_task(safe_anext(execute_iter, _EXHAUSTED), name="execute")
            if execute_task and not execute_task.done():
                tasks_to_wait.append(execute_task)

            # Create fetch task if no pending task
            if fetch_task is None:
                fetch_task = asyncio.create_task(safe_anext(fetch_iter, _EXHAUSTED), name="fetch")
            if fetch_task and not fetch_task.done():
                tasks_to_wait.append(fetch_task)

            if not tasks_to_wait:
                # Both exhausted and no pending
                break

            # Wait for first completed task
            done, _ = await asyncio.wait(tasks_to_wait, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                result = task.result()

                if task.get_name() == "execute":
                    execute_task = None
                    if result is _EXHAUSTED:
                        execute_exhausted = True
                        logger.debug("merge_interaction_stream: execute_stream exhausted")
                    else:
                        yield result

                elif task.get_name() == "fetch":
                    fetch_task = None
                    if result is not _EXHAUSTED:
                        yield result

    finally:
        # Clean up any remaining tasks
        for task in [execute_task, fetch_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Close the broker when done
        broker.close()