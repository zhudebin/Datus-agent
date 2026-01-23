# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Plan mode hooks implementation for intercepting agent execution flow."""

import time

from agents import SQLiteSession
from agents.lifecycle import AgentHooks

from datus.cli.execution_state import InteractionBroker, InteractionCancelled
from datus.utils.loggings import get_logger

logger = get_logger(__name__)


class PlanningPhaseException(Exception):
    """Exception raised when trying to execute tools during planning phase."""


class UserCancelledException(Exception):
    """Exception raised when user explicitly cancels execution"""


class PlanModeHooks(AgentHooks):
    """Plan Mode hooks for workflow management"""

    def __init__(self, broker: InteractionBroker, session: SQLiteSession, auto_mode: bool = False):
        """
        Initialize the PlanModeHooks instance and prepare plan-mode state.
        
        Parameters:
            broker (InteractionBroker): Broker used for user and LLM interactions.
            session (SQLiteSession): SQLite session used to persist and load plan/todo data.
            auto_mode (bool): If True, start with execution mode set to "auto"; otherwise start in "manual".
        
        Notes:
            Sets up internal storage and initial plan-mode state including `todo_storage`, `plan_phase`,
            `execution_mode`, `replan_feedback`, `_state_transitions`, and `_plan_generated_pending`.
        """
        self.broker = broker
        self.session = session
        self.auto_mode = auto_mode
        from datus.tools.func_tool.plan_tools import SessionTodoStorage

        self.todo_storage = SessionTodoStorage(session)
        self.plan_phase = "generating"
        self.execution_mode = "auto" if auto_mode else "manual"
        self.replan_feedback = ""
        self._state_transitions = []
        self._plan_generated_pending = False  # Flag to defer plan display until LLM ends

    async def on_start(self, context, agent) -> None:
        logger.debug(f"Plan mode start: phase={self.plan_phase}")

    async def on_tool_start(self, context, agent, tool) -> None:
        tool_name = getattr(tool, "name", getattr(tool, "__name__", str(tool)))
        logger.debug(f"Plan mode tool start: {tool_name}, phase: {self.plan_phase}, mode: {self.execution_mode}")

        if tool_name == "todo_update" and self.execution_mode == "manual" and self.plan_phase == "executing":
            # Check if this is updating to pending status
            if self._is_pending_update(context):
                await self._handle_execution_step(tool_name)

    async def on_tool_end(self, context, agent, tool, result) -> None:
        tool_name = getattr(tool, "name", getattr(tool, "__name__", str(tool)))

        if tool_name == "todo_write":
            logger.info("Plan generation completed, will show plan after LLM finishes current turn")
            # Set flag instead of immediately showing plan
            # This allows any remaining "Thinking" messages to be generated first
            self._plan_generated_pending = True

    async def on_llm_end(self, context, agent, response) -> None:
        """Called when LLM finishes a turn - perfect time to show plan after all thinking is done"""
        if self._plan_generated_pending and self.plan_phase == "generating":
            self._plan_generated_pending = False
            await self._on_plan_generated()

    async def on_end(self, context, agent, output) -> None:
        logger.info(f"Plan mode end: phase={self.plan_phase}")

    def _transition_state(self, new_state: str, context: dict = None):
        old_state = self.plan_phase
        self.plan_phase = new_state

        transition_data = {
            "from_state": old_state,
            "to_state": new_state,
            "context": context or {},
            "timestamp": time.time(),
        }

        self._state_transitions.append(transition_data)
        logger.info(f"Plan mode state transition: {old_state} -> {new_state}")
        return transition_data

    async def _on_plan_generated(self):
        """
        Handle the result of plan generation and start either an automatic execution flow or an interactive confirmation.
        
        Clears stored replan feedback, records a transition to the "confirming" phase, and inspects the stored todo list. If no plan was generated, sends an error/acknowledgement prompt via the broker. If a plan exists, presents the plan: in auto mode it transitions to "executing", sets execution_mode to "auto", and notifies the broker; in interactive mode it delegates to _get_user_confirmation to obtain the user's decision.
        
        Raises:
            PlanningPhaseException: propagated when the interactive confirmation flow triggers a replan request.
        """
        todo_list = self.todo_storage.get_todo_list()
        logger.info(f"Plan generation - todo_list: {todo_list.model_dump() if todo_list else None}")

        # Clear replan feedback BEFORE transitioning state to ensure prompt updates correctly
        self.replan_feedback = ""
        self._transition_state("confirming", {"todo_count": len(todo_list.items) if todo_list else 0})

        if not todo_list:
            # No plan generated - need a simple request to show error
            choice, callback = await self.broker.request(
                content="**No plan generated**\n\nPlease try again with a different request.",
                choices=["OK"],
                context={"hook_type": "plan_mode", "phase": "error"},
            )
            await callback("Plan generation failed")
            return

        # Build plan display content (markdown format)
        plan_content = "## Plan Generated Successfully!\n\n"
        plan_content += "### Execution Plan:\n"
        for i, item in enumerate(todo_list.items, 1):
            plan_content += f"{i}. {item.content}\n"

        # Auto mode: skip user confirmation, use a simple request to show plan
        if self.auto_mode:
            self.execution_mode = "auto"
            self._transition_state("executing", {"mode": "auto"})
            choice, callback = await self.broker.request(
                content=f"{plan_content}\n\n**Auto execution mode** (workflow/benchmark context)",
                choices=["Continue"],
                context={"hook_type": "plan_mode", "phase": "auto_confirm"},
            )
            await callback("Auto execution mode started")
            return

        # Interactive mode: ask for user confirmation with plan content
        try:
            await self._get_user_confirmation(plan_content)
        except PlanningPhaseException:
            # Re-raise to be handled by chat_agentic_node.py
            raise

    async def _get_user_confirmation(self, plan_content: str = ""):
        """
        Present the generated plan to the user and obtain confirmation for execution mode.
        
        Displays the optional plan_content and prompts the user (via the broker) to choose one of: manual confirmation, automatic execution, revision, or cancellation. Updates internal execution_mode and plan_phase state based on the user's choice. May trigger a replan flow or cancel execution.
        
        Parameters:
            plan_content (str): Optional Markdown-formatted plan summary to include in the prompt.
        
        Raises:
            PlanningPhaseException: When the user requests a revision (replan) after providing feedback.
            UserCancelledException: When the user cancels or the interaction is cancelled.
        """
        try:
            # Merge plan content into request content
            request_content = ""
            if plan_content:
                request_content = f"{plan_content}\n\n"
            request_content += "### Choose Execution Mode:"

            choice, callback = await self.broker.request(
                content=request_content,
                choices=[
                    "Manual Confirm - Confirm each step",
                    "Auto Execute - Run all steps automatically",
                    "Revise - Provide feedback and regenerate plan",
                    "Cancel",
                ],
                default_choice=0,
                context={"hook_type": "plan_mode", "phase": "confirming"},
            )
            logger.info(f"choice: {choice}")

            if choice.startswith("Manual"):
                self.execution_mode = "manual"
                self._transition_state("executing", {"mode": "manual"})
                await callback("**Manual confirmation mode selected**")
                return
            elif choice.startswith("Auto"):
                self.execution_mode = "auto"
                self._transition_state("executing", {"mode": "auto"})
                await callback("**Auto execution mode selected**")
                return
            elif choice.startswith("Revise"):
                await callback("Revising plan...")
                await self._handle_replan()
                raise PlanningPhaseException(f"REPLAN_REQUIRED: Revise the plan with feedback: {self.replan_feedback}")
            elif choice.startswith("Cancel"):
                self._transition_state("cancelled", {})
                await callback("**Plan cancelled**")
                raise UserCancelledException("User cancelled plan execution")
            else:
                await callback("**Invalid choice, please try again**")
                await self._get_user_confirmation()

        except InteractionCancelled:
            self._transition_state("cancelled", {"reason": "interaction_cancelled"})
            raise UserCancelledException("Plan cancelled")

    async def _handle_replan(self):
        """
        Request replanning feedback from the user and trigger a replan if feedback is provided.
        
        If the user supplies non-empty feedback, sends a callback summarizing any completed plan steps and the feedback, stores the feedback in self.replan_feedback, and transitions the plan phase to "generating" with replan context. If no feedback is provided, sends a "No feedback provided" callback and, when currently in the "confirming" phase, re-prompts the user for plan confirmation. Interaction cancellations are ignored (no callback or state change).
        """
        try:
            # Request text input - merge feedback prompt into request content
            feedback, callback = await self.broker.request(
                content="### Provide feedback for replanning\n\nEnter your feedback:",
                choices=["Submit feedback"],
                context={"hook_type": "plan_mode", "input_mode": "text"},
            )

            if feedback and feedback != "Submit feedback":
                todo_list = self.todo_storage.get_todo_list()
                completed_items = [item for item in todo_list.items if item.status == "completed"] if todo_list else []

                # Build callback content with status info
                callback_content = ""
                if completed_items:
                    callback_content += f"Found {len(completed_items)} completed steps\n\n"
                callback_content += f"**Replanning with feedback:** {feedback}"

                await callback(callback_content)
                self.replan_feedback = feedback
                # Transition back to generating phase for replan
                self._transition_state("generating", {"replan_triggered": True, "feedback": feedback})
            else:
                await callback("**No feedback provided**")
                if self.plan_phase == "confirming":
                    await self._get_user_confirmation()

        except InteractionCancelled:
            pass  # Replan cancelled, no callback needed

    async def _handle_execution_step(self, _tool_name: str):
        """
        Prompt the user (via the InteractionBroker) to confirm or control execution of the next pending plan step.
        
        If auto_mode is enabled, confirmations are skipped. Otherwise this will:
        - Retrieve the current todo list and identify the next pending item.
        - Present a progress view and options to execute the current step, switch to automatic continuation, request a plan revision, or cancel.
        - Update internal execution_mode and plan_phase as appropriate and invoke replan handling when requested.
        
        Parameters:
            _tool_name (str): Name of the tool invocation that triggered this check (used for logging/context).
        
        Raises:
            UserCancelledException: If the user cancels or the interaction is interrupted.
            PlanningPhaseException: If the user requests a plan revision (replan flow is initiated).
        """
        logger.info(f"PlanHooks: _handle_execution_step called with tool: {_tool_name}")

        # Auto mode: skip all step confirmations
        if self.auto_mode:
            logger.info("Auto mode enabled, executing step without confirmation")
            return

        todo_list = self.todo_storage.get_todo_list()
        logger.info(f"PlanHooks: Retrieved todo list with {len(todo_list.items) if todo_list else 0} items")

        if not todo_list:
            logger.warning("PlanHooks: No todo list found!")
            return

        pending_items = [item for item in todo_list.items if item.status == "pending"]
        logger.info(f"PlanHooks: Found {len(pending_items)} pending items")

        if not pending_items:
            return

        current_item = pending_items[0]

        # Build progress display (markdown format)
        progress_content = "---\n\n### Plan Progress:\n\n"

        for i, item in enumerate(todo_list.items, 1):
            if item.status == "completed":
                progress_content += f"- [x] ~~{i}. {item.content}~~\n"
            elif item.id == current_item.id:
                progress_content += f"- [ ] **{i}. {item.content}** (current)\n"
            else:
                progress_content += f"- [ ] {i}. {item.content}\n"

        progress_content += f"\n**Next step:** {current_item.content}"

        try:
            if self.execution_mode == "auto":
                # Merge progress into request content
                choice, callback = await self.broker.request(
                    content=f"{progress_content}\n\n**Auto Mode:** {current_item.content}",
                    choices=["Execute (y)", "Cancel (n)"],
                    default_choice=0,
                    context={"hook_type": "plan_mode", "step": current_item.content},
                )

                if choice.startswith("Execute"):
                    await callback("**Executing...**")
                    return
                else:
                    await callback("**Execution cancelled**")
                    self.plan_phase = "cancelled"
                    raise UserCancelledException("Execution cancelled by user")
            else:
                # Manual mode - merge progress into request content
                choice, callback = await self.broker.request(
                    content=f"{progress_content}\n\n### Options:",
                    choices=[
                        "Execute this step",
                        "Execute this step and continue automatically",
                        "Revise remaining plan",
                        "Cancel",
                    ],
                    default_choice=0,
                    context={"hook_type": "plan_mode", "step": current_item.content},
                )

                if choice == "Execute this step":
                    await callback("**Executing step...**")
                    return
                elif choice.startswith("Execute this step and continue"):
                    self.execution_mode = "auto"
                    await callback("**Switching to auto mode...**")
                    return
                elif choice.startswith("Revise"):
                    await callback("**Revising plan...**")
                    await self._handle_replan()
                    raise PlanningPhaseException(
                        f"REPLAN_REQUIRED: Revise the plan with feedback: {self.replan_feedback}"
                    )
                elif choice.startswith("Cancel"):
                    self._transition_state("cancelled", {"step": current_item.content, "user_choice": choice})
                    await callback("**Execution cancelled**")
                    raise UserCancelledException("User cancelled execution")
                else:
                    await callback(f"**Invalid choice '{choice}'. Please enter 1, 2, 3, or 4.**")

        except InteractionCancelled:
            self._transition_state("cancelled", {"reason": "execution_interrupted"})
            raise UserCancelledException("Execution interrupted")

    def _is_pending_update(self, context) -> bool:
        """
        Determine whether the tool invocation represents a todo status update setting the status to "pending".
        
        Parameters:
            context: ToolContext with a `tool_arguments` attribute containing a JSON string (expected to parse to a dict).
        
        Returns:
            `true` if `tool_arguments` parses to a dict with `"status" == "pending"`, `false` otherwise.
        """
        try:
            import json

            if hasattr(context, "tool_arguments"):
                if context.tool_arguments:
                    tool_args = json.loads(context.tool_arguments)

                    # Check if status is 'pending'
                    if isinstance(tool_args, dict):
                        if tool_args.get("status") == "pending":
                            logger.debug(f"Detected pending status update with args: {tool_args}")
                            return True

            logger.debug("Not a pending status update")
            return False

        except Exception as e:
            logger.debug(f"Error checking tool arguments: {e}")
            return False

    def get_plan_tools(self):
        from datus.tools.func_tool.plan_tools import PlanTool

        plan_tool = PlanTool(self.session)
        plan_tool.storage = self.todo_storage
        return plan_tool.available_tools()