# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""
Unified streaming output component for CLI operations.

Provides a consistent UI for displaying:
- Progress bar
- Current task/file being processed
- Rolling message window (keeps last N lines)
- LLM output (maximized visibility)
"""

import json
import re
from collections import deque
from contextlib import contextmanager
from typing import Optional

from rich.console import Console, Group
from rich.live import Live
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn
from rich.text import Text


class StreamOutputManager:
    """Unified streaming output manager for displaying task progress and large model output"""

    def __init__(
        self,
        console: Console,
        max_message_lines: int = 10,
        show_progress: bool = True,
        title: str = "Processing",
    ):
        """
        Initialize StreamOutputManager with console and display settings.
        
        Parameters:
            console (Console): Rich Console used for rendering output and Live updates.
            max_message_lines (int): Maximum number of lines retained in the scrolling message window.
            show_progress (bool): Whether to display a progress indicator during operations.
            title (str): Title displayed above the progress area.
        """
        self.console = console
        self.max_message_lines = max_message_lines
        self.show_progress = show_progress
        self.title = title

        # Progress bar will be created in start() based on total_items
        self.progress: Optional[Progress] = None

        # Message Queue (keep last N rows, auto-scroll)
        self.messages = deque(maxlen=max_message_lines)

        # Current task information
        self.current_task = ""
        self.current_file = ""
        self.task_number = 0

        # Live display
        self.live: Optional[Live] = None
        self.progress_task: Optional[TaskID] = None
        self._is_running = False

        # Store complete LLM output for markdown rendering
        self.full_output: list[str] = []

    def _create_progress(self, total_items: int) -> Progress:
        """
        Create a Rich Progress instance configured for the expected number of items.
        
        Parameters:
            total_items (int): Number of items to process; when less than or equal to 1, a spinner-only progress (description column only) is used, otherwise a full progress bar with percentage and completed/total count is returned.
        
        Returns:
            Progress: A configured Rich `Progress` object appropriate for single-item (spinner) or multi-item (bar + percentage + count) display.
        """
        if total_items <= 1:
            # Single task mode: spinner + description only
            return Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
                transient=False,
            )
        else:
            # Multi-task mode: full progress bar with count
            return Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TextColumn("({task.completed}/{task.total})"),
                console=self.console,
                transient=False,
            )

    def start(self, total_items: int, description: Optional[str] = None):
        """
        Start the output manager

        Args:
            total_items: Total number of tasks
            description: Progress bar description (optional)
        """
        if self._is_running:
            return

        # Create progress bar based on total_items count
        self.progress = self._create_progress(total_items)

        desc = description or self.title
        self.progress_task = self.progress.add_task(desc, total=total_items)
        self.live = Live(
            self._render(),
            console=self.console,
            refresh_per_second=4,  # Moderate refresh rate
            transient=False,
            vertical_overflow="visible",  # Prevent line duplication on height changes
        )
        self.live.start()
        self._is_running = True

    def stop(self):
        """Stop the output manager"""
        if self.live and self._is_running:
            self.live.stop()
            self.live = None
            self._is_running = False

    def update_progress(self, advance: int = 1, description: Optional[str] = None):
        """
        Update the progress bar

        Args:
            advance: Progress increments
            description: New description (optional)
        """
        if self.progress is not None and self.progress_task is not None:
            self.progress.update(self.progress_task, advance=advance)
            if description:
                self.progress.update(self.progress_task, description=description)
        self._refresh()

    def set_progress(self, completed: int, description: Optional[str] = None):
        """
        Set the absolute value of the progress bar

        Args:
            completed: Completed quantity
            description: New description (optional)
        """
        if self.progress is not None and self.progress_task is not None:
            self.progress.update(self.progress_task, completed=completed)
            if description:
                self.progress.update(self.progress_task, description=description)
        self._refresh()

    def start_file(self, filepath: str, total_items: Optional[int] = None):
        """
        Start working on the file

        Args:
            filepath: File path
            total_items: Total number of items in the file (optional)
        """
        self.current_file = filepath
        self.messages.clear()
        self.task_number = 0
        if total_items:
            self.add_message(f"Processing {total_items} items...", style="cyan")
        self._refresh()

    def complete_file(self, filepath: str):
        """
        Complete file processing

        Args:
            filepath: File path
        """
        self.current_file = ""
        self._refresh()

    def start_task(self, task_description: str):
        """
        Start working on the task

        Args:
            task_description: Mission description
        """
        self.task_number += 1
        self.current_task = f"[{self.task_number}] {task_description}"
        self._refresh()

    def add_message(self, message: str, style: str = ""):
        """
        Add a message to a scrolling window (auto-scroll, keep only the most recent N lines)

        Args:
            message: Message content (supports multiple lines)
            style: Rich Style (Optional)
        """
        if not message:
            return

        # Handle multi-line messages
        lines = str(message).strip().splitlines()
        for line in lines:
            if line.strip():
                self.messages.append((line, style))

        self._refresh()

    def add_llm_output(self, output: str):
        """
        Add LLM-generated text to the manager and display it in the message window.
        
        Parameters:
            output (str): LLM output to store for later markdown rendering and to display immediately with a prominent white style.
        """
        self.full_output.append(output)
        self.add_message(output, style="white")

    def complete_task(self, success: bool = True, message: str = ""):
        """
        Mark the currently active task as completed and optionally append a styled completion message.
        
        Parameters:
            success (bool): Whether the task succeeded; determines the icon and message style.
            message (str): Optional completion message to add to the message window; if empty no message is added.
        """
        if message:
            icon = "✓" if success else "✗"
            style = "green" if success else "red"
            self.add_message(f"{icon} {message}", style=style)
        self.current_task = ""
        self._refresh()

    def error(self, message: str):
        """
        Display an error message

        Args:
            message: Error message
        """
        self.add_message(f"✗ {message}", style="bold red")

    def warning(self, message: str):
        """
        Display a warning message

        Args:
            message: Warning message
        """
        self.add_message(f"⚠ {message}", style="yellow")

    def success(self, message: str):
        """
        Record a success message prefixed with a checkmark and styled green.
        
        Parameters:
            message (str): Text of the success message to add.
        """
        self.add_message(f"✓ {message}", style="green")

    def render_markdown_summary(self, title: str = "Summary"):
        """
        Render accumulated LLM output as Markdown inside a titled panel.
        
        If the manager has stored LLM output, combine it, extract Markdown content (preferring JSON "output" fields when present), render it as Markdown inside a green-bordered panel with the given title, and then clear the stored output.
        
        Parameters:
            title (str): Title to display on the summary panel (default "Summary").
        """
        if not self.full_output:
            return

        # Combine all output and extract markdown content
        full_text = "\n".join(self.full_output)
        markdown_content = self._extract_markdown_from_output(full_text)

        if markdown_content:
            md = Markdown(markdown_content)
            self.console.print(Panel(md, title=f"📋 {title}", border_style="green"))

        # Clear full_output after rendering
        self.full_output.clear()

    def _extract_markdown_from_output(self, text: str) -> str:
        """
        Extract markdown content from LLM output, preferring an `output` field inside a JSON block.
        
        Parameters:
            text (str): Raw LLM output which may contain plain markdown or a JSON object embedding an `output` field.
        
        Returns:
            str: The markdown text found in the JSON `output` field if present, otherwise the original input `text`.
        """
        # Try to extract 'output' field from JSON
        try:
            # Look for JSON blocks containing 'output' field
            json_match = re.search(r'\{[^{}]*"output"[^{}]*\}', text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                if "output" in data:
                    return data["output"]
        except (json.JSONDecodeError, KeyError):
            pass

        # If no JSON found, return the original text
        return text

    def _render(self):
        """
        Builds the composite renderable group for the live CLI display.
        
        Assembles the display components in order: optional progress bar (at the top), current file panel (if set), current task line (if set), and a messages panel containing the most recent message lines. Each message line is styled and indented; the messages panel is wrapped in a bordered panel.
        
        Returns:
            render_group (rich.console.Group): A Group containing the assembled renderables in display order.
        """
        components = []

        # 1. Progress bar (fixed at the top)
        if self.show_progress and self.progress is not None and self.progress_task is not None:
            components.append(self.progress)
            components.append("")  # Empty line separation

        # 2. Current file (if any)
        if self.current_file:
            file_panel = Panel(
                Text(self.current_file, style="bold cyan"),
                title="📁 Current File",
                border_style="cyan",
                padding=(0, 1),
            )
            components.append(file_panel)

        # 3. Current Task (if any)
        if self.current_task:
            components.append(Text(f"→ {self.current_task}", style="bold yellow"))

        # 4. Message scrolling area (display up to N lines, auto-scrolling)
        if self.messages:
            message_lines = []
            for msg, style in self.messages:
                # Add indentation to make the output clearer
                text = Text(f"  {msg}", style=style or "dim")
                message_lines.append(text)

            # Use Panel to wrap your message, providing a border and title
            messages_panel = Panel(
                Group(*message_lines),
                title="💬 Output ",
                border_style="blue",
                padding=(0, 1),
            )
            components.append(messages_panel)

        return Group(*components)

    def _refresh(self):
        """Refresh the display"""
        if self.live and self._is_running:
            self.live.update(self._render())

    @contextmanager
    def task_context(self, task_description: str):
        """
        Task context manager that automatically handles start and finish

        Usage:
            with output_mgr.task_context("Processing item"):
                # do work
                output_mgr.add_message("Step 1 done")
                output_mgr.add_message("Step 2 done")
            # Automatically marked as complete
        """
        self.start_task(task_description)
        try:
            yield self
            self.complete_task(success=True)
        except Exception as e:
            self.complete_task(success=False, message=str(e))
            raise

    @contextmanager
    def file_context(self, filepath: str, total_items: Optional[int] = None):
        """
        File processing context manager

        Usage:
            with output_mgr.file_context("data.sql", total_items=10):
                for item in items:
                    with output_mgr.task_context(f"Processing {item}"):
                        # do work
        """
        self.start_file(filepath, total_items)
        try:
            yield self
            self.complete_file(filepath)
        except Exception:
            self.complete_file(filepath)
            raise


def create_stream_output_manager(
    console: Console,
    max_message_lines: int = 10,
    show_progress: bool = True,
    title: str = "Processing",
) -> StreamOutputManager:
    """
    Create a factory function for the Streaming Output Manager

    Args:
        console: Rich console instance
        max_message_lines: The maximum number of lines in the message scroll window
        show_progress: Whether a progress bar is displayed
        title: Progress bar title

    Returns:
        StreamOutputManager instance
    """
    return StreamOutputManager(
        console=console,
        max_message_lines=max_message_lines,
        show_progress=show_progress,
        title=title,
    )