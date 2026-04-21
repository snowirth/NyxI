#!/usr/bin/env python3
"""Code agent — shell commands, file ops, code review."""
import sys
sys.path.insert(0, "agents")
from base import Agent


class CodeAgent(Agent):
    def run(self):
        task = self.task.lower()

        # Shell command
        if "run " in task or "shell " in task or "command " in task:
            cmd = self.task.split(":", 1)[-1].strip() if ":" in self.task else self.task
            for prefix in ["run the command ", "run command ", "run ", "shell "]:
                if prefix in task:
                    cmd = self.task[task.index(prefix) + len(prefix):].strip()
                    break
            result = self.call_tool("shell", {"command": cmd})
            self.respond(result.get("output", result.get("error", "no output")))
            return

        # Read file
        if "read " in task and ("file" in task or "." in task):
            path = self.task.split()[-1]
            result = self.call_tool("file_ops", {"action": "read", "path": path})
            self.respond(result.get("output", result.get("error", "can't read")))
            return

        # Default: ask LLM
        output = self.llm(f"You are a coding assistant. Be precise.\n\nTask: {self.task}")
        self.respond(output or "need more details.")


if __name__ == "__main__":
    CodeAgent().run()
