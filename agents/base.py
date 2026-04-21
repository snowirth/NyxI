#!/usr/bin/env python3
"""Base agent for V2 swarm. Minimal — reads JSON from stdin, calls tools, responds."""
import sys, json


class Agent:
    def __init__(self):
        self.request = json.loads(sys.stdin.readline())
        self.task = self.request.get("task", "")
        self.context = self.request.get("context", {})

    def call_tool(self, name, args):
        print(json.dumps({"type": "tool_call", "name": name, "arguments": args}), flush=True)
        return json.loads(sys.stdin.readline())

    def llm(self, prompt, max_tokens=200):
        result = self.call_tool("_llm_chat", {"prompt": prompt, "max_tokens": max_tokens})
        return result.get("output", "")

    def search(self, query):
        return self.call_tool("web_search", {"query": query}).get("output", "")

    def remember(self, fact):
        self.call_tool("remember", {"content": fact, "network": "knowledge", "importance": 0.6})

    def respond(self, text):
        print(json.dumps({"type": "response", "output": text}), flush=True)

    def handoff(self, agent_type, task):
        """Delegate to another agent. Returns their output."""
        print(json.dumps({"type": "handoff", "agent_type": agent_type, "task": task}), flush=True)
        return json.loads(sys.stdin.readline()).get("output", "")

    def run(self):
        raise NotImplementedError
