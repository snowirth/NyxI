#!/usr/bin/env python3
"""Research agent — search web, read articles, extract facts."""
import sys
sys.path.insert(0, "agents")
from base import Agent


class ResearchAgent(Agent):
    def run(self):
        # Search
        results = self.search(self.task)
        if not results or len(results) < 20:
            self.respond("no useful results found.")
            return

        # Summarize
        summary = self.llm(f"Summarize these search results. Be factual and concise:\n\n{results[:2000]}")

        # Auto-remember key facts
        if len(summary) > 50:
            self.remember(f"researched: {summary[:200]}")

        self.respond(summary or "couldn't find much.")


if __name__ == "__main__":
    ResearchAgent().run()
