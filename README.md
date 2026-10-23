# weapons-kb

News about military drones (UAVs) is spread across dozens of Telegram channels that repost, rename and contradict each other.
This project reads public posts from a list of 15 Ukrainian and Russian channels once a day and classifies whether each post is about drones, tagging it with types such as FPV, recon, bomber, middle or deep strike, interceptor and jet-powered.
Drone mentions are linked to a single profile per drone system, even when channels use different names for it.
Each profile is a knowledge-base entry that a local LLM keeps up to date, with every source post linked.
In Lab 5 a Telegram bot agent answers questions from this knowledge base and cites the posts it used.

## Maturity scorecard

| Property | Evidence a reviewer checks | Added in |
|---|---|---|
| Repository reproducible from a clean clone | CI is green; `make setup` and `make lint` succeed on a fresh clone | Lab 1 |
| Design document exists and is current | `docs/design.md` follows the template, every section filled or marked as an assumption | Lab 1 |

## Quick start

```bash
make setup   # install the pinned environment with uv
make lint    # run ruff
uv run pre-commit install   # run ruff automatically before every commit
```
