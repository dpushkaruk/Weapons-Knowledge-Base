# weapons-kb

Information about weapons used in the Russia-Ukraine war is spread across dozens of public Telegram channels that repost, rename and contradict each other. THe miltech Knowledge Base will collect new posts from a fixed list of Ukrainian and Russian channels once a day, uses one trained model with two heads to decide which posts are about weapons, which categories they belong to and which weapon systems they name, and links every mention to one profile per weapon system. A local LLM keeps each profile up to date and a Telegram bot answers questions related to these profiles, citing the posts it used. The desired outcome is a knowledge base that is never more than one day behind the channels and whose every fact can be traced to a post.

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
