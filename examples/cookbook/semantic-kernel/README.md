# Moss Semantic Kernel Cookbook

This cookbook demonstrates how to integrate [Moss](https://moss.dev) with [Semantic Kernel](https://learn.microsoft.com/en-us/semantic-kernel/).

## Overview

Moss delivers sub-10ms semantic retrieval, giving your Semantic Kernel agents instant access to a knowledge base during conversations. This integration provides:

1. **MossPlugin**: A Semantic Kernel plugin that exposes a `search` kernel function for semantic search.

## Installation

```bash
pip install moss semantic-kernel python-dotenv
```

## Setup

Create a `.env` file with your Moss credentials (see `.env.example`):

```env
MOSS_PROJECT_ID=your_project_id
MOSS_PROJECT_KEY=your_project_key
MOSS_INDEX_NAME=your_index_name
```

## Usage

```python
import semantic_kernel as sk
from moss_semantic_kernel import MossPlugin

moss = MossPlugin(
    project_id="your_id",
    project_key="your_key",
    index_name="your_index",
)
await moss.load_index()

kernel = sk.Kernel()
kernel.add_plugin(moss, plugin_name="moss")

result = await kernel.invoke(
    function_name="search", plugin_name="moss", query="What is your refund policy?"
)
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `project_id` | `MOSS_PROJECT_ID` env var | Moss project ID |
| `project_key` | `MOSS_PROJECT_KEY` env var | Moss project key |
| `index_name` | (required) | Name of the Moss index to query |
| `top_k` | `5` | Number of results to retrieve per query |
| `alpha` | `0.8` | Blend: 1.0 = semantic only, 0.0 = keyword only |
| `result_prefix` | `Relevant knowledge base results:\n\n` | Prefix for formatted results |

## Examples

See [moss_sk_simple.py](moss_sk_simple.py) for a complete working example.

## License

This cookbook is provided under the [BSD 2-Clause License](../../../LICENSE).

## Support

- [Moss Docs](https://docs.moss.dev)
- [Moss Discord](https://discord.com/invite/eMXExuafBR)
- [Semantic Kernel Docs](https://learn.microsoft.com/en-us/semantic-kernel/)
