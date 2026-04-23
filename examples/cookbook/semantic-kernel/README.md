# Moss Semantic Kernel Cookbook

This cookbook demonstrates how to integrate [Moss](https://moss.dev) with [Semantic Kernel](https://learn.microsoft.com/en-us/semantic-kernel/).

## Overview

Moss is a semantic search platform that allows you to build and query high-performance vector indices without managing infrastructure. This integration provides:

1. **MossPlugin**: A Semantic Kernel plugin that exposes Moss search as a `@kernel_function`.

## Installation

```bash
pip install moss semantic-kernel
```

## Setup

Create a `.env` file with your Moss credentials:

```env
MOSS_PROJECT_ID=your_project_id
MOSS_PROJECT_KEY=your_project_key
MOSS_INDEX_NAME=your_index_name
```

## Usage

```python
import asyncio
import os

import semantic_kernel as sk
from moss_semantic_kernel import MossPlugin


async def main():
    moss = MossPlugin(
        project_id=os.getenv("MOSS_PROJECT_ID"),
        project_key=os.getenv("MOSS_PROJECT_KEY"),
        index_name=os.getenv("MOSS_INDEX_NAME", "my-index"),
    )
    await moss.load_index()

    kernel = sk.Kernel()
    kernel.add_plugin(moss, plugin_name="moss")

    result = await kernel.invoke(
        function_name="search", plugin_name="moss", query="What is your refund policy?"
    )
    print(result)


asyncio.run(main())
```

## Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `project_id` | `MOSS_PROJECT_ID` env var | Moss project ID |
| `project_key` | `MOSS_PROJECT_KEY` env var | Moss project key |
| `index_name` | (required) | Name of the Moss index to query |
| `top_k` | `5` | Number of results to retrieve per query |
| `alpha` | `0.8` | Blend: 1.0 = semantic only, 0.0 = keyword only |
| `result_prefix` | `Relevant knowledge base results:\n\n` | Prefix for formatted results |

## Using with Chat Completion

Moss works with any Semantic Kernel chat completion service. The kernel can automatically invoke the search function when the LLM decides it needs knowledge base information:

```python
import semantic_kernel as sk
from semantic_kernel.connectors.ai.open_ai import OpenAIChatCompletion
from moss_semantic_kernel import MossPlugin

kernel = sk.Kernel()
kernel.add_service(OpenAIChatCompletion(service_id="chat"))

moss = MossPlugin(index_name="product-docs")
await moss.load_index()
kernel.add_plugin(moss, plugin_name="moss")

result = await kernel.invoke_prompt(
    "Use the moss-search function to answer: {{$input}}",
    input_vars={"input": "What are your shipping options?"},
)
```

## Examples

- [`demo_mock.py`](demo_mock.py) — Full demo with mocked data (no credentials needed)
- [`test_moss_plugin.py`](test_moss_plugin.py) — Test suite

## Support

- [Moss Docs](https://docs.moss.dev)
- [Moss Discord](https://discord.com/invite/eMXExuafBR)
- [Semantic Kernel Docs](https://learn.microsoft.com/en-us/semantic-kernel/)
