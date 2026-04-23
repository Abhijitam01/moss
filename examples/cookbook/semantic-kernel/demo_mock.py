"""Demo: Semantic Kernel + Moss Plugin (no credentials needed).

Run with: python demo_mock.py
"""

import asyncio
import types
from unittest.mock import AsyncMock, patch

import semantic_kernel as sk

from moss_semantic_kernel import MossPlugin


def _build_fake_docs():
    """Return mock document objects matching real Moss response shape."""
    return [
        types.SimpleNamespace(
            text="We offer free standard shipping on orders over $50. "
                 "Express shipping is available for $9.99 with 2-day delivery.",
            metadata={"source": "shipping-policy.md"},
            score=0.952,
        ),
        types.SimpleNamespace(
            text="International shipping is available to 40+ countries. "
                 "Delivery times range from 7-14 business days.",
            metadata={"source": "international-faq.md"},
            score=0.891,
        ),
        types.SimpleNamespace(
            text="You can track your order using the tracking link "
                 "sent to your email after shipment.",
            metadata={"source": "order-tracking.md"},
            score=0.834,
        ),
    ]


async def main():
    print("=" * 60)
    print("  Semantic Kernel + Moss Plugin Demo")
    print("=" * 60)

    # 1. Create the plugin
    print("\n[1/4] Creating MossPlugin...")
    moss = MossPlugin(
        project_id="demo-project-id",
        project_key="demo-project-key",
        index_name="product-faq",
        top_k=3,
        alpha=0.8,
    )
    print("       Done - MossPlugin initialized (index='product-faq', top_k=3)")

    # 2. Load the index (mocked)
    print("\n[2/4] Loading Moss index...")
    with patch.object(moss, "_client", autospec=True) as mock_client:
        mock_client.load_index = AsyncMock()
        fake_result = types.SimpleNamespace(
            docs=_build_fake_docs(),
            time_taken_ms=8,
        )
        mock_client.query = AsyncMock(return_value=fake_result)

        await moss.load_index()
        print("       Done - Index 'product-faq' loaded successfully")

        # 3. Register with Semantic Kernel
        print("\n[3/4] Registering plugin with Semantic Kernel...")
        kernel = sk.Kernel()
        kernel.add_plugin(moss, plugin_name="moss")
        print("       Done - Plugin 'moss' registered with kernel")

        # 4. Run a search query
        query = "What are your shipping options?"
        print(f"\n[4/4] Searching: \"{query}\"")
        print("-" * 60)

        result = await kernel.invoke(
            function_name="search",
            plugin_name="moss",
            query=query,
        )

        print(str(result))
        print("-" * 60)

    print("\nDemo complete! The Moss SK plugin is working.\n")


if __name__ == "__main__":
    asyncio.run(main())
