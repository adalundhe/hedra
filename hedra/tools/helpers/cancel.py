import asyncio


async def cancel(pending_item: asyncio.Task) -> None:
    pending_item.cancel()
    if not pending_item.cancelled():
        try:
            await pending_item

        except asyncio.CancelledError:
            pass

        except asyncio.IncompleteReadError:
            pass
