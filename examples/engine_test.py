import asyncio

from hedra.core_rewrite.engines.client.playwright import MercurySyncPlaywrightConnection

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


async def run():
    eng = MercurySyncPlaywrightConnection(pool_size=10)

    await eng.start()

    page = await eng.open_page()

    await page.goto("https://www.google.com")

    search_locator = page.locator('[aria-label="Search"]')
    await search_locator.result.click()
    await search_locator.result.fill("Angie Jones")

    await search_locator.result.press("Enter")

    homepage_locator = page.get_by_text("Angie Jones - Angie Jones")

    await homepage_locator.result.click()

    logo_locator = page.locator('[class="eltd-normal-logo"]')

    await logo_locator.result.wait_for()

    await page.wait_for_load_state(state="networkidle")

    contact_locator = page.get_by_role("link", name=" Contact")

    await contact_locator.result.click()

    name_field = page.get_by_text("Your Name (required)")
    await name_field.result.first.wait_for()

    await page.wait_for_load_state(state="networkidle")

    await page.screenshot("/workspace/examples/test.png")

    await eng.close()


loop.run_until_complete(run())
