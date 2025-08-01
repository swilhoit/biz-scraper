import asyncio
from playwright.async_api import async_playwright
from config.settings import SITES

async def get_search_page_html():
    acquire_config = next((site for site in SITES if site['name'] == 'Acquire'), None)
    if not acquire_config:
        print("Acquire config not found")
        return

    url = acquire_config['search_url']
    
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        await page.goto(url, wait_until='networkidle')
        content = await page.content()
        print(content)
        await browser.close()

if __name__ == '__main__':
    asyncio.run(get_search_page_html())
