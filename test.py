import asyncio
from pyppeteer import launch


async def main():
    browser = await launch()
    page = await browser.newPage()
    await page.goto('http://localhost:5000')
    watchDog = page.waitForFunction('window.status === "ready_to_print"')
    await watchDog
    await page.pdf({'path': 'fast_app/pdfs/schaden.pdf', format: 'A4'})
    await browser.close()

asyncio.get_event_loop().run_until_complete(main())
