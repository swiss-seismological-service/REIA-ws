// Require Puppeteer.
const puppeteer = require('puppeteer');

async function generatePDF() {
	// Launch a new browser session.
	const browser = await puppeteer.launch();
	// Open a new `Page`.
	const page = await browser.newPage();

	// Go to our invoice page that we serve on `localhost:8000`.
	await page.goto('http://localhost:5000');
	// Store the PDF in a file named `invoice.pdf`.
	const watchDog = page.waitForFunction('window.status === "ready_to_print"');
	await watchDog;
	await page.pdf({ path: 'schaden.pdf', format: 'A4' });

	await browser.close();
}
generatePDF();