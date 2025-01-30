const { startScraping } = require('./0039_cop');

async function run() {
  console.log('Starting the scraper...');
  await startScraping();
  console.log('Scraping finished!');
}

run();