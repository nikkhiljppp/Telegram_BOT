import asyncio
import logging
import os

from bot import bot, dp, background_tasks
from database import db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

async def main():
    # Initialize database
    await db.connect()
    logger.info("Database initialized")
    
    # Start background tasks
    asyncio.create_task(background_tasks())
    logger.info("Background tasks started")
    
    # Start the bot
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        # Start the bot
        logger.info("Starting bot...")
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")