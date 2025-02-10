from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackContext
import os

TOKEN = os.getenv("BOT_TOKEN")  # Load from environment variables
WEB_APP_URL = "https://ankitasharma.shop/"  # Your deployed Vercel app

def start(update: Update, context: CallbackContext):
    keyboard = [[InlineKeyboardButton("🚀 Launch App", url=WEB_APP_URL)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("Click below to launch the app:", reply_markup=reply_markup)

def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
