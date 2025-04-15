import logging
import os
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
import random
import string

from aiogram import Bot, Dispatcher, F, types
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery
from aiogram import Router
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties

# Environment variable for API token (more secure than hardcoding)
API_TOKEN = os.getenv("TELEGRAM_API_TOKEN", "YOUR_API_TOKEN_HERE")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Initialize bot and dispatcher
bot = Bot(token=API_TOKEN,
          default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Create and configure router
router = Router()
dp.include_router(router)

# Admin user IDs for accessing admin features
ADMIN_IDS = [6189058729]  # Replace with actual admin Telegram IDs

# Payment methods and details
details_map = {
    # International Payment Methods
    "pay_remitly":
    "Link: https://www.remitly.com/\n\nHow To Pay:\nEnter UPI ID: illusionarts@ybl\nName: Ankita Sharma\nContact: @YG_JOIN",
    "pay_paysend":
    "Link: https://paysend.com/\n\nEnter UPI ID: illusionarts@ybl\nName: Ankita Sharma\nContact: @YG_JOIN",
    "pay_wise":
    "Link: https://wise.com/\n\nEnter UPI ID: illusionarts@ybl\nName: Ankita Sharma\nContact: @YG_JOIN",
    "pay_amazon_intl":
    "Buy Indian Amazon Gift Card\nShare screenshot & code\nContact: @YG_JOIN",
    "pay_wu":
    "Go to https://westernunion.com\nDM @YG_JOIN with 'Western Union'",
    "pay_paypal":
    "Use PayPal ID: SakshiAher66\nNote: May take 21 days\nContact @YG_JOIN",
    "pay_other":
    "If you have any other method, DM @YG_JOIN",

    # Indian Payment Methods
    "pay_upi":
    "Please pay on below UPI ID or scan QR code and share the screenshot:\n\nUPI ID: <code>illusionarts@ybl</code>\n\n<a href='https://imgur.com/a/85cFhHe'>QR Code</a>",
    "pay_amazon_india":
    "Visit amazon.in and purchase a gift card of the requested amount.\nShare the code with @YG_JOIN",

    # Crypto Payment Method
    "pay_crypto":
    "To pay via cryptocurrency, please contact @YG_JOIN\nMention your selected plan and write \"CRYPTO\"."
}

# Service options and pricing
service_options = {
    "video_call": {
        "durations": [{
            "name": "15 min",
            "price": 10
        }, {
            "name": "30 min",
            "price": 15
        }, {
            "name": "60 min",
            "price": 25
        }]
    },
    "group": {
        "names": ["Exclusive", "Ankita's Den"],
        "durations": [{
            "name": "2 Months",
            "price": 20
        }, {
            "name": "6 Months",
            "price": 50
        }, {
            "name": "12 Months",
            "price": 90
        }]
    },
    "private_chat": {
        "durations": [{
            "name": "2 Hr",
            "price": 20
        }, {
            "name": "4 Hr",
            "price": 35
        }],
        "types": [{
            "name": "Sx Chat with Notes",
            "price": 60
        }, {
            "name": "Normal Chat",
            "price": 35
        }]
    },
    "album": [{
        "name": "Node Pic Full Collection (300+)",
        "price": 30
    }, {
        "name": "Node Pic + Vid Full Collection (800+)",
        "price": 60
    }, {
        "name": "My Exclusive Bj Vids (50 Vids)",
        "price": 50
    }, {
        "name": "Master Album (All-in-One)",
        "price": 90
    }]
}

# Database simulation (in a real application, use a proper database)
user_database = {}
transaction_history = []
pending_payments = {}  # To track abandoned payments
scheduled_tasks = {}   # To track scheduled tasks
promo_codes = {
    # Example: "WELCOME10": {"discount": 10, "type": "percentage", "expires": "2024-12-31", "uses": 0, "max_uses": 100}
}
bundle_packages = [
    {
        "id": "bundle1",
        "name": "Starter Bundle",
        "description": "1 Month Group + 1 Album",
        "items": [
            {"service": "group", "duration": "2 Months", "group_name": "Exclusive"},
            {"service": "album", "name": "Node Pic Full Collection (300+)"}
        ],
        "original_price": 50,
        "bundle_price": 40,
        "discount_percentage": 20
    },
    {
        "id": "bundle2",
        "name": "Premium Bundle",
        "description": "6 Months Group + Master Album",
        "items": [
            {"service": "group", "duration": "6 Months", "group_name": "Ankita's Den"},
            {"service": "album", "name": "Master Album (All-in-One)"}
        ],
        "original_price": 140,
        "bundle_price": 110,
        "discount_percentage": 21
    }
]
limited_time_offers = [
    # Example: {"id": "summer_sale", "name": "Summer Sale", "discount": 15, "type": "percentage", "expires": "2024-08-31"}
]
supported_languages = {
    "en": "English",
    "hi": "Hindi",
    "es": "Spanish"
}
translations = {
    "en": {
        "welcome": "Welcome to our service bot! Choose an option below:",
        "payment_success": "Your payment was successful!",
        "payment_pending": "Your payment is pending verification."
    },
    "hi": {
        "welcome": "हमारे सेवा बॉट में आपका स्वागत है! नीचे एक विकल्प चुनें:",
        "payment_success": "आपका भुगतान सफल रहा!",
        "payment_pending": "आपका भुगतान सत्यापन के लिए लंबित है।"
    },
    "es": {
        "welcome": "¡Bienvenido a nuestro bot de servicio! Elija una opción a continuación:",
        "payment_success": "¡Su pago fue exitoso!",
        "payment_pending": "Su pago está pendiente de verificación."
    }
}


class Form(StatesGroup):
    video_duration = State()
    group_name = State()
    group_duration = State()
    chat_duration = State()
    waiting_for_screenshot = State()
    processing_payment = State()
    chat_type = State()
    album_choice = State()
    price = State()
    payment_type = State()
    admin_broadcast = State()
    custom_price = State()
    feedback = State()
    promo_code = State()
    language_selection = State()
    admin_service_edit = State()
    admin_pricing_edit = State()
    admin_scheduled_broadcast = State()
    admin_promo_create = State()
    admin_bundle_create = State()


# Helper function to create a back button
def create_back_button(builder: InlineKeyboardBuilder) -> None:
    builder.button(text="« Back to Main Menu", callback_data="back_to_main")


# Helper function to generate main menu
async def show_main_menu(message: types.Message) -> None:
    user_id = message.from_user.id if hasattr(message, 'from_user') else None
    user_language = "en"  # Default language
    
    # Get user's preferred language if available
    if user_id and user_id in user_database and "language" in user_database[user_id]:
        user_language = user_database[user_id]["language"]
    
    kb = InlineKeyboardBuilder()
    kb.button(text="📞 Video Call", callback_data="video_call")
    kb.button(text="👥 Join Group", callback_data="group")
    kb.button(text="💬 Private Chat", callback_data="private_chat")
    kb.button(text="📸 Album", callback_data="album")
    kb.button(text="📦 Bundle Packages", callback_data="bundles")
    kb.button(text="🎁 Enter Promo Code", callback_data="enter_promo")
    kb.button(text="📝 Leave Feedback", callback_data="feedback")
    kb.button(text="❓ Help", callback_data="help")
    kb.button(text="🌐 Change Language", callback_data="change_language")
    kb.adjust(1)

    # Check if there are any active limited-time offers to display
    current_time = datetime.now()
    active_offers = [
        offer for offer in limited_time_offers 
        if datetime.fromisoformat(offer["expires"]) > current_time
    ]
    
    welcome_text = translations[user_language].get("welcome", "Choose an option below:")
    
    if active_offers:
        offer_text = "\n\n🔥 <b>Limited Time Offers:</b>\n"
        for offer in active_offers:
            offer_text += f"• {offer['name']}: {offer['discount']}% off! Ends {offer['expires'].split('T')[0]}\n"
        welcome_text += offer_text
    
    await message.answer(welcome_text, reply_markup=kb.as_markup())


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    """Handle the /start command"""
    user_id = message.from_user.id
    user_name = message.from_user.username or message.from_user.first_name

    # Store user info
    if user_id not in user_database:
        user_database[user_id] = {
            "username": user_name,
            "joined_date": datetime.now().isoformat(),
            "purchases": []
        }
        logger.info(f"New user registered: {user_id} ({user_name})")

    await state.clear()
    await show_main_menu(message)


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Handle the /help command"""
    help_text = ("<b>Available Commands:</b>\n"
                 "/start - Show main menu\n"
                 "/help - Show this help message\n"
                 "/cancel - Cancel current operation\n\n"
                 "For any issues or questions, contact @YG_JOIN")
    await message.answer(help_text)


@dp.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    """Admin panel access"""
    user_id = message.from_user.id
    if user_id in ADMIN_IDS:
        kb = InlineKeyboardBuilder()
        kb.button(text="📊 User Statistics", callback_data="admin_stats")
        kb.button(text="📣 Broadcast Message", callback_data="admin_broadcast")
        kb.button(text="📅 Schedule Broadcast", callback_data="admin_schedule_broadcast")
        kb.button(text="💰 Custom Price Quote", callback_data="admin_custom_price")
        kb.button(text="🛠️ Manage Services", callback_data="admin_manage_services")
        kb.button(text="💲 Manage Pricing", callback_data="admin_manage_pricing")
        kb.button(text="🎁 Create Promo Code", callback_data="admin_create_promo")
        kb.button(text="📦 Manage Bundles", callback_data="admin_manage_bundles")
        kb.button(text="⏱️ Create Limited Offer", callback_data="admin_create_offer")
        kb.button(text="📊 Advanced Analytics", callback_data="admin_advanced_analytics")
        kb.adjust(1)
        await message.answer("<b>Admin Panel</b>\nWelcome to the admin panel.",
                             reply_markup=kb.as_markup())
    else:
        await message.answer(
            "You don't have permission to access this feature.")


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    """Cancel the current operation and return to main menu"""
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer("Operation cancelled.")
    await show_main_menu(message)


@dp.callback_query(F.data == "back_to_main")
async def back_to_main(call: CallbackQuery, state: FSMContext) -> None:
    """Handle back to main menu button"""
    # Get transaction ID if available to track abandoned payment
    user_data = await state.get_data()
    transaction_id = user_data.get("transaction_id")
    
    # If returning from payment flow, mark as potentially abandoned
    if transaction_id and user_data.get("price") and transaction_id in pending_payments:
        # Keep it in pending_payments for reminder system
        logger.info(f"User {call.from_user.id} potentially abandoned payment {transaction_id}")
    
    await state.clear()
    await call.message.edit_text("Choose an option below:")
    await show_main_menu(call.message)


@dp.callback_query(F.data == "video_call")
async def video_call_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle video call selection"""
    await state.set_state(Form.video_duration)
    options = service_options["video_call"]["durations"]

    kb = InlineKeyboardBuilder()
    for option in options:
        kb.button(text=f"{option['name']} - ${option['price']}",
                  callback_data=f"price_{option['price']}")
    create_back_button(kb)
    kb.adjust(1)

    await call.message.edit_text("Select video call duration:",
                                 reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("price_"))
async def video_price_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle price selection"""
    price = call.data.split("_")[1]
    original_price = float(price)
    
    # Get current service type from state
    user_data = await state.get_data()
    service_type = user_data.get("service_type", "unknown")
    
    # Check for active promo codes
    user_id = call.from_user.id
    final_price = original_price
    applied_promo = None
    
    if user_id in user_database and "active_promos" in user_database[user_id]:
        # Find the most recent active promo
        active_promos = user_database[user_id]["active_promos"]
        if active_promos:
            # Sort by applied date (most recent first)
            active_promos.sort(key=lambda x: x.get("applied_date", ""), reverse=True)
            promo = active_promos[0]
            
            # Apply the discount
            if promo["type"] == "percentage":
                discount_amount = original_price * (promo["discount"] / 100)
                final_price = original_price - discount_amount
            else:  # amount discount
                final_price = max(0, original_price - promo["discount"])
            
            applied_promo = promo["code"]
            
            # Remove the used promo
            user_database[user_id]["active_promos"].remove(promo)
    
    # Round to 2 decimal places
    final_price = round(final_price, 2)
    await state.update_data(price=str(final_price), original_price=str(original_price))

    # Record transaction details
    if user_id in user_database:
        purchase_data = {
            "service": service_type,
            "price": str(final_price),
            "original_price": str(original_price),
            "date": datetime.now().isoformat(),
            "status": "pending"
        }
        
        if applied_promo:
            purchase_data["promo_code"] = applied_promo
            purchase_data["discount_amount"] = str(original_price - final_price)
        
        user_database[user_id]["purchases"].append(purchase_data)
    
    # Generate transaction ID
    transaction_id = f"TRX{datetime.now().strftime('%Y%m%d%H%M%S')}{call.from_user.id}"
    await state.update_data(transaction_id=transaction_id)
    
    # Track this as a pending payment for reminder purposes
    pending_payments[transaction_id] = {
        "user_id": user_id,
        "service_type": service_type,
        "price": str(final_price),
        "timestamp": datetime.now(),
        "reminder_1_sent": False,
        "reminder_2_sent": False,
        "reminder_3_sent": False
    }
    
    # Show payment type options with discount info if applicable
    if applied_promo:
        kb = InlineKeyboardBuilder()
        kb.button(text="🇮🇳 Indian Payment", callback_data="payment_type_indian")
        kb.button(text="🌍 International Payment", callback_data="payment_type_international")
        kb.button(text="💰 Crypto Payment", callback_data="payment_type_crypto")
        create_back_button(kb)
        kb.adjust(1)
        
        discount_text = f"Original price: ${original_price}\nDiscount applied: ${original_price - final_price} (Promo: {applied_promo})\nFinal price: ${final_price}"
        
        await call.message.edit_text(
            f"<b>Discount Applied!</b>\n\n{discount_text}\n\nChoose your payment type:",
            reply_markup=kb.as_markup()
        )
    else:
        # Standard payment flow
        await show_payment_types(call)


@dp.callback_query(F.data == "group")
async def group_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle group selection"""
    # Clear previous state and set new state
    await state.clear()
    await state.set_state(Form.group_name)
    await state.update_data(service_type="group")
    
    # Create keyboard with group options
    kb = InlineKeyboardBuilder()
    for name in service_options["group"]["names"]:
        kb.button(text=name, callback_data=f"group_{name}")
    create_back_button(kb)
    kb.adjust(1)

    try:
        await call.message.edit_text("Choose a group to join:", reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Error displaying group options: {e}")


@dp.callback_query(F.data.startswith("group_") & ~F.data.startswith("group_duration_"))
async def group_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle specific group selection"""
    group_name = call.data.split("_")[1]
    await state.update_data(group_name=group_name, service_type="group")
    await state.set_state(Form.group_duration)

    # Create keyboard with duration options
    kb = InlineKeyboardBuilder()
    for duration in service_options["group"]["durations"]:
        kb.button(text=f"{duration['name']} - ${duration['price']}",
                  callback_data=f"group_duration_{duration['price']}")
    create_back_button(kb)
    kb.adjust(1)

    try:
        await call.message.edit_text(
            f"<b>Selected Group:</b> {group_name}\n\nChoose your plan duration:",
            reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Error displaying group duration options: {e}")


@dp.callback_query(F.data.startswith("group_duration_"))
async def group_price_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle group duration price selection"""
    price = call.data.split("_")[2]
    await state.update_data(price=price, service_type="group")

    # Record transaction
    user_data = await state.get_data()
    user_id = call.from_user.id

    if user_id in user_database:
        user_database[user_id]["purchases"].append({
            "service": "group",
            "group_name": user_data.get("group_name"),
            "price": price,
            "date": datetime.now().isoformat(),
            "status": "pending"
        })

    # Show payment type options directly
    kb = InlineKeyboardBuilder()
    kb.button(text="🇮🇳 Indian Payment", callback_data="payment_type_indian")
    kb.button(text="🌍 International Payment", callback_data="payment_type_international")
    kb.button(text="💰 Crypto Payment", callback_data="payment_type_crypto")
    create_back_button(kb)
    kb.adjust(1)

    try:
        await call.message.edit_text("Choose your payment type:", reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Error displaying payment types: {e}")


@dp.callback_query(F.data == "private_chat")
async def private_chat_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle private chat selection"""
    await state.set_state(Form.chat_duration)
    await state.update_data(service_type="private_chat")
    durations = service_options["private_chat"]["durations"]

    kb = InlineKeyboardBuilder()
    for duration in durations:
        kb.button(text=f"{duration['name']} - ${duration['price']}",
                  callback_data=f"chat_{duration['name'].split()[0]}")
    create_back_button(kb)
    kb.adjust(1)

    await call.message.edit_text("Select chat duration:",
                                 reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("chat_"))
async def chat_duration_selected(call: CallbackQuery,
                                 state: FSMContext) -> None:
    """Handle chat duration selection"""
    # Check if this is a chat type selection
    if call.data.startswith("chat_type_"):
        chat_type = call.data.split("_")[2]
        types = service_options["private_chat"]["types"]
        selected_type = next(
            (t for t in types if t["name"].lower().startswith(chat_type)),
            None)

        if selected_type:
            price = selected_type["price"]
            await state.update_data(price=price,
                                    chat_type=selected_type["name"])

            # Record transaction
            user_data = await state.get_data()
            user_id = call.from_user.id

            if user_id in user_database:
                user_database[user_id]["purchases"].append({
                    "service":
                    "private_chat",
                    "chat_duration":
                    user_data.get("chat_duration"),
                    "chat_type":
                    selected_type["name"],
                    "price":
                    price,
                    "date":
                    datetime.now().isoformat(),
                    "status":
                    "pending"
                })

            # Show payment type options
            await show_payment_types(call)
    else:
        # This is a duration selection
        chat_duration = call.data.split("_")[1]
        await state.update_data(chat_duration=chat_duration)
        await state.set_state(Form.chat_type)

        types = service_options["private_chat"]["types"]

        kb = InlineKeyboardBuilder()
        for type_info in types:
            kb.button(text=f"{type_info['name']} - ${type_info['price']}",
                      callback_data=
                      f"chat_type_{type_info['name'].split()[0].lower()}")
        create_back_button(kb)
        kb.adjust(1)

        await call.message.edit_text("Choose chat type:",
                                     reply_markup=kb.as_markup())


@dp.callback_query(F.data == "album")
async def album_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle album selection"""
    await state.set_state(Form.album_choice)
    await state.update_data(service_type="album")
    albums = service_options["album"]

    kb = InlineKeyboardBuilder()
    for album in albums:
        kb.button(text=f"{album['name']} - ${album['price']}",
                  callback_data=f"album_{album['price']}")
    create_back_button(kb)
    kb.adjust(1)

    await call.message.edit_text("Choose Album Option:",
                                 reply_markup=kb.as_markup())


@dp.callback_query(F.data.startswith("album_"))
async def album_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle specific album selection"""
    price = call.data.split("_")[1]
    album_name = next(
        (a["name"]
         for a in service_options["album"] if str(a["price"]) == price),
        "Unknown Album")

    await state.update_data(price=price, album_name=album_name)

    # Record transaction
    user_id = call.from_user.id
    if user_id in user_database:
        user_database[user_id]["purchases"].append({
            "service":
            "album",
            "album_name":
            album_name,
            "price":
            price,
            "date":
            datetime.now().isoformat(),
            "status":
            "pending"
        })

    # Show payment type options
    await show_payment_types(call)


async def show_payment_types(call: CallbackQuery) -> None:
    """Show payment type options (Indian, International, Crypto)"""
    kb = InlineKeyboardBuilder()
    kb.button(text="🇮🇳 Indian Payment", callback_data="payment_type_indian")
    kb.button(text="🌍 International Payment", callback_data="payment_type_international")
    kb.button(text="💰 Crypto Payment", callback_data="payment_type_crypto")
    create_back_button(kb)
    kb.adjust(1)

    try:
        await call.message.edit_text("Choose your payment type:", reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Error displaying payment types: {e}")


@dp.callback_query(F.data.startswith("payment_type_"))
async def payment_type_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle payment type selection"""
    payment_type = call.data.split("_")[2]
    await state.update_data(payment_type=payment_type)

    # Handle crypto payment directly
    if payment_type == "crypto":
        await show_payment_details(call, state)
        return

    kb = InlineKeyboardBuilder()

    if payment_type == "indian":
        # Indian payment options
        kb.button(text="UPI ID/QR Code", callback_data="pay_upi")
        kb.button(text="Amazon Gift Card", callback_data="pay_amazon_india")
    elif payment_type == "international":
        # International payment options
        kb.button(text="Remitly", callback_data="pay_remitly")
        kb.button(text="Paysend", callback_data="pay_paysend")
        kb.button(text="Wise", callback_data="pay_wise")
        kb.button(text="Amazon Gift Card", callback_data="pay_amazon_intl")
        kb.button(text="Western Union", callback_data="pay_wu")
        kb.button(text="PayPal", callback_data="pay_paypal")
        kb.button(text="Other", callback_data="pay_other")

    kb.button(text="« Back to Payment Types", callback_data="back_to_payment_types")
    create_back_button(kb)
    kb.adjust(1)

    try:
        await call.message.edit_text(
            "Choose your payment method:\n\n"
            "<i>After payment, please contact @YG_JOIN with your payment confirmation.</i>",
            reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Error displaying payment methods: {e}")


@dp.callback_query(F.data == "back_to_payment_types")
async def back_to_payment_types(call: CallbackQuery) -> None:
    """Handle back to payment types button"""
    await show_payment_types(call)


@dp.callback_query(F.data.startswith("pay_"))
async def show_payment_details(call: CallbackQuery, state: FSMContext) -> None:
    """Show payment details for selected method"""
    method = call.data

    # For crypto payment from payment type selection
    if method == "payment_type_crypto":
        method = "pay_crypto"

    details = details_map.get(method, "Payment method not found.")

    # Get transaction info
    user_data = await state.get_data()
    service_type = user_data.get("service_type", "Unknown")
    price = user_data.get("price", "0")
    original_price = user_data.get("original_price", price)
    
    # Get or generate transaction ID
    transaction_id = user_data.get("transaction_id")
    if not transaction_id:
        transaction_id = f"TRX{datetime.now().strftime('%Y%m%d%H%M%S')}{call.from_user.id}"
        await state.update_data(transaction_id=transaction_id)
        
        # Track this as a pending payment for reminder purposes
        pending_payments[transaction_id] = {
            "user_id": call.from_user.id,
            "service_type": service_type,
            "price": price,
            "timestamp": datetime.now(),
            "reminder_1_sent": False,
            "reminder_2_sent": False,
            "reminder_3_sent": False
        }

    kb = InlineKeyboardBuilder()

    # Add "I've Made the Payment" button
    kb.button(text="✅ I've Made the Payment", callback_data="confirm_payment")

    # Add "Copy UPI ID" button for UPI payment method
    if method == "pay_upi":
        kb.button(text="📋 Copy UPI ID", callback_data="copy_upi_id")

    # Add appropriate back button based on payment type
    payment_type = user_data.get("payment_type", "")
    if payment_type:
        kb.button(text="« Back to Payment Methods",
                  callback_data=f"back_to_{payment_type}_methods")
    else:
        kb.button(text="« Back to Payment Types",
                  callback_data="back_to_payment_types")

    kb.button(text="« Back to Main Menu", callback_data="back_to_main")
    kb.adjust(1)

    # Prepare order summary text
    order_summary = f"Service: {service_type}\nAmount: ${price}"
    
    # Add discount info if applicable
    if original_price != price:
        order_summary += f"\nOriginal Price: ${original_price}\nDiscount: ${float(original_price) - float(price)}"
    
    # Add bundle items if this is a bundle purchase
    if service_type == "bundle":
        bundle_id = user_data.get("bundle_id")
        selected_bundle = next((b for b in bundle_packages if b["id"] == bundle_id), None)
        if selected_bundle:
            order_summary = f"Bundle: {selected_bundle['name']}\nAmount: ${price}"
            
            # Add items list
            items_text = "\n\nIncludes:"
            for item in selected_bundle["items"]:
                if item["service"] == "group":
                    items_text += f"\n• {item['group_name']} Group ({item['duration']})"
                elif item["service"] == "album":
                    items_text += f"\n• {item['name']}"
                else:
                    items_text += f"\n• {item['service'].capitalize()}"
            
            order_summary += items_text

    try:
        await call.message.edit_text(
            f"<b>Payment Instructions:</b>\n\n"
            f"{details}\n\n"
            f"<b>Order Summary:</b>\n"
            f"{order_summary}\n"
            f"Transaction ID: <code>{transaction_id}</code>\n\n"
            f"<i>After making payment, click 'I've Made the Payment' button and send a screenshot as proof.</i>",
            reply_markup=kb.as_markup())
    except Exception as e:
        logger.error(f"Error displaying payment details: {e}")

    # Log transaction if not already logged
    existing_transaction = next((t for t in transaction_history if t.get("transaction_id") == transaction_id), None)
    if not existing_transaction:
        payment_method = method.replace("pay_", "")
        transaction_data = {
            "transaction_id": transaction_id,
            "user_id": call.from_user.id,
            "username": call.from_user.username or call.from_user.first_name,
            "service": service_type,
            "amount": price,
            "payment_method": payment_method,
            "payment_type": user_data.get("payment_type", "unknown"),
            "status": "pending",
            "created_at": datetime.now().isoformat()
        }
        
        # Add original price and discount if applicable
        if original_price != price:
            transaction_data["original_price"] = original_price
            transaction_data["discount_amount"] = str(float(original_price) - float(price))
            
            if "promo_code" in user_data:
                transaction_data["promo_code"] = user_data["promo_code"]
        
        transaction_history.append(transaction_data)


@dp.callback_query(F.data == "confirm_payment")
async def confirm_payment_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle payment confirmation button"""
    await state.set_state(Form.waiting_for_screenshot)
    
    # Get transaction ID to remove from pending payments tracking
    user_data = await state.get_data()
    transaction_id = user_data.get("transaction_id")
    
    # If user confirms payment, remove from abandoned payment tracking
    if transaction_id and transaction_id in pending_payments:
        # We'll keep it in the dictionary but mark it as confirmed
        pending_payments[transaction_id]["payment_confirmed"] = True
        logger.info(f"User {call.from_user.id} confirmed payment for {transaction_id}")

    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_main")

    await call.message.edit_text(
        "Please share a screenshot of your payment as proof.\n\n"
        "Send the image directly in this chat.",
        reply_markup=kb.as_markup())


@dp.message(Form.waiting_for_screenshot, F.photo)
async def handle_screenshot(message: Message, state: FSMContext) -> None:
    """Process payment screenshot"""
    user_id = message.from_user.id
    user_name = message.from_user.username or message.from_user.first_name

    # Get transaction data
    user_data = await state.get_data()
    service_type = user_data.get("service_type", "Unknown")
    price = user_data.get("price", "0")
    transaction_id = user_data.get("transaction_id", "unknown")
    
    # If this is a bundle purchase, include bundle details
    bundle_details = ""
    if service_type == "bundle":
        bundle_id = user_data.get("bundle_id")
        selected_bundle = next((b for b in bundle_packages if b["id"] == bundle_id), None)
        if selected_bundle:
            bundle_details = f"\nBundle: {selected_bundle['name']}"

    # Update transaction status
    for purchase in user_database.get(user_id, {}).get("purchases", []):
        if purchase.get("status") == "pending":
            purchase["status"] = "processing"
            
            # If this is a group subscription, set expiry date
            if purchase.get("service") == "group" and "expiry_date" not in purchase:
                # Get duration in months from the purchase
                duration_text = purchase.get("group_duration", "2 Months")
                try:
                    # Extract number of months from text like "2 Months"
                    months = int(duration_text.split()[0])
                    expiry_date = (datetime.now() + timedelta(days=30*months)).isoformat()
                    purchase["expiry_date"] = expiry_date
                    purchase["renewal_reminder_sent"] = False
                    purchase["final_reminder_sent"] = False
                    purchase["auto_renew"] = False
                except (ValueError, IndexError):
                    # Default to 2 months if parsing fails
                    expiry_date = (datetime.now() + timedelta(days=60)).isoformat()
                    purchase["expiry_date"] = expiry_date
            break

    # Update transaction history
    for transaction in transaction_history:
        if transaction.get("transaction_id") == transaction_id:
            transaction["status"] = "processing"
            break
    
    # Remove from pending payments tracking or mark as processing
    if transaction_id in pending_payments:
        pending_payments[transaction_id]["status"] = "processing"

    # Notify user about processing status
    await message.answer(
        "✅ Screenshot received!\n\n"
        "Your payment is now being processed. An admin will verify it shortly.\n"
        "You'll receive a confirmation message once verified.\n\n"
        f"Transaction ID: <code>{transaction_id}</code>")

    # Notify admins about new payment
    for admin_id in ADMIN_IDS:
        try:
            # Forward the screenshot to admin
            await message.forward(admin_id)

            # Send payment details after the screenshot
            kb = InlineKeyboardBuilder()
            kb.button(text="✅ Approve Payment", callback_data=f"approve_{user_id}_{transaction_id}")
            kb.button(text="❌ Reject Payment", callback_data=f"reject_{user_id}_{transaction_id}")
            kb.adjust(1)
            
            # Prepare order summary
            order_summary = f"Service: {service_type}{bundle_details}\nAmount: ${price}"
            
            # Add discount info if applicable
            original_price = user_data.get("original_price")
            if original_price and original_price != price:
                order_summary += f"\nOriginal Price: ${original_price}\nDiscount: ${float(original_price) - float(price)}"
                if "promo_code" in user_data:
                    order_summary += f"\nPromo Code: {user_data['promo_code']}"

            await bot.send_message(
                admin_id, f"<b>📥 New Payment Submission</b>\n\n"
                f"From: {user_name} (ID: {user_id})\n"
                f"{order_summary}\n"
                f"Transaction ID: <code>{transaction_id}</code>",
                reply_markup=kb.as_markup())
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id} about payment: {e}")

    await state.set_state(Form.processing_payment)


@dp.callback_query(F.data.startswith("approve_"))
async def approve_payment(call: CallbackQuery) -> None:
    """Handle payment approval by admin"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return

    # Extract user ID and transaction ID from callback data
    parts = call.data.split("_")
    user_id = int(parts[1])
    transaction_id = parts[2] if len(parts) > 2 else "unknown"
    service_type = "Unknown"
    bundle_info = None

    # Update transaction status
    for purchase in user_database.get(user_id, {}).get("purchases", []):
        if purchase.get("status") == "processing":
            purchase["status"] = "completed"
            service_type = purchase.get("service", "Unknown")
            
            # If this is a bundle purchase, get bundle details
            if service_type == "bundle":
                bundle_id = purchase.get("bundle_id")
                bundle_info = next((b for b in bundle_packages if b["id"] == bundle_id), None)
            
            # If this is a renewal, update the expiry date
            if service_type == "renewal":
                renewal_service = purchase.get("renewal_service", "")
                
                # Find the original subscription to update
                for old_purchase in user_database.get(user_id, {}).get("purchases", []):
                    if (old_purchase.get("service") == "group" and 
                        renewal_service in old_purchase.get("group_name", "") and
                        old_purchase.get("status") == "completed"):
                        
                        # Get current expiry date
                        current_expiry = datetime.fromisoformat(old_purchase.get("expiry_date", datetime.now().isoformat()))
                        
                        # Add duration based on the renewal
                        duration_text = purchase.get("renewal_duration", "2 Months")
                        try:
                            months = int(duration_text.split()[0])
                            new_expiry = (current_expiry + timedelta(days=30*months)).isoformat()
                            old_purchase["expiry_date"] = new_expiry
                            old_purchase["renewal_reminder_sent"] = False
                            old_purchase["final_reminder_sent"] = False
                        except (ValueError, IndexError):
                            # Default to 2 months if parsing fails
                            new_expiry = (current_expiry + timedelta(days=60)).isoformat()
                            old_purchase["expiry_date"] = new_expiry
                        
                        break
            break

    # Update transaction history
    for transaction in transaction_history:
        if transaction.get("transaction_id") == transaction_id:
            transaction["status"] = "completed"
            break
    
    # Remove from pending payments tracking
    if transaction_id in pending_payments:
        del pending_payments[transaction_id]

    # Send confirmation to user
    try:
        # Default instructions based on service type
        service_instructions = {
            "group": "You'll receive an invite link to join the group within 24 hours.",
            "video_call": "Our agent will contact you to schedule your video call.",
            "private_chat": "Our agent will initiate a private chat with you shortly.",
            "album": "You'll receive access to the album content within 24 hours.",
            "bundle": "You'll receive access to all bundle items within 24 hours.",
            "renewal": "Your subscription has been extended. Thank you for renewing!"
        }.get(service_type, "")
        
        # Customize message for bundle purchases
        message_text = f"🎉 <b>Payment Approved!</b>\n\n"
        
        if service_type == "bundle" and bundle_info:
            message_text += f"Your payment for the {bundle_info['name']} bundle has been verified and approved.\n\n"
            message_text += "<b>Your bundle includes:</b>\n"
            
            for item in bundle_info["items"]:
                if item["service"] == "group":
                    message_text += f"• {item['group_name']} Group ({item['duration']})\n"
                elif item["service"] == "album":
                    message_text += f"• {item['name']}\n"
                else:
                    message_text += f"• {item['service'].capitalize()}\n"
            
            message_text += f"\n{service_instructions}\n"
        elif service_type == "renewal":
            message_text += "Your subscription renewal has been verified and approved.\n"
            message_text += f"Your subscription has been extended.\n\n"
        else:
            message_text += f"Your payment for {service_type} has been verified and approved.\n"
            message_text += f"{service_instructions}\n\n"
        
        message_text += f"Transaction ID: <code>{transaction_id}</code>\n\n"
        message_text += f"Thank you for your purchase! If you have any questions, contact @YG_JOIN."
        
        await bot.send_message(user_id, message_text)
    except Exception as e:
        logger.error(f"Failed to send approval notification to user {user_id}: {e}")

    await call.message.edit_text(
        f"{call.message.text}\n\n"
        f"✅ <b>APPROVED</b> by {call.from_user.username or call.from_user.first_name}"
    )


@dp.callback_query(F.data.startswith("reject_"))
async def reject_payment(call: CallbackQuery) -> None:
    """Handle payment rejection by admin"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return

    # Extract user ID and transaction ID from callback data
    parts = call.data.split("_")
    user_id = int(parts[1])
    transaction_id = parts[2] if len(parts) > 2 else "unknown"

    # Update transaction status
    for purchase in user_database.get(user_id, {}).get("purchases", []):
        if purchase.get("status") == "processing":
            purchase["status"] = "rejected"
            break

    for transaction in transaction_history:
        if transaction.get("transaction_id") == transaction_id:
            transaction["status"] = "rejected"
            break
    
    # Remove from pending payments tracking
    if transaction_id in pending_payments:
        del pending_payments[transaction_id]

    # Send rejection to user
    try:
        kb = InlineKeyboardBuilder()
        kb.button(text="Try Again", callback_data="back_to_main")
        kb.button(text="Contact Support", url="https://t.me/YG_JOIN")
        kb.adjust(1)
        
        await bot.send_message(
            user_id, 
            f"❌ <b>Payment Verification Failed</b>\n\n"
            f"We were unable to verify your payment.\n"
            f"Please contact @YG_JOIN for assistance or try again with a clearer screenshot.\n\n"
            f"Transaction ID: <code>{transaction_id}</code>",
            reply_markup=kb.as_markup()
        )
    except Exception as e:
        logger.error(f"Failed to send rejection notification to user {user_id}: {e}")

    await call.message.edit_text(
        f"{call.message.text}\n\n"
        f"❌ <b>REJECTED</b> by {call.from_user.username or call.from_user.first_name}"
    )


@dp.callback_query(F.data == "copy_upi_id")
async def copy_upi_id(call: CallbackQuery) -> None:
    """Handle copy UPI ID button press"""
    await call.answer("UPI ID copied: illusionarts@ybl", show_alert=True)


@dp.callback_query(F.data.startswith("back_to_") & F.data.endswith("_methods"))
async def back_to_payment_methods(call: CallbackQuery,
                                  state: FSMContext) -> None:
    """Handle back to payment methods button"""
    payment_type = call.data.split("_")[2]
    await state.update_data(payment_type=payment_type)
    await payment_type_selected(call, state)


@dp.callback_query(F.data == "bundles")
async def bundle_packages_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle bundle packages selection"""
    kb = InlineKeyboardBuilder()
    
    for bundle in bundle_packages:
        kb.button(
            text=f"{bundle['name']} - ${bundle['bundle_price']} (Save {bundle['discount_percentage']}%)",
            callback_data=f"bundle_{bundle['id']}"
        )
    
    create_back_button(kb)
    kb.adjust(1)
    
    await call.message.edit_text(
        "<b>📦 Bundle Packages</b>\n\n"
        "Get more value with our special bundles! Each bundle combines multiple services at a discounted price.",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("bundle_"))
async def bundle_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle specific bundle selection"""
    bundle_id = call.data.split("_")[1]
    selected_bundle = next((b for b in bundle_packages if b["id"] == bundle_id), None)
    
    if not selected_bundle:
        await call.answer("Bundle not found. Please try again.")
        return
    
    # Store bundle info in state
    await state.update_data(
        service_type="bundle",
        bundle_id=bundle_id,
        price=selected_bundle["bundle_price"]
    )
    
    # Format bundle items for display
    items_text = ""
    for item in selected_bundle["items"]:
        if item["service"] == "group":
            items_text += f"• {item['group_name']} Group ({item['duration']})\n"
        elif item["service"] == "album":
            items_text += f"• {item['name']}\n"
        else:
            items_text += f"• {item['service'].capitalize()}\n"
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Purchase Bundle", callback_data=f"purchase_bundle_{bundle_id}")
    create_back_button(kb)
    kb.adjust(1)
    
    await call.message.edit_text(
        f"<b>{selected_bundle['name']}</b>\n\n"
        f"{selected_bundle['description']}\n\n"
        f"<b>Includes:</b>\n{items_text}\n"
        f"Original Price: ${selected_bundle['original_price']}\n"
        f"Bundle Price: ${selected_bundle['bundle_price']}\n"
        f"You Save: ${selected_bundle['original_price'] - selected_bundle['bundle_price']} ({selected_bundle['discount_percentage']}%)",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("purchase_bundle_"))
async def purchase_bundle(call: CallbackQuery, state: FSMContext) -> None:
    """Handle bundle purchase"""
    await show_payment_types(call)

@dp.callback_query(F.data == "enter_promo")
async def enter_promo_code(call: CallbackQuery, state: FSMContext) -> None:
    """Handle promo code entry"""
    await state.set_state(Form.promo_code)
    
    kb = InlineKeyboardBuilder()
    create_back_button(kb)
    
    await call.message.edit_text(
        "🎁 <b>Promo Code</b>\n\n"
        "Please enter your promo code to receive a discount on your next purchase.",
        reply_markup=kb.as_markup()
    )

@dp.message(Form.promo_code)
async def process_promo_code(message: Message, state: FSMContext) -> None:
    """Process entered promo code"""
    promo_code = message.text.strip().upper()
    user_id = message.from_user.id
    
    # Check if promo code exists and is valid
    if promo_code in promo_codes:
        promo = promo_codes[promo_code]
        current_date = datetime.now()
        
        # Check if promo code is expired
        if "expires" in promo and datetime.fromisoformat(promo["expires"]) < current_date:
            await message.answer(
                "❌ This promo code has expired.\n\n"
                "Please check for other available promotions."
            )
        # Check if promo code has reached max uses
        elif "max_uses" in promo and promo["uses"] >= promo["max_uses"]:
            await message.answer(
                "❌ This promo code has reached its maximum number of uses.\n\n"
                "Please check for other available promotions."
            )
        else:
            # Valid promo code - store in user data
            if user_id in user_database:
                if "active_promos" not in user_database[user_id]:
                    user_database[user_id]["active_promos"] = []
                
                user_database[user_id]["active_promos"].append({
                    "code": promo_code,
                    "discount": promo["discount"],
                    "type": promo["type"],
                    "applied_date": current_date.isoformat()
                })
                
                # Increment usage count
                promo_codes[promo_code]["uses"] += 1
                
                discount_text = f"{promo['discount']}% off" if promo["type"] == "percentage" else f"${promo['discount']} off"
                
                await message.answer(
                    f"✅ Promo code <b>{promo_code}</b> applied successfully!\n\n"
                    f"You will receive {discount_text} on your next purchase.\n\n"
                    f"Return to the main menu to continue shopping."
                )
            else:
                await message.answer("❌ Error applying promo code. Please try again later.")
    else:
        await message.answer(
            "❌ Invalid promo code.\n\n"
            "Please check the code and try again."
        )
    
    await state.clear()
    await show_main_menu(message)

@dp.callback_query(F.data == "change_language")
async def change_language_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle language change request"""
    await state.set_state(Form.language_selection)
    
    kb = InlineKeyboardBuilder()
    for lang_code, lang_name in supported_languages.items():
        kb.button(text=lang_name, callback_data=f"lang_{lang_code}")
    
    create_back_button(kb)
    kb.adjust(1)
    
    await call.message.edit_text(
        "🌐 <b>Language Selection</b>\n\n"
        "Please select your preferred language:",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("lang_"))
async def language_selected(call: CallbackQuery, state: FSMContext) -> None:
    """Handle language selection"""
    lang_code = call.data.split("_")[1]
    user_id = call.from_user.id
    
    if lang_code in supported_languages:
        # Update user's language preference
        if user_id in user_database:
            user_database[user_id]["language"] = lang_code
            
            await call.answer(f"Language set to {supported_languages[lang_code]}")
            await state.clear()
            await call.message.edit_text(translations[lang_code].get("welcome", "Choose an option below:"))
            await show_main_menu(call.message)
        else:
            await call.answer("Error setting language. Please try again.")
            await back_to_main(call, state)
    else:
        await call.answer("Invalid language selection. Please try again.")
        await back_to_main(call, state)

@dp.callback_query(F.data.startswith("resume_payment_"))
async def resume_payment_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle payment resumption"""
    transaction_id = call.data.split("_")[2]
    
    # Find the transaction in pending payments
    if transaction_id in pending_payments:
        payment_data = pending_payments[transaction_id]
        
        # Restore state data
        await state.update_data(
            service_type=payment_data["service_type"],
            price=payment_data["price"]
        )
        
        # Show payment options again
        await show_payment_types(call)
    else:
        await call.answer("This payment session has expired. Please start a new order.")
        await back_to_main(call, state)

@dp.callback_query(F.data.startswith("cancel_payment_"))
async def cancel_payment_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle payment cancellation"""
    transaction_id = call.data.split("_")[2]
    user_id = call.from_user.id
    
    # Find and update the transaction
    if transaction_id in pending_payments:
        # Remove from pending payments
        del pending_payments[transaction_id]
        
        # Update transaction history
        for transaction in transaction_history:
            if transaction.get("transaction_id") == transaction_id:
                transaction["status"] = "cancelled"
                break
        
        # Update user purchases
        if user_id in user_database:
            for purchase in user_database[user_id]["purchases"]:
                if purchase.get("status") == "pending":
                    purchase["status"] = "cancelled"
                    break
        
        await call.message.edit_text(
            "✅ Your order has been cancelled.\n\n"
            "You can start a new order from the main menu whenever you're ready."
        )
        
        # Show main menu
        await asyncio.sleep(2)
        await show_main_menu(call.message)
    else:
        await call.answer("This order has already been processed or expired.")
        await back_to_main(call, state)

@dp.callback_query(F.data.startswith("renew_"))
async def renew_subscription_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle subscription renewal"""
    service_type = call.data.split("_")[1]
    
    # Set up state for renewal
    await state.update_data(
        service_type="renewal",
        renewal_service=service_type
    )
    
    # Show payment options
    await show_payment_types(call)

@dp.callback_query(F.data.startswith("auto_renew_"))
async def auto_renew_handler(call: CallbackQuery) -> None:
    """Handle auto-renewal toggle"""
    service_type = call.data.split("_")[2]
    user_id = call.from_user.id
    
    if user_id in user_database:
        for purchase in user_database[user_id]["purchases"]:
            if purchase.get("service") == "group" and service_type in purchase.get("group_name", ""):
                # Toggle auto-renew setting
                purchase["auto_renew"] = not purchase.get("auto_renew", False)
                
                status = "enabled" if purchase.get("auto_renew", False) else "disabled"
                
                await call.message.edit_text(
                    f"✅ Auto-renewal for your {service_type} subscription has been {status}.\n\n"
                    f"Your subscription will {'' if status == 'enabled' else 'not '}automatically renew when it expires."
                )
                return
    
    await call.answer("Subscription not found or already expired.")
    await back_to_main(call, state)


@dp.callback_query(F.data == "help")
async def help_handler(call: CallbackQuery) -> None:
    """Handle help button"""
    help_text = (
        "<b>How to use this bot:</b>\n\n"
        "1. Select a service from the main menu\n"
        "2. Choose your desired options\n"
        "3. Select a payment type (Indian, International, or Crypto)\n"
        "4. Choose a payment method\n"
        "5. Follow the payment instructions\n"
        "6. Contact @YG_JOIN after payment\n\n"
        "For any questions or issues, contact @YG_JOIN")

    kb = InlineKeyboardBuilder()
    create_back_button(kb)

    await call.message.edit_text(help_text, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "feedback")
async def feedback_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle feedback button"""
    await state.set_state(Form.feedback)

    kb = InlineKeyboardBuilder()
    create_back_button(kb)

    await call.message.edit_text(
        "Please share your feedback or suggestions. Type your message now:",
        reply_markup=kb.as_markup())


@dp.message(Form.feedback)
async def process_feedback(message: Message, state: FSMContext) -> None:
    """Process user feedback"""
    feedback_text = message.text
    user_id = message.from_user.id
    user_name = message.from_user.username or message.from_user.first_name

    # Store feedback
    if user_id in user_database:
        if "feedback" not in user_database[user_id]:
            user_database[user_id]["feedback"] = []

        user_database[user_id]["feedback"].append({
            "text":
            feedback_text,
            "date":
            datetime.now().isoformat()
        })

    # Notify admins about feedback (in a real app, you'd send to all admins)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"<b>New Feedback from {user_name} (ID: {user_id}):</b>\n\n{feedback_text}"
            )
        except Exception as e:
            logger.error(f"Failed to send feedback to admin {admin_id}: {e}")

    await message.answer(
        "Thank you for your feedback! We appreciate your input.")
    await state.clear()
    await show_main_menu(message)


# Admin functions
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(call: CallbackQuery) -> None:
    """Show admin statistics"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return

    total_users = len(user_database)
    total_transactions = len(transaction_history)

    # Calculate revenue
    pending_revenue = sum(float(t["amount"]) for t in transaction_history if t["status"] == "pending")
    completed_revenue = sum(float(t["amount"]) for t in transaction_history if t["status"] == "completed")
    total_revenue = pending_revenue + completed_revenue

    stats = (f"<b>Bot Statistics</b>\n\n"
             f"Total Users: {total_users}\n"
             f"Total Transactions: {total_transactions}\n"
             f"Pending Revenue: ${pending_revenue}\n"
             f"Completed Revenue: ${completed_revenue}\n"
             f"Total Revenue: ${total_revenue}\n\n"
             f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    kb = InlineKeyboardBuilder()
    kb.button(text="« Back to Admin Panel", callback_data="back_to_admin")

    await call.message.edit_text(stats, reply_markup=kb.as_markup())


@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle admin broadcast preparation"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return

    await state.set_state(Form.admin_broadcast)

    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_admin")

    await call.message.edit_text(
        "Please enter the message you want to broadcast to all users:",
        reply_markup=kb.as_markup())


@dp.message(Form.admin_broadcast)
async def process_admin_broadcast(message: Message, state: FSMContext) -> None:
    """Process admin broadcast message"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Unauthorized access")
        return

    broadcast_message = message.text
    sent_count = 0
    failed_count = 0

    # Send message to all users
    for user_id in user_database:
        try:
            await bot.send_message(
                user_id,
                f"<b>📢 Announcement</b>\n\n{broadcast_message}\n\n<i>To see available services, type /start</i>"
            )
            sent_count += 1
        except Exception as e:
            logger.error(f"Failed to send broadcast to user {user_id}: {e}")
            failed_count += 1

    await message.answer(f"Broadcast complete!\n\nSent to: {sent_count} users\nFailed: {failed_count} users")
    await state.clear()


@dp.callback_query(F.data == "admin_custom_price")
async def admin_custom_price(call: CallbackQuery, state: FSMContext) -> None:
    """Handle admin custom price quote"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return

    await state.set_state(Form.custom_price)

    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_admin")

    await call.message.edit_text(
        "Enter the custom price quote in format:\n"
        "<user_id> <service_description> <price>\n\n"
        "Example: 123456789 Premium Video Call 40",
        reply_markup=kb.as_markup())


@dp.message(Form.custom_price)
async def process_custom_price(message: Message, state: FSMContext) -> None:
    """Process admin custom price quote"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Unauthorized access")
        return

    try:
        parts = message.text.split()
        user_id = int(parts[0])
        price = parts[-1]
        service = ' '.join(parts[1:-1])

        # Create custom keyboard for payment type selection
        kb = InlineKeyboardBuilder()
        kb.button(text="🇮🇳 Indian Payment",
                  callback_data="payment_type_indian")
        kb.button(text="🌍 International Payment",
                  callback_data="payment_type_international")
        kb.button(text="💰 Crypto Payment", callback_data="payment_type_crypto")
        kb.adjust(1)

        await bot.send_message(user_id, f"<b>Custom Price Quote</b>\n\n"
                               f"Service: {service}\n"
                               f"Price: ${price}\n\n"
                               f"Please select a payment type:",
                               reply_markup=kb.as_markup())

        await message.answer(f"Custom price quote sent to user {user_id}")
    except Exception as e:
        await message.answer(f"Error sending custom price quote: {e}")

    await state.clear()


@dp.callback_query(F.data == "admin_schedule_broadcast")
async def admin_schedule_broadcast(call: CallbackQuery, state: FSMContext) -> None:
    """Handle scheduled broadcast setup"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    await state.set_state(Form.admin_scheduled_broadcast)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_admin")
    
    await call.message.edit_text(
        "Please enter the scheduled broadcast in the format:\n"
        "<message>|<YYYY-MM-DD HH:MM>\n\n"
        "Example: Hello everyone! New offers available!|2023-12-25 10:00",
        reply_markup=kb.as_markup()
    )

@dp.message(Form.admin_scheduled_broadcast)
async def process_scheduled_broadcast(message: Message, state: FSMContext) -> None:
    """Process scheduled broadcast setup"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Unauthorized access")
        return
    
    try:
        parts = message.text.split("|")
        if len(parts) != 2:
            raise ValueError("Invalid format")
        
        broadcast_message = parts[0].strip()
        scheduled_time = datetime.strptime(parts[1].strip(), "%Y-%m-%d %H:%M")
        
        if scheduled_time <= datetime.now():
            await message.answer("Scheduled time must be in the future.")
            return
        
        # Generate a unique task ID
        task_id = f"broadcast_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Store the scheduled task
        scheduled_tasks[task_id] = {
            "type": "broadcast",
            "message": broadcast_message,
            "scheduled_time": scheduled_time,
            "created_by": message.from_user.id,
            "created_at": datetime.now().isoformat()
        }
        
        await message.answer(
            f"✅ Broadcast scheduled successfully!\n\n"
            f"Message: {broadcast_message}\n"
            f"Scheduled for: {scheduled_time.strftime('%Y-%m-%d %H:%M')}"
        )
    except ValueError as e:
        await message.answer(f"Error: {str(e)}\n\nPlease use the correct format.")
    except Exception as e:
        await message.answer(f"An error occurred: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "admin_create_promo")
async def admin_create_promo(call: CallbackQuery, state: FSMContext) -> None:
    """Handle promo code creation"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    await state.set_state(Form.admin_promo_create)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_admin")
    
    await call.message.edit_text(
        "Please enter the promo code details in the format:\n"
        "<CODE>|<discount>|<type>|<expiry_date>|<max_uses>\n\n"
        "Example: SUMMER25|25|percentage|2023-12-31|100\n"
        "Example: WELCOME10|10|amount|2023-12-31|50\n\n"
        "Types: 'percentage' or 'amount'\n"
        "Expiry date format: YYYY-MM-DD",
        reply_markup=kb.as_markup()
    )

@dp.message(Form.admin_promo_create)
async def process_promo_creation(message: Message, state: FSMContext) -> None:
    """Process promo code creation"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Unauthorized access")
        return
    
    try:
        parts = message.text.split("|")
        if len(parts) != 5:
            raise ValueError("Invalid format")
        
        code = parts[0].strip().upper()
        discount = float(parts[1].strip())
        discount_type = parts[2].strip().lower()
        expiry_date = parts[3].strip()
        max_uses = int(parts[4].strip())
        
        if discount_type not in ["percentage", "amount"]:
            raise ValueError("Type must be 'percentage' or 'amount'")
        
        # Validate expiry date
        datetime.strptime(expiry_date, "%Y-%m-%d")
        
        # Create the promo code
        promo_codes[code] = {
            "discount": discount,
            "type": discount_type,
            "expires": f"{expiry_date}T23:59:59",
            "uses": 0,
            "max_uses": max_uses,
            "created_by": message.from_user.id,
            "created_at": datetime.now().isoformat()
        }
        
        await message.answer(
            f"✅ Promo code created successfully!\n\n"
            f"Code: {code}\n"
            f"Discount: {discount}{'%' if discount_type == 'percentage' else '$'}\n"
            f"Expires: {expiry_date}\n"
            f"Max uses: {max_uses}"
        )
    except ValueError as e:
        await message.answer(f"Error: {str(e)}\n\nPlease use the correct format.")
    except Exception as e:
        await message.answer(f"An error occurred: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "admin_manage_bundles")
async def admin_manage_bundles(call: CallbackQuery, state: FSMContext) -> None:
    """Handle bundle management"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    await state.set_state(Form.admin_bundle_create)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_admin")
    
    await call.message.edit_text(
        "Please enter the bundle details in the format:\n"
        "<name>|<description>|<original_price>|<bundle_price>\n\n"
        "Example: Ultimate Package|3 Months Group + Master Album|120|95\n\n"
        "After creating the bundle, you'll need to add items to it separately.",
        reply_markup=kb.as_markup()
    )

@dp.message(Form.admin_bundle_create)
async def process_bundle_creation(message: Message, state: FSMContext) -> None:
    """Process bundle creation"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Unauthorized access")
        return
    
    try:
        parts = message.text.split("|")
        if len(parts) != 4:
            raise ValueError("Invalid format")
        
        name = parts[0].strip()
        description = parts[1].strip()
        original_price = float(parts[2].strip())
        bundle_price = float(parts[3].strip())
        
        if bundle_price >= original_price:
            await message.answer("Bundle price should be less than original price.")
            return
        
        # Calculate discount percentage
        discount_percentage = round(((original_price - bundle_price) / original_price) * 100)
        
        # Generate a unique bundle ID
        bundle_id = f"bundle_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Create the bundle
        new_bundle = {
            "id": bundle_id,
            "name": name,
            "description": description,
            "items": [],  # Empty initially
            "original_price": original_price,
            "bundle_price": bundle_price,
            "discount_percentage": discount_percentage,
            "created_by": message.from_user.id,
            "created_at": datetime.now().isoformat()
        }
        
        bundle_packages.append(new_bundle)
        
        await message.answer(
            f"✅ Bundle created successfully!\n\n"
            f"Name: {name}\n"
            f"Description: {description}\n"
            f"Original Price: ${original_price}\n"
            f"Bundle Price: ${bundle_price}\n"
            f"Discount: {discount_percentage}%\n\n"
            f"To add items to this bundle, use the format:\n"
            f"{bundle_id}|group|Exclusive|2 Months\n"
            f"or\n"
            f"{bundle_id}|album|Master Album (All-in-One)"
        )
    except ValueError as e:
        await message.answer(f"Error: {str(e)}\n\nPlease use the correct format.")
    except Exception as e:
        await message.answer(f"An error occurred: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "admin_create_offer")
async def admin_create_offer(call: CallbackQuery, state: FSMContext) -> None:
    """Handle limited time offer creation"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="back_to_admin")
    
    await call.message.edit_text(
        "Please enter the limited time offer details in the format:\n"
        "<name>|<discount>|<expiry_date>\n\n"
        "Example: Summer Sale|15|2023-08-31\n\n"
        "Discount is in percentage\n"
        "Expiry date format: YYYY-MM-DD",
        reply_markup=kb.as_markup()
    )

@dp.message(lambda message: message.text and "|" in message.text and message.from_user.id in ADMIN_IDS)
async def process_offer_creation(message: Message) -> None:
    """Process limited time offer creation"""
    # Check if this might be an offer creation message
    if len(message.text.split("|")) == 3:
        try:
            parts = message.text.split("|")
            name = parts[0].strip()
            discount = float(parts[1].strip())
            expiry_date = parts[2].strip()
            
            # Validate expiry date
            datetime.strptime(expiry_date, "%Y-%m-%d")
            
            # Generate a unique offer ID
            offer_id = f"offer_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            # Create the offer
            new_offer = {
                "id": offer_id,
                "name": name,
                "discount": discount,
                "type": "percentage",
                "expires": f"{expiry_date}T23:59:59",
                "created_by": message.from_user.id,
                "created_at": datetime.now().isoformat()
            }
            
            limited_time_offers.append(new_offer)
            
            await message.answer(
                f"✅ Limited time offer created successfully!\n\n"
                f"Name: {name}\n"
                f"Discount: {discount}%\n"
                f"Expires: {expiry_date}"
            )
            return
        except Exception:
            # Not an offer creation message, continue with other handlers
            pass

@dp.callback_query(F.data == "admin_advanced_analytics")
async def admin_advanced_analytics(call: CallbackQuery) -> None:
    """Handle advanced analytics display"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    # Calculate various metrics
    total_users = len(user_database)
    total_transactions = len(transaction_history)
    
    # Revenue metrics
    pending_revenue = sum(float(t["amount"]) for t in transaction_history if t["status"] == "pending")
    completed_revenue = sum(float(t["amount"]) for t in transaction_history if t["status"] == "completed")
    total_revenue = pending_revenue + completed_revenue
    
    # Conversion metrics
    abandoned_count = sum(1 for t in transaction_history if t["status"] == "cancelled")
    completed_count = sum(1 for t in transaction_history if t["status"] == "completed")
    
    conversion_rate = 0
    if total_transactions > 0:
        conversion_rate = (completed_count / total_transactions) * 100
    
    # Service popularity
    service_counts = {}
    for t in transaction_history:
        service = t.get("service", "unknown")
        if service not in service_counts:
            service_counts[service] = 0
        service_counts[service] += 1
    
    # Format service popularity
    popularity_text = ""
    for service, count in sorted(service_counts.items(), key=lambda x: x[1], reverse=True):
        popularity_text += f"{service.capitalize()}: {count} orders\n"
    
    # Promo code usage
    promo_usage = {}
    for code, data in promo_codes.items():
        promo_usage[code] = data.get("uses", 0)
    
    # Format promo usage
    promo_text = ""
    for code, uses in sorted(promo_usage.items(), key=lambda x: x[1], reverse=True):
        if uses > 0:
            promo_text += f"{code}: {uses} uses\n"
    
    if not promo_text:
        promo_text = "No promo codes used yet"
    
    analytics_text = (
        f"<b>📊 Advanced Analytics</b>\n\n"
        f"<b>User Metrics:</b>\n"
        f"Total Users: {total_users}\n"
        f"Total Transactions: {total_transactions}\n\n"
        
        f"<b>Revenue Metrics:</b>\n"
        f"Pending Revenue: ${pending_revenue}\n"
        f"Completed Revenue: ${completed_revenue}\n"
        f"Total Revenue: ${total_revenue}\n\n"
        
        f"<b>Conversion Metrics:</b>\n"
        f"Completed Orders: {completed_count}\n"
        f"Abandoned Orders: {abandoned_count}\n"
        f"Conversion Rate: {conversion_rate:.2f}%\n\n"
        
        f"<b>Service Popularity:</b>\n{popularity_text}\n"
        f"<b>Promo Code Usage:</b>\n{promo_text}\n\n"
        
        f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    kb = InlineKeyboardBuilder()
    kb.button(text="« Back to Admin Panel", callback_data="back_to_admin")
    
    await call.message.edit_text(analytics_text, reply_markup=kb.as_markup())

@dp.callback_query(F.data == "admin_manage_services")
async def admin_manage_services(call: CallbackQuery, state: FSMContext) -> None:
    """Handle service management"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    await state.set_state(Form.admin_service_edit)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Add Video Call Duration", callback_data="add_service_video_call")
    kb.button(text="Add Group Option", callback_data="add_service_group")
    kb.button(text="Add Chat Option", callback_data="add_service_chat")
    kb.button(text="Add Album Option", callback_data="add_service_album")
    kb.button(text="« Back to Admin Panel", callback_data="back_to_admin")
    kb.adjust(1)
    
    await call.message.edit_text(
        "<b>🛠️ Manage Services</b>\n\n"
        "Select an option to add or modify services:",
        reply_markup=kb.as_markup()
    )

@dp.callback_query(F.data.startswith("add_service_"))
async def add_service_handler(call: CallbackQuery, state: FSMContext) -> None:
    """Handle adding a new service option"""
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Unauthorized access")
        return
    
    service_type = call.data.split("_")[2]
    await state.update_data(edit_service_type=service_type)
    
    kb = InlineKeyboardBuilder()
    kb.button(text="Cancel", callback_data="admin_manage_services")
    
    instructions = ""
    if service_type == "video_call":
        instructions = "Please enter the new video call duration and price in the format:\n<duration>|<price>\nExample: 45 min|20"
    elif service_type == "group":
        instructions = "Please enter the new group option in the format:\n<group_name>\nExample: VIP Group"
    elif service_type == "chat":
        instructions = "Please enter the new chat option in the format:\n<type>|<price>\nExample: Premium Chat|45"
    elif service_type == "album":
        instructions = "Please enter the new album option in the format:\n<name>|<price>\nExample: Special Collection (100+ pics)|40"
    
    await call.message.edit_text(instructions, reply_markup=kb.as_markup())

@dp.message(Form.admin_service_edit)
async def process_service_edit(message: Message, state: FSMContext) -> None:
    """Process service edit/addition"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Unauthorized access")
        return
    
    user_data = await state.get_data()
    service_type = user_data.get("edit_service_type")
    
    try:
        if service_type == "video_call":
            parts = message.text.split("|")
            if len(parts) != 2:
                raise ValueError("Invalid format")
            
            duration = parts[0].strip()
            price = float(parts[1].strip())
            
            service_options["video_call"]["durations"].append({
                "name": duration,
                "price": price
            })
            
            await message.answer(f"✅ Added new video call duration: {duration} - ${price}")
            
        elif service_type == "group":
            group_name = message.text.strip()
            
            if group_name not in service_options["group"]["names"]:
                service_options["group"]["names"].append(group_name)
                await message.answer(f"✅ Added new group option: {group_name}")
            else:
                await message.answer(f"Group '{group_name}' already exists.")
            
        elif service_type == "chat":
            parts = message.text.split("|")
            if len(parts) != 2:
                raise ValueError("Invalid format")
            
            chat_type = parts[0].strip()
            price = float(parts[1].strip())
            
            service_options["private_chat"]["types"].append({
                "name": chat_type,
                "price": price
            })
            
            await message.answer(f"✅ Added new chat type: {chat_type} - ${price}")
            
        elif service_type == "album":
            parts = message.text.split("|")
            if len(parts) != 2:
                raise ValueError("Invalid format")
            
            album_name = parts[0].strip()
            price = float(parts[1].strip())
            
            service_options["album"].append({
                "name": album_name,
                "price": price
            })
            
            await message.answer(f"✅ Added new album option: {album_name} - ${price}")
            
    except ValueError as e:
        await message.answer(f"Error: {str(e)}\n\nPlease use the correct format.")
    except Exception as e:
        await message.answer(f"An error occurred: {str(e)}")
    
    await state.clear()

@dp.callback_query(F.data == "back_to_admin")
async def back_to_admin(call: CallbackQuery, state: FSMContext) -> None:
    """Handle back to admin panel button"""
    await state.clear()
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 User Statistics", callback_data="admin_stats")
    kb.button(text="📣 Broadcast Message", callback_data="admin_broadcast")
    kb.button(text="📅 Schedule Broadcast", callback_data="admin_schedule_broadcast")
    kb.button(text="💰 Custom Price Quote", callback_data="admin_custom_price")
    kb.button(text="🛠️ Manage Services", callback_data="admin_manage_services")
    kb.button(text="💲 Manage Pricing", callback_data="admin_manage_pricing")
    kb.button(text="🎁 Create Promo Code", callback_data="admin_create_promo")
    kb.button(text="📦 Manage Bundles", callback_data="admin_manage_bundles")
    kb.button(text="⏱️ Create Limited Offer", callback_data="admin_create_offer")
    kb.button(text="📊 Advanced Analytics", callback_data="admin_advanced_analytics")
    kb.adjust(1)
    await call.message.edit_text(
        "<b>Admin Panel</b>\nWelcome to the admin panel.",
        reply_markup=kb.as_markup())


# Error handler
@dp.error()
async def error_handler(update, exception) -> None:
    """Handle errors"""
    logger.error(f"Update {update} caused error {exception}")

    try:
        if isinstance(update, Message):
            await update.answer(
                "An error occurred while processing your request. Please try again later or contact support."
            )
        elif isinstance(update, CallbackQuery):
            await update.message.answer(
                "An error occurred while processing your request. Please try again later or contact support."
            )
    except Exception as e:
        logger.error(f"Error in error handler: {e}")


async def send_payment_reminder(user_id: int, service_type: str, price: str, transaction_id: str) -> None:
    """Send a reminder to users who abandoned the payment process"""
    try:
        kb = InlineKeyboardBuilder()
        kb.button(text="Resume Payment", callback_data=f"resume_payment_{transaction_id}")
        kb.button(text="Cancel Order", callback_data=f"cancel_payment_{transaction_id}")
        kb.adjust(1)
        
        await bot.send_message(
            user_id,
            f"📢 <b>Payment Reminder</b>\n\n"
            f"You have an incomplete order for {service_type} (${price}).\n\n"
            f"Would you like to complete your payment or cancel this order?",
            reply_markup=kb.as_markup()
        )
        logger.info(f"Payment reminder sent to user {user_id} for transaction {transaction_id}")
    except Exception as e:
        logger.error(f"Failed to send payment reminder to user {user_id}: {e}")

async def send_subscription_renewal_reminder(user_id: int, service_type: str, expiry_date: str) -> None:
    """Send a reminder for subscription renewal"""
    try:
        kb = InlineKeyboardBuilder()
        kb.button(text="Renew Subscription", callback_data=f"renew_{service_type}")
        kb.button(text="Auto-Renew", callback_data=f"auto_renew_{service_type}")
        kb.adjust(1)
        
        await bot.send_message(
            user_id,
            f"📅 <b>Subscription Reminder</b>\n\n"
            f"Your {service_type} subscription will expire on {expiry_date}.\n\n"
            f"Would you like to renew your subscription?",
            reply_markup=kb.as_markup()
        )
        logger.info(f"Renewal reminder sent to user {user_id} for {service_type}")
    except Exception as e:
        logger.error(f"Failed to send renewal reminder to user {user_id}: {e}")

async def check_pending_payments() -> None:
    """Check for abandoned payments and send reminders"""
    current_time = datetime.now()
    to_remove = []
    
    for transaction_id, data in pending_payments.items():
        time_diff = current_time - data["timestamp"]
        
        # Send first reminder after 30 minutes
        if time_diff > timedelta(minutes=30) and not data.get("reminder_1_sent"):
            await send_payment_reminder(data["user_id"], data["service_type"], data["price"], transaction_id)
            pending_payments[transaction_id]["reminder_1_sent"] = True
        
        # Send second reminder after 4 hours
        elif time_diff > timedelta(hours=4) and not data.get("reminder_2_sent"):
            await send_payment_reminder(data["user_id"], data["service_type"], data["price"], transaction_id)
            pending_payments[transaction_id]["reminder_2_sent"] = True
        
        # Send final reminder after 24 hours
        elif time_diff > timedelta(hours=24) and not data.get("reminder_3_sent"):
            await send_payment_reminder(data["user_id"], data["service_type"], data["price"], transaction_id)
            pending_payments[transaction_id]["reminder_3_sent"] = True
            
        # Remove from tracking after 48 hours
        if time_diff > timedelta(hours=48):
            to_remove.append(transaction_id)
    
    # Remove old transactions
    for transaction_id in to_remove:
        del pending_payments[transaction_id]

async def check_subscription_renewals() -> None:
    """Check for expiring subscriptions and send renewal reminders"""
    current_date = datetime.now().date()
    
    for user_id, user_data in user_database.items():
        for purchase in user_data.get("purchases", []):
            if purchase.get("service") == "group" and purchase.get("status") == "completed":
                # Check if expiry date exists
                if "expiry_date" in purchase:
                    expiry_date = datetime.fromisoformat(purchase["expiry_date"]).date()
                    days_until_expiry = (expiry_date - current_date).days
                    
                    # Send reminder 7 days before expiry
                    if days_until_expiry == 7 and not purchase.get("renewal_reminder_sent"):
                        await send_subscription_renewal_reminder(
                            user_id, 
                            f"{purchase.get('group_name', 'Group')} subscription", 
                            expiry_date.strftime("%Y-%m-%d")
                        )
                        purchase["renewal_reminder_sent"] = True
                    
                    # Send final reminder 1 day before expiry
                    elif days_until_expiry == 1 and not purchase.get("final_reminder_sent"):
                        await send_subscription_renewal_reminder(
                            user_id, 
                            f"{purchase.get('group_name', 'Group')} subscription", 
                            expiry_date.strftime("%Y-%m-%d")
                        )
                        purchase["final_reminder_sent"] = True
                    
                    # Process auto-renewal if enabled
                    elif days_until_expiry == 0 and purchase.get("auto_renew"):
                        # Logic for auto-renewal would go here
                        # This would typically involve charging the user and extending their subscription
                        logger.info(f"Auto-renewal triggered for user {user_id}")

async def background_tasks() -> None:
    """Run background tasks periodically"""
    while True:
        try:
            await check_pending_payments()
            await check_subscription_renewals()
            
            # Process scheduled broadcasts
            current_time = datetime.now()
            to_remove = []
            
            for task_id, task in scheduled_tasks.items():
                if task["type"] == "broadcast" and current_time >= task["scheduled_time"]:
                    # Execute the scheduled broadcast
                    sent_count = 0
                    failed_count = 0
                    
                    for user_id in user_database:
                        try:
                            await bot.send_message(
                                user_id,
                                f"<b>📢 Scheduled Announcement</b>\n\n{task['message']}\n\n<i>To see available services, type /start</i>"
                            )
                            sent_count += 1
                        except Exception as e:
                            logger.error(f"Failed to send scheduled broadcast to user {user_id}: {e}")
                            failed_count += 1
                    
                    # Notify admin about completion
                    for admin_id in ADMIN_IDS:
                        try:
                            await bot.send_message(
                                admin_id,
                                f"Scheduled broadcast completed!\n\nSent to: {sent_count} users\nFailed: {failed_count} users"
                            )
                        except Exception:
                            pass
                    
                    to_remove.append(task_id)
            
            # Remove completed tasks
            for task_id in to_remove:
                del scheduled_tasks[task_id]
                
            # Run every 5 minutes
            await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Error in background tasks: {e}")
            await asyncio.sleep(60)  # If there's an error, wait a bit before retrying

async def on_startup(bot: Bot) -> None:
    """Actions to perform on startup"""
    logger.info("Bot started")
    # Notify admins that bot is online
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id, f"🟢 Bot is now online\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")
    
    # Start background tasks
    asyncio.create_task(background_tasks())


async def on_shutdown(bot: Bot) -> None:
    """Actions to perform on shutdown"""
    logger.info("Bot shutting down")
    # Notify admins that bot is offline
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id, f"🔴 Bot is shutting down\n"
                f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")


async def main() -> None:
    """Main function to start the bot"""
    # Set up startup and shutdown handlers
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Start polling
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
