# Telegram Mini App Bot with PostgreSQL Database

This bot provides a comprehensive service management and payment system for Telegram, with features like:

- Video call bookings
- Group subscriptions
- Private chat sessions
- Album/content sales
- Bundle packages
- Promo codes and discounts
- Multi-language support
- Payment processing
- Admin dashboard

## PostgreSQL Database Setup

The bot uses PostgreSQL for data persistence, which is ideal for Railway deployment as it:

1. Provides robust data storage and querying capabilities
2. Scales well with growing user base
3. Offers advanced features like JSON storage and full-text search

### How the Database Works

- The database tables are automatically created when the bot starts
- All tables are initialized with the necessary schema
- Default service options are loaded automatically
- No manual database setup is required

## Deployment to Railway

### Prerequisites

1. Create a [Railway](https://railway.app/) account
2. Install the [Railway CLI](https://docs.railway.app/develop/cli)
3. Create a Telegram bot via [BotFather](https://t.me/botfather) and get your API token

### Steps to Deploy

1. **Login to Railway**
   ```
   railway login
   ```

2. **Initialize a new project**
   ```
   railway init
   ```

3. **Add a PostgreSQL database**
   ```
   railway add
   ```
   Select PostgreSQL from the options.

4. **Set environment variables**
   ```
   railway variables set TELEGRAM_API_TOKEN=your_telegram_token_here
   ```

5. **Deploy the bot**
   ```
   railway up
   ```

### Environment Variables

The following environment variables need to be set in Railway:

- `TELEGRAM_API_TOKEN`: Your Telegram bot token
- `DATABASE_URL`: PostgreSQL connection string (automatically set by Railway)

## Local Development

1. Clone the repository
   ```
   git clone https://github.com/yourusername/telegram-mini-app-bot.git
   cd telegram-mini-app-bot
   ```

2. Install dependencies
   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file with the following variables:
   ```
   TELEGRAM_API_TOKEN=your_telegram_token_here
   DATABASE_URL=postgresql://username:password@localhost:5432/database_name
   ```

4. Run the bot
   ```
   python main.py
   ```

## Database Schema

The database includes the following tables:

- `users`: User information and preferences
- `purchases`: Record of all user purchases
- `transactions`: Payment transaction details
- `pending_payments`: Payments awaiting confirmation
- `promo_codes`: Discount codes
- `bundle_packages`: Service bundles
- `bundle_items`: Items included in bundles
- `limited_time_offers`: Time-limited promotions
- `scheduled_tasks`: Scheduled broadcasts and reminders
- `feedback`: User feedback
- `service_options`: Available services and pricing

## Maintenance

- Regularly backup your database using Railway's backup feature
- Monitor logs for any errors or issues
- Update the bot code as needed to add new features or fix bugs

## Security Considerations

- The PostgreSQL database contains sensitive user data
- Ensure proper access controls are in place
- Consider encrypting sensitive fields for additional security
- Railway provides secure database connections by default

## Extending the Database

To add new tables or fields:

1. Update the `initialize_db` method in database.py
2. Add corresponding methods for the new data
3. Update the bot code to use the new database features