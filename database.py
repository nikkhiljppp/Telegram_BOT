import os
import json
import psycopg2
import psycopg2.extras
from datetime import datetime
import logging
from typing import Dict, List, Any, Optional, Union

# Configure logging
logger = logging.getLogger(__name__)

# PostgreSQL connection details from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

class Database:
    def __init__(self):
        """Initialize database connection"""
        self.initialize_db()
    
    def get_connection(self):
        """Get database connection with dictionary cursor"""
        conn = psycopg2.connect(DATABASE_URL)
        return conn
        
    def get_cursor(self, conn):
        """Get a dictionary cursor"""
        return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    def initialize_db(self):
        """Create database tables if they don't exist"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            
            # Users table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_date TIMESTAMP,
                language TEXT DEFAULT 'en',
                last_active TIMESTAMP
            )
            ''')
            
            # Purchases table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                purchase_id TEXT PRIMARY KEY,
                user_id BIGINT,
                service_type TEXT,
                price NUMERIC,
                original_price NUMERIC,
                status TEXT,
                date TIMESTAMP,
                promo_code TEXT,
                discount_amount NUMERIC,
                expiry_date TIMESTAMP,
                renewal_reminder_sent BOOLEAN DEFAULT FALSE,
                final_reminder_sent BOOLEAN DEFAULT FALSE,
                auto_renew BOOLEAN DEFAULT FALSE,
                metadata JSONB DEFAULT '{}',
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            ''')
            
            # Transactions table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                service TEXT,
                amount NUMERIC,
                original_price NUMERIC,
                payment_method TEXT,
                payment_type TEXT,
                status TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                promo_code TEXT,
                discount_amount NUMERIC,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            ''')
            
            # Pending payments table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_payments (
                transaction_id TEXT PRIMARY KEY,
                user_id BIGINT,
                service_type TEXT,
                price NUMERIC,
                timestamp TIMESTAMP,
                reminder_1_sent BOOLEAN DEFAULT FALSE,
                reminder_2_sent BOOLEAN DEFAULT FALSE,
                reminder_3_sent BOOLEAN DEFAULT FALSE,
                payment_confirmed BOOLEAN DEFAULT FALSE,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            ''')
            
            # Promo codes table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                discount NUMERIC,
                type TEXT,
                expires TIMESTAMP,
                uses INTEGER DEFAULT 0,
                max_uses INTEGER,
                created_by BIGINT,
                created_at TIMESTAMP
            )
            ''')
            
            # Bundle packages table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS bundle_packages (
                id TEXT PRIMARY KEY,
                name TEXT,
                description TEXT,
                original_price NUMERIC,
                bundle_price NUMERIC,
                discount_percentage INTEGER,
                created_by BIGINT,
                created_at TIMESTAMP,
                active BOOLEAN DEFAULT TRUE
            )
            ''')
            
            # Bundle items table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS bundle_items (
                id SERIAL PRIMARY KEY,
                bundle_id TEXT,
                service TEXT,
                item_name TEXT,
                duration TEXT,
                metadata JSONB DEFAULT '{}',
                FOREIGN KEY (bundle_id) REFERENCES bundle_packages (id)
            )
            ''')
            
            # Limited time offers table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS limited_time_offers (
                id TEXT PRIMARY KEY,
                name TEXT,
                discount NUMERIC,
                type TEXT,
                expires TIMESTAMP,
                created_by BIGINT,
                created_at TIMESTAMP
            )
            ''')
            
            # Scheduled tasks table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                task_id TEXT PRIMARY KEY,
                type TEXT,
                message TEXT,
                scheduled_time TIMESTAMP,
                created_by BIGINT,
                created_at TIMESTAMP,
                executed BOOLEAN DEFAULT FALSE
            )
            ''')
            
            # Feedback table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS feedback (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                text TEXT,
                date TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
            ''')
            
            # Service options table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_options (
                id SERIAL PRIMARY KEY,
                service_type TEXT,
                option_type TEXT,
                name TEXT,
                price NUMERIC,
                metadata JSONB DEFAULT '{}'
            )
            ''')
            
            # Insert default service options if table is empty
            cursor.execute("SELECT COUNT(*) as count FROM service_options")
            count = cursor.fetchone()["count"]
            
            if count == 0:
                # Insert video call durations
                video_call_durations = [
                    {"name": "15 min", "price": 10},
                    {"name": "30 min", "price": 15},
                    {"name": "60 min", "price": 25}
                ]
                for option in video_call_durations:
                    cursor.execute(
                        "INSERT INTO service_options (service_type, option_type, name, price) VALUES (%s, %s, %s, %s)",
                        ("video_call", "duration", option["name"], option["price"])
                    )
                
                # Insert group names
                group_names = ["Exclusive", "Ankita's Den"]
                for name in group_names:
                    cursor.execute(
                        "INSERT INTO service_options (service_type, option_type, name) VALUES (%s, %s, %s)",
                        ("group", "name", name)
                    )
                
                # Insert group durations
                group_durations = [
                    {"name": "2 Months", "price": 20},
                    {"name": "6 Months", "price": 50},
                    {"name": "12 Months", "price": 90}
                ]
                for option in group_durations:
                    cursor.execute(
                        "INSERT INTO service_options (service_type, option_type, name, price) VALUES (%s, %s, %s, %s)",
                        ("group", "duration", option["name"], option["price"])
                    )
                
                # Insert chat durations
                chat_durations = [
                    {"name": "2 Hr", "price": 20},
                    {"name": "4 Hr", "price": 35}
                ]
                for option in chat_durations:
                    cursor.execute(
                        "INSERT INTO service_options (service_type, option_type, name, price) VALUES (%s, %s, %s, %s)",
                        ("private_chat", "duration", option["name"], option["price"])
                    )
                
                # Insert chat types
                chat_types = [
                    {"name": "Sx Chat with Notes", "price": 60},
                    {"name": "Normal Chat", "price": 35}
                ]
                for option in chat_types:
                    cursor.execute(
                        "INSERT INTO service_options (service_type, option_type, name, price) VALUES (%s, %s, %s, %s)",
                        ("private_chat", "type", option["name"], option["price"])
                    )
                
                # Insert album options
                album_options = [
                    {"name": "Node Pic Full Collection (300+)", "price": 30},
                    {"name": "Node Pic + Vid Full Collection (800+)", "price": 60},
                    {"name": "My Exclusive Bj Vids (50 Vids)", "price": 50},
                    {"name": "Master Album (All-in-One)", "price": 90}
                ]
                for option in album_options:
                    cursor.execute(
                        "INSERT INTO service_options (service_type, option_type, name, price) VALUES (%s, %s, %s, %s)",
                        ("album", "album", option["name"], option["price"])
                    )
            
            # Insert default bundles if table is empty
            cursor.execute("SELECT COUNT(*) as count FROM bundle_packages")
            count = cursor.fetchone()["count"]
            
            if count == 0:
                # Insert default bundles
                bundles = [
                    {
                        "id": "bundle1",
                        "name": "Starter Bundle",
                        "description": "1 Month Group + 1 Album",
                        "original_price": 50,
                        "bundle_price": 40,
                        "discount_percentage": 20,
                        "created_by": 0,
                        "created_at": datetime.now(),
                        "items": [
                            {"service": "group", "item_name": "Exclusive", "duration": "2 Months"},
                            {"service": "album", "item_name": "Node Pic Full Collection (300+)"}
                        ]
                    },
                    {
                        "id": "bundle2",
                        "name": "Premium Bundle",
                        "description": "6 Months Group + Master Album",
                        "original_price": 140,
                        "bundle_price": 110,
                        "discount_percentage": 21,
                        "created_by": 0,
                        "created_at": datetime.now(),
                        "items": [
                            {"service": "group", "item_name": "Ankita's Den", "duration": "6 Months"},
                            {"service": "album", "item_name": "Master Album (All-in-One)"}
                        ]
                    }
                ]
                
                for bundle in bundles:
                    cursor.execute(
                        """INSERT INTO bundle_packages 
                        (id, name, description, original_price, bundle_price, discount_percentage, created_by, created_at) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            bundle["id"], 
                            bundle["name"], 
                            bundle["description"], 
                            bundle["original_price"], 
                            bundle["bundle_price"], 
                            bundle["discount_percentage"], 
                            bundle["created_by"], 
                            bundle["created_at"]
                        )
                    )
                    
                    # Insert bundle items
                    for item in bundle["items"]:
                        metadata = json.dumps({k: v for k, v in item.items() if k not in ["service", "item_name", "duration"]})
                        cursor.execute(
                            """INSERT INTO bundle_items 
                            (bundle_id, service, item_name, duration, metadata) 
                            VALUES (%s, %s, %s, %s, %s)""",
                            (
                                bundle["id"],
                                item["service"],
                                item["item_name"],
                                item.get("duration", ""),
                                metadata
                            )
                        )
            
            conn.commit()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
        finally:
            conn.close()
    
    # User methods
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
            return None
        finally:
            conn.close()
    
    def add_user(self, user_id: int, username: str, first_name: str) -> bool:
        """Add new user or update existing user"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            
            # Check if user exists
            cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
            existing_user = cursor.fetchone()
            
            if existing_user:
                # Update existing user
                cursor.execute(
                    "UPDATE users SET username = %s, first_name = %s, last_active = %s WHERE user_id = %s",
                    (username, first_name, datetime.now(), user_id)
                )
            else:
                # Add new user
                cursor.execute(
                    "INSERT INTO users (user_id, username, first_name, joined_date, last_active) VALUES (%s, %s, %s, %s, %s)",
                    (user_id, username, first_name, datetime.now(), datetime.now())
                )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding/updating user {user_id}: {e}")
            return False
        finally:
            conn.close()
    
    def update_user_language(self, user_id: int, language: str) -> bool:
        """Update user's preferred language"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            cursor.execute(
                "UPDATE users SET language = %s WHERE user_id = %s",
                (language, user_id)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating language for user {user_id}: {e}")
            return False
        finally:
            conn.close()
    
    def get_user_language(self, user_id: int) -> str:
        """Get user's preferred language"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            cursor.execute("SELECT language FROM users WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            return result["language"] if result else "en"
        except Exception as e:
            logger.error(f"Error getting language for user {user_id}: {e}")
            return "en"
        finally:
            conn.close()
    
    def get_all_users(self) -> List[Dict]:
        """Get all users"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            cursor.execute("SELECT * FROM users")
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []
        finally:
            conn.close()
    
    # Purchase methods
    def add_purchase(self, purchase_data: Dict) -> bool:
        """Add new purchase"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            
            # Extract metadata fields
            metadata = {k: v for k, v in purchase_data.items() if k not in [
                "purchase_id", "user_id", "service_type", "price", "original_price", 
                "status", "date", "promo_code", "discount_amount", "expiry_date"
            ]}
            
            # Convert date string to datetime if needed
            date = purchase_data.get("date", datetime.now())
            if isinstance(date, str):
                date = datetime.fromisoformat(date)
                
            # Convert expiry_date string to datetime if needed
            expiry_date = purchase_data.get("expiry_date")
            if isinstance(expiry_date, str) and expiry_date:
                expiry_date = datetime.fromisoformat(expiry_date)
            
            cursor.execute(
                """INSERT INTO purchases 
                (purchase_id, user_id, service_type, price, original_price, status, date, 
                promo_code, discount_amount, expiry_date, metadata) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    purchase_data["purchase_id"],
                    purchase_data["user_id"],
                    purchase_data["service_type"],
                    purchase_data["price"],
                    purchase_data.get("original_price", purchase_data["price"]),
                    purchase_data.get("status", "pending"),
                    date,
                    purchase_data.get("promo_code", None),
                    purchase_data.get("discount_amount", 0),
                    expiry_date,
                    json.dumps(metadata)
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding purchase: {e}")
            return False
        finally:
            conn.close()
    
    def update_purchase_status(self, purchase_id: str, status: str) -> bool:
        """Update purchase status"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            cursor.execute(
                "UPDATE purchases SET status = %s WHERE purchase_id = %s",
                (status, purchase_id)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating purchase status: {e}")
            return False
        finally:
            conn.close()
    
    def get_user_purchases(self, user_id: int) -> List[Dict]:
        """Get all purchases for a user"""
        try:
            conn = self.get_connection()
            cursor = self.get_cursor(conn)
            cursor.execute("SELECT * FROM purchases WHERE user_id = %s ORDER BY date DESC", (user_id,))
            purchases = cursor.fetchall()
            
            # Parse metadata JSON
            for purchase in purchases:
                if "metadata" in purchase and purchase["metadata"]:
                    try:
                        metadata = json.loads(purchase["metadata"])
                        purchase.update(metadata)
                    except:
                        pass
                purchase.pop("metadata", None)
            
            return purchases
        except Exception as e:
            logger.error(f"Error getting purchases for user {user_id}: {e}")
            return []
        finally:
            conn.close()
    
    def get_expiring_subscriptions(self, days_threshold: int) -> List[Dict]:
        """Get subscriptions expiring within the given days threshold"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Calculate date threshold
            today = datetime.now().date()
            target_date = (today + datetime.timedelta(days=days_threshold)).isoformat()
            
            cursor.execute(
                """SELECT p.*, u.user_id, u.username, u.language 
                FROM purchases p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.service_type = 'group' 
                AND p.status = 'completed'
                AND p.expiry_date <= ?
                AND p.expiry_date >= ?
                """, 
                (target_date, today.isoformat())
            )
            
            subscriptions = cursor.fetchall()
            
            # Parse metadata JSON
            for subscription in subscriptions:
                if "metadata" in subscription and subscription["metadata"]:
                    try:
                        metadata = json.loads(subscription["metadata"])
                        subscription.update(metadata)
                    except:
                        pass
                subscription.pop("metadata", None)
            
            return subscriptions
        except Exception as e:
            logger.error(f"Error getting expiring subscriptions: {e}")
            return []
        finally:
            conn.close()
    
    # Transaction methods
    def add_transaction(self, transaction_data: Dict) -> bool:
        """Add new transaction"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """INSERT INTO transactions 
                (transaction_id, user_id, username, service, amount, original_price, 
                payment_method, payment_type, status, created_at, updated_at, promo_code, discount_amount) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    transaction_data["transaction_id"],
                    transaction_data["user_id"],
                    transaction_data["username"],
                    transaction_data["service"],
                    transaction_data["amount"],
                    transaction_data.get("original_price", transaction_data["amount"]),
                    transaction_data["payment_method"],
                    transaction_data["payment_type"],
                    transaction_data["status"],
                    transaction_data["created_at"],
                    datetime.now().isoformat(),
                    transaction_data.get("promo_code", None),
                    transaction_data.get("discount_amount", 0)
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding transaction: {e}")
            return False
        finally:
            conn.close()
    
    def update_transaction_status(self, transaction_id: str, status: str) -> bool:
        """Update transaction status"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE transactions SET status = ?, updated_at = ? WHERE transaction_id = ?",
                (status, datetime.now().isoformat(), transaction_id)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating transaction status: {e}")
            return False
        finally:
            conn.close()
    
    def get_transaction(self, transaction_id: str) -> Optional[Dict]:
        """Get transaction by ID"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,))
            return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting transaction {transaction_id}: {e}")
            return None
        finally:
            conn.close()
    
    def get_all_transactions(self) -> List[Dict]:
        """Get all transactions"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM transactions ORDER BY created_at DESC")
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting all transactions: {e}")
            return []
        finally:
            conn.close()
    
    # Pending payment methods
    def add_pending_payment(self, payment_data: Dict) -> bool:
        """Add new pending payment"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """INSERT INTO pending_payments 
                (transaction_id, user_id, service_type, price, timestamp) 
                VALUES (?, ?, ?, ?, ?)""",
                (
                    payment_data["transaction_id"],
                    payment_data["user_id"],
                    payment_data["service_type"],
                    payment_data["price"],
                    payment_data["timestamp"].isoformat() if isinstance(payment_data["timestamp"], datetime) else payment_data["timestamp"]
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding pending payment: {e}")
            return False
        finally:
            conn.close()
    
    def update_pending_payment(self, transaction_id: str, updates: Dict) -> bool:
        """Update pending payment fields"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
            params = list(updates.values()) + [transaction_id]
            
            cursor.execute(
                f"UPDATE pending_payments SET {set_clause} WHERE transaction_id = ?",
                params
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating pending payment {transaction_id}: {e}")
            return False
        finally:
            conn.close()
    
    def delete_pending_payment(self, transaction_id: str) -> bool:
        """Delete pending payment"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM pending_payments WHERE transaction_id = ?", (transaction_id,))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error deleting pending payment {transaction_id}: {e}")
            return False
        finally:
            conn.close()
    
    def get_pending_payments_for_reminders(self) -> List[Dict]:
        """Get pending payments that need reminders"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all pending payments
            cursor.execute(
                """SELECT p.*, u.user_id, u.username, u.language 
                FROM pending_payments p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.payment_confirmed = 0
                """
            )
            
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting pending payments for reminders: {e}")
            return []
        finally:
            conn.close()
    
    # Promo code methods
    def add_promo_code(self, promo_data: Dict) -> bool:
        """Add new promo code"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """INSERT INTO promo_codes 
                (code, discount, type, expires, max_uses, created_by, created_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    promo_data["code"],
                    promo_data["discount"],
                    promo_data["type"],
                    promo_data["expires"],
                    promo_data["max_uses"],
                    promo_data["created_by"],
                    promo_data["created_at"]
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding promo code: {e}")
            return False
        finally:
            conn.close()
    
    def get_promo_code(self, code: str) -> Optional[Dict]:
        """Get promo code by code"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM promo_codes WHERE code = ?", (code,))
            return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting promo code {code}: {e}")
            return None
        finally:
            conn.close()
    
    def increment_promo_usage(self, code: str) -> bool:
        """Increment promo code usage count"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE promo_codes SET uses = uses + 1 WHERE code = ?",
                (code,)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error incrementing promo code usage {code}: {e}")
            return False
        finally:
            conn.close()
    
    def get_all_promo_codes(self) -> List[Dict]:
        """Get all promo codes"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM promo_codes ORDER BY created_at DESC")
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting all promo codes: {e}")
            return []
        finally:
            conn.close()
    
    # Bundle methods
    def get_all_bundles(self) -> List[Dict]:
        """Get all bundle packages with their items"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all active bundles
            cursor.execute("SELECT * FROM bundle_packages WHERE active = 1")
            bundles = cursor.fetchall()
            
            # Get items for each bundle
            for bundle in bundles:
                cursor.execute("SELECT * FROM bundle_items WHERE bundle_id = ?", (bundle["id"],))
                items = cursor.fetchall()
                
                # Parse metadata JSON for each item
                for item in items:
                    if "metadata" in item and item["metadata"]:
                        try:
                            metadata = json.loads(item["metadata"])
                            item.update(metadata)
                        except:
                            pass
                    item.pop("metadata", None)
                
                bundle["items"] = items
            
            return bundles
        except Exception as e:
            logger.error(f"Error getting all bundles: {e}")
            return []
        finally:
            conn.close()
    
    def get_bundle(self, bundle_id: str) -> Optional[Dict]:
        """Get bundle by ID with its items"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get bundle
            cursor.execute("SELECT * FROM bundle_packages WHERE id = ?", (bundle_id,))
            bundle = cursor.fetchone()
            
            if not bundle:
                return None
            
            # Get items
            cursor.execute("SELECT * FROM bundle_items WHERE bundle_id = ?", (bundle_id,))
            items = cursor.fetchall()
            
            # Parse metadata JSON for each item
            for item in items:
                if "metadata" in item and item["metadata"]:
                    try:
                        metadata = json.loads(item["metadata"])
                        item.update(metadata)
                    except:
                        pass
                item.pop("metadata", None)
            
            bundle["items"] = items
            return bundle
        except Exception as e:
            logger.error(f"Error getting bundle {bundle_id}: {e}")
            return None
        finally:
            conn.close()
    
    def add_bundle(self, bundle_data: Dict) -> bool:
        """Add new bundle package with its items"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Insert bundle
            cursor.execute(
                """INSERT INTO bundle_packages 
                (id, name, description, original_price, bundle_price, discount_percentage, created_by, created_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    bundle_data["id"],
                    bundle_data["name"],
                    bundle_data["description"],
                    bundle_data["original_price"],
                    bundle_data["bundle_price"],
                    bundle_data["discount_percentage"],
                    bundle_data["created_by"],
                    bundle_data["created_at"]
                )
            )
            
            # Insert items
            for item in bundle_data.get("items", []):
                metadata = {k: v for k, v in item.items() if k not in ["service", "item_name", "duration"]}
                
                cursor.execute(
                    """INSERT INTO bundle_items 
                    (bundle_id, service, item_name, duration, metadata) 
                    VALUES (?, ?, ?, ?, ?)""",
                    (
                        bundle_data["id"],
                        item["service"],
                        item["item_name"],
                        item.get("duration", ""),
                        json.dumps(metadata)
                    )
                )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding bundle: {e}")
            return False
        finally:
            conn.close()
    
    # Limited time offer methods
    def add_limited_time_offer(self, offer_data: Dict) -> bool:
        """Add new limited time offer"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """INSERT INTO limited_time_offers 
                (id, name, discount, type, expires, created_by, created_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    offer_data["id"],
                    offer_data["name"],
                    offer_data["discount"],
                    offer_data["type"],
                    offer_data["expires"],
                    offer_data["created_by"],
                    offer_data["created_at"]
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding limited time offer: {e}")
            return False
        finally:
            conn.close()
    
    def get_active_offers(self) -> List[Dict]:
        """Get active limited time offers"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            cursor.execute(
                "SELECT * FROM limited_time_offers WHERE expires > ?",
                (current_time,)
            )
            
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting active offers: {e}")
            return []
        finally:
            conn.close()
    
    # Scheduled task methods
    def add_scheduled_task(self, task_data: Dict) -> bool:
        """Add new scheduled task"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                """INSERT INTO scheduled_tasks 
                (task_id, type, message, scheduled_time, created_by, created_at) 
                VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    task_data["task_id"],
                    task_data["type"],
                    task_data["message"],
                    task_data["scheduled_time"],
                    task_data["created_by"],
                    task_data["created_at"]
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding scheduled task: {e}")
            return False
        finally:
            conn.close()
    
    def get_pending_tasks(self) -> List[Dict]:
        """Get pending scheduled tasks that are due"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            cursor.execute(
                "SELECT * FROM scheduled_tasks WHERE executed = 0 AND scheduled_time <= ?",
                (current_time,)
            )
            
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting pending tasks: {e}")
            return []
        finally:
            conn.close()
    
    def mark_task_executed(self, task_id: str) -> bool:
        """Mark scheduled task as executed"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE scheduled_tasks SET executed = 1 WHERE task_id = ?",
                (task_id,)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error marking task {task_id} as executed: {e}")
            return False
        finally:
            conn.close()
    
    # Feedback methods
    def add_feedback(self, user_id: int, text: str) -> bool:
        """Add user feedback"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO feedback (user_id, text, date) VALUES (?, ?, ?)",
                (user_id, text, datetime.now().isoformat())
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding feedback for user {user_id}: {e}")
            return False
        finally:
            conn.close()
    
    def get_user_feedback(self, user_id: int) -> List[Dict]:
        """Get all feedback from a user"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM feedback WHERE user_id = ? ORDER BY date DESC",
                (user_id,)
            )
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting feedback for user {user_id}: {e}")
            return []
        finally:
            conn.close()
    
    # Service options methods
    def get_service_options(self, service_type: str = None, option_type: str = None) -> List[Dict]:
        """Get service options, optionally filtered by service type and option type"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = "SELECT * FROM service_options"
            params = []
            
            if service_type or option_type:
                query += " WHERE"
                
                if service_type:
                    query += " service_type = ?"
                    params.append(service_type)
                    
                    if option_type:
                        query += " AND option_type = ?"
                        params.append(option_type)
                elif option_type:
                    query += " option_type = ?"
                    params.append(option_type)
            
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting service options: {e}")
            return []
        finally:
            conn.close()
    
    def add_service_option(self, option_data: Dict) -> bool:
        """Add new service option"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            metadata = {k: v for k, v in option_data.items() if k not in [
                "service_type", "option_type", "name", "price"
            ]}
            
            cursor.execute(
                """INSERT INTO service_options 
                (service_type, option_type, name, price, metadata) 
                VALUES (?, ?, ?, ?, ?)""",
                (
                    option_data["service_type"],
                    option_data["option_type"],
                    option_data["name"],
                    option_data.get("price", 0),
                    json.dumps(metadata) if metadata else None
                )
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding service option: {e}")
            return False
        finally:
            conn.close()
    
    def update_service_option(self, option_id: int, updates: Dict) -> bool:
        """Update service option"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
            params = list(updates.values()) + [option_id]
            
            cursor.execute(
                f"UPDATE service_options SET {set_clause} WHERE id = ?",
                params
            )
            
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating service option {option_id}: {e}")
            return False
        finally:
            conn.close()
    
    # Helper methods to convert between database and in-memory formats
    def get_all_service_options_formatted(self) -> Dict:
        """Get all service options formatted as in the original code"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            result = {
                "video_call": {"durations": []},
                "group": {"names": [], "durations": []},
                "private_chat": {"durations": [], "types": []},
                "album": []
            }
            
            # Get video call durations
            cursor.execute(
                "SELECT * FROM service_options WHERE service_type = 'video_call' AND option_type = 'duration'"
            )
            for option in cursor.fetchall():
                result["video_call"]["durations"].append({
                    "name": option["name"],
                    "price": option["price"]
                })
            
            # Get group names
            cursor.execute(
                "SELECT * FROM service_options WHERE service_type = 'group' AND option_type = 'name'"
            )
            for option in cursor.fetchall():
                result["group"]["names"].append(option["name"])
            
            # Get group durations
            cursor.execute(
                "SELECT * FROM service_options WHERE service_type = 'group' AND option_type = 'duration'"
            )
            for option in cursor.fetchall():
                result["group"]["durations"].append({
                    "name": option["name"],
                    "price": option["price"]
                })
            
            # Get chat durations
            cursor.execute(
                "SELECT * FROM service_options WHERE service_type = 'private_chat' AND option_type = 'duration'"
            )
            for option in cursor.fetchall():
                result["private_chat"]["durations"].append({
                    "name": option["name"],
                    "price": option["price"]
                })
            
            # Get chat types
            cursor.execute(
                "SELECT * FROM service_options WHERE service_type = 'private_chat' AND option_type = 'type'"
            )
            for option in cursor.fetchall():
                result["private_chat"]["types"].append({
                    "name": option["name"],
                    "price": option["price"]
                })
            
            # Get album options
            cursor.execute(
                "SELECT * FROM service_options WHERE service_type = 'album'"
            )
            for option in cursor.fetchall():
                result["album"].append({
                    "name": option["name"],
                    "price": option["price"]
                })
            
            return result
        except Exception as e:
            logger.error(f"Error getting formatted service options: {e}")
            return {
                "video_call": {"durations": []},
                "group": {"names": [], "durations": []},
                "private_chat": {"durations": [], "types": []},
                "album": []
            }
        finally:
            conn.close()

# Create a global database instance
db = Database()