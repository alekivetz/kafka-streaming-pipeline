# =============================================================================
# Restaurant Simulator
# =============================================================================
# Script Purpose:
#     This script is responsible for generating a realistic set of events for 
#     the pos-event-stream project. It simulates a restaurant environment
#     with multiple servers and tables, and generates events such as initiating
#     tables, adding items to orders, and payments. 
# =============================================================================

import time
import random
import uuid
import os
import sys
from dataclasses import dataclass, field
from typing import List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.utils import get_current_profile

# Define menu items, tables, servers, payment methods, and profiles (quiet, steady, busy, demo)
MENU = {
    "food": {
        "appetizers": [
            ("Pull Apart Brioche", 10.00),
            ("Crispy Yam Fries", 13.25),
            ("Avocado Dip", 14.75),
            ("Szechuan Chicken Lettuce Wraps", 24.50),
            ("Chicken Wings", 20.75)
        ],
        "salads": [
            ("Chicken Caesar Salad", 26.00),
            ("Green Goddess", 23.00),
            ("Lemongrass Noodle Salad", 18.25)
        ],
        "handhelds": [
            ("Baja Fish Tacos", 21.75),
            ("Crispy Chicken Sandwich", 24.25),
            ("Cheddar Bacon Burger", 25.50),
            ("Cajun Chicken Sandwich", 23.75)
        ],
        "mains": [
            ("Truffle Chicken", 33.25),
            ("Blackened Chicken", 31.75),
            ("Thai Curry", 24.75)
        ],
        "desserts": [
            ("Key Lime Pie", 13.50),
            ("Chocolate Lava Cake", 13.75)
        ]
    },
    "drinks": {
        "beer": [
            ("House Lager", 8.25),
            ("Prairie Fairy", 8.75),
            ("Wolf", 8.75)
        ],
        "cocktails": [
            ("Jalapeno Margarita", 16.00),
            ("Paloma", 14.00),
            ("Signature Sangria", 14.50),
            ("Old Fashioned", 16.50)
        ],
        "wine": [
            ("Cabernet Sauvignon", 12.00),
            ("Chardonnay", 12.50),
            ("Rose", 12.75),
            ("Sparkling", 12.75)
        ],
        "non-alcoholic": [
            ("Iced Tea", 6.00),
            ("Limonada", 7.00),
            ("Soda", 4.25),
            ("Coffee", 4.00)
        ]
    }
}

SERVERS = ["Angela", "Brooklyn", "Sara", "Amber", "Sammy", "Dave"]
TABLE_COUNT = 12
PAYMENT_METHODS = ["CASH", "CREDIT", "DEBIT", "TAP"]

LOAD_PROFILES = {
    "quiet": {
        "arrival_interval": 30 * 60,
        "max_tables": 4,
        "round_interval": (10 * 60, 20 * 60)
    },
    "steady": {
        "arrival_interval": 20 * 60,
        "max_tables": 8,
        "round_interval": (8 * 60, 15 * 60)
    },
    "busy": {
        "arrival_interval": 10 * 60,
        "max_tables": 12,
        "round_interval": (5 * 60, 12 * 60)
    },
    "demo": {
        "arrival_interval": 10,
        "max_tables": 8,
        "round_interval": (20, 60)
    }
}

@dataclass
class ActiveOrder:
    """
    Simulates an active order in the restaurant. Contains all the information needed 
    throughout the order's lifetime, such as the order id, table number, opened at
    time, current round, rounds remaining, and the items in the order.
    """
    order_id: str
    table_number: int
    table_size: int
    server_name: str
    opened_at: float
    current_round: int
    rounds_remaining: int
    send_next_round: float
    items: List[dict] = field(default_factory=list)

def random_item_from_category(category):
    """
    Helper function for the restaurant simulator class. 
    Randomly selects an item from the menu for a given category.
    Returns the subcategory, item name, and price of the item.
    """
    subcategory = random.choice(list(MENU[category].keys()))
    item_name, price = random.choice(MENU[category][subcategory])
    return subcategory, item_name, price

def sum_total_amount(items):
    """
    Helper function for the restaurant simulator class.
    Sums the total amount of items in a list of order items, returning the total amount.
    """
    total = 0
    for item in items:
        total += item["price"] * item["quantity"]
    return total

class RestaurantSimulator:
    def __init__(self):
        """
        Initiates a new restaurant simulator instance.
        Selects a load profile based on the current time, initiates available tables,
        a storage dictionary for active orders, and sets up timing for the next table arrival.
        """
        self.profile = None
        self.available_tables = set(range(1, TABLE_COUNT + 1))
        self.active_orders = {}
        self.next_arrival = time.time() 
    
    def tick(self):
        """
        This method is called every second to generate a new set of events.
        It checks if a table is available, if so, it creates a new order and adds it to the active orders dictionary.
        If a table is not available, it checks if there are any active orders and if so, it generates events for those orders.
        """
        events = []
        current_time = time.time()

        profile_name = get_current_profile()
        if profile_name is None:
            return []
        self.profile = LOAD_PROFILES[profile_name]

        # Seat a new table if possible
        if current_time > self.next_arrival and len(self.available_tables) > 0 and len(self.active_orders) < self.profile["max_tables"]:
            table = random.choice(list(self.available_tables))
            order_id = str(uuid.uuid4())
            
            self.available_tables.remove(table)
            self.active_orders[table] = ActiveOrder(
                order_id, 
                table,
                random.choices([1, 2, 3, 4, 5, 6, 7, 8], weights=[5, 25, 20, 25, 15, 15, 3, 2], k=1)[0],
                random.choice(SERVERS),
                current_time, 
                1,
                random.choices([2, 3, 4], weights=[50, 30, 20], k=1)[0],
                current_time + random.uniform(*self.profile["round_interval"]))
            
            order_event = {
                "order_id": order_id,
                "table_number": table,
                "table_size": self.active_orders[table].table_size,
                "server_name": self.active_orders[table].server_name,
                "opened_at": int(current_time * 1000)
            }
            events.append(("orders", order_event))

            self.next_arrival = current_time + self.profile["arrival_interval"]
            
        # Iterate through active orders, adding items to them as needed
        for order in self.active_orders.values():
            if order.rounds_remaining > 0 and current_time > order.send_next_round:
                items = []

                # Order drinks
                if order.current_round == 1:    
                    for _ in range(order.table_size):
                        subcategory, item_name, price = random_item_from_category("drinks")
                        items.append([subcategory, item_name, price])
                    # Optional order appetizers
                    if random.choice([True, False]):
                        for _ in range(random.choices([1, 2], weights=[60, 40], k=1)[0]):
                            item_name, price = random.choice(MENU["food"]["appetizers"])
                            items.append(["appetizers", item_name, price])
                # Order entrees
                elif order.current_round == 2:
                    for _ in range(order.table_size):
                        food_subcategories = [cat for cat in MENU["food"].keys() if cat not in ["appetizers", "desserts"]]
                        chosen_subcategory = random.choice(food_subcategories)
                        item_name, price = random.choice(MENU["food"][chosen_subcategory])
                        items.append([chosen_subcategory, item_name, price])
                # Optional order extras
                else:
                    for _ in range(random.randint(1, 3)):
                        category = random.choice(list(MENU.keys()))
                        subcategory, item_name, price = random_item_from_category(category)
                        items.append([subcategory, item_name, price])

                for item in items:
                    order_item_event = {
                        "item_id": str(uuid.uuid4()),
                        "order_id": order.order_id,
                        "item_name": item[1],
                        "category": item[0],
                        "price": item[2],
                        "quantity": 1,
                        "added_at": int(current_time * 1000)
                    }
                    order.items.append(order_item_event)
                    events.append(("order-items", order_item_event))    

                order.current_round += 1
                order.rounds_remaining -= 1
                order.send_next_round = current_time + random.uniform(*self.profile["round_interval"])

                
        # Check to see if any orders are ready to be paid, and if so, generate a payment event
        ready_to_pay = [order for order in self.active_orders.values() if order.rounds_remaining == 0]
        for order in ready_to_pay:
            payment_event = {
                "payment_id": str(uuid.uuid4()),
                "order_id": order.order_id,
                "table_number": order.table_number,  
                "table_size": order.table_size,
                "server_name": order.server_name,
                "total_amount": sum_total_amount(order.items),
                "payment_method": random.choice(PAYMENT_METHODS),
                "paid_at": int(current_time * 1000),
                "table_duration_minutes": (current_time - order.opened_at) / 60
            }
            events.append(("payments", payment_event))
            self.active_orders.pop(order.table_number)
            self.available_tables.add(order.table_number)
            
        return events

if __name__ == "__main__":
    sim = RestaurantSimulator()
    while True:
        events = sim.tick()
        for topic, event in events:
            print(f"[{topic}] {event}")
        time.sleep(1)

