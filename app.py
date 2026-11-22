"""
Foodies - customer-facing food ordering app (Flask + SQLite).

Run:   pip install flask
       python app.py          then open http://127.0.0.1:5000
Demo login:  demo@foodies.pk  /  demo123

How this maps to the architecture diagram
    API Gateway        -> Flask routes + login_required (every request is authenticated)
    Auth service       -> /login, session cookie, hashed passwords
    Catalog service    -> get_menu() with an in-memory cache (the "Redis" of this demo)
    Cart / business    -> cart_summary(): totals, delivery fee, tax, minimum order rule
    Order service      -> api_checkout(): saves order as PENDING, calls payment, publishes event
    Payment service    -> charge(): simulated provider (cash on delivery or card)
    SNS + SQS          -> EventBus: each subscriber gets its own delivery, with retries
    Consumers          -> restaurant_consumer, dispatch_consumer, notification_consumer (Lambda)
"""
import os
import random
import re
import sqlite3
import threading
import time
from datetime import datetime
from functools import wraps

from flask import (Flask, jsonify, redirect, render_template, request, session, url_for)
from jinja2 import DictLoader
from werkzeug.security import check_password_hash, generate_password_hash

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("FOODIES_DB", os.path.join(BASE_DIR, "foodies.db"))
STEP_DELAY = float(os.environ.get("FOODIES_STEP_DELAY", "5"))  # seconds between order stages
DELIVERY_FEE, FREE_DELIVERY_OVER, TAX_RATE, MAX_QTY = 120, 2000, 0.05, 10
COURIERS = ["Ali Raza", "Sara Khan", "Bilal Ahmed"]
_version_file = os.path.join(BASE_DIR, "VERSION")
APP_VERSION = open(_version_file).read().strip() if os.path.exists(_version_file) else "dev"

app = Flask(__name__)
app.secret_key = os.environ.get("FOODIES_SECRET", "foodies-demo-secret-change-me")


# =============================================================================
# 1. DATABASE (SQLite) and dummy data
# =============================================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE, password_hash TEXT, address TEXT);
CREATE TABLE IF NOT EXISTS restaurants(id INTEGER PRIMARY KEY, name TEXT, cuisine TEXT, rating REAL,
    delivery_minutes INTEGER, min_order INTEGER, emoji TEXT);
CREATE TABLE IF NOT EXISTS menu_items(id INTEGER PRIMARY KEY, restaurant_id INTEGER, name TEXT, description TEXT,
    price INTEGER, emoji TEXT, available INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS cart_items(user_id INTEGER, item_id INTEGER, qty INTEGER, PRIMARY KEY(user_id, item_id));
CREATE TABLE IF NOT EXISTS orders(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, restaurant_id INTEGER,
    status TEXT, subtotal INTEGER, delivery_fee INTEGER, tax INTEGER, total INTEGER, payment_method TEXT,
    address TEXT, idempotency_key TEXT UNIQUE, courier TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS order_items(order_id INTEGER, item_id INTEGER, name TEXT, price INTEGER, qty INTEGER);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT, order_id INTEGER, source TEXT,
    message TEXT, created_at TEXT);
"""

RESTAURANTS = [
    (1, "Biryani House", "Biryani", 4.6, 35, 500, "🍛"),
    (2, "Pizza Corner", "Pizza", 4.4, 30, 800, "🍕"),
    (3, "Burger Lab", "Burgers", 4.5, 25, 600, "🍔"),
    (4, "Karachi Chai Dhaba", "Desi breakfast and tea", 4.7, 20, 300, "☕"),
]
MENU = [  # restaurant_id, name, description, price, emoji
    (1, "Chicken Biryani", "Spiced basmati rice with tender chicken", 450, "🍛"),
    (1, "Beef Biryani", "Slow-cooked beef, aromatic rice", 520, "🍛"),
    (1, "Mutton Biryani", "Rich, fragrant, served with salad", 750, "🍖"),
    (1, "Raita", "Cool yogurt with cucumber", 60, "🥣"),
    (1, "Soft Drink", "345 ml can", 90, "🥤"),
    (2, "Margherita Pizza", "Tomato, mozzarella, basil", 900, "🍕"),
    (2, "Fajita Pizza", "Chicken fajita, peppers, onions", 1150, "🍕"),
    (2, "Pepperoni Pizza", "Classic pepperoni and cheese", 1250, "🍕"),
    (2, "Garlic Bread", "Toasted with garlic butter", 350, "🥖"),
    (2, "Chocolate Brownie", "Warm, with chocolate sauce", 400, "🍫"),
    (3, "Zinger Burger", "Crispy fried chicken, mayo, lettuce", 550, "🍔"),
    (3, "Smash Beef Burger", "Double smashed patty, cheddar", 650, "🍔"),
    (3, "Fries", "Large, salted", 250, "🍟"),
    (3, "Chocolate Shake", "Thick and cold", 400, "🥤"),
    (4, "Doodh Patti Chai", "Strong milky tea", 120, "☕"),
    (4, "Paratha Roll", "Chicken paratha roll with chutney", 280, "🌯"),
    (4, "Anda Paratha", "Fried egg paratha", 250, "🍳"),
    (4, "Halwa Puri", "Classic Karachi breakfast plate", 450, "🥘"),
    (4, "Sweet Lassi", "Chilled yogurt drink", 220, "🥛"),
]


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def query(sql, args=(), one=False):
    conn = db()
    try:
        rows = conn.execute(sql, args).fetchall()
    finally:
        conn.close()
    return (rows[0] if rows else None) if one else rows


def run(sql, args=()):
    conn = db()
    try:
        cur = conn.execute(sql, args)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def init_db():
    conn = db()
    conn.executescript(SCHEMA)
    if conn.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0] == 0:
        conn.executemany("INSERT INTO restaurants VALUES (?,?,?,?,?,?,?)", RESTAURANTS)
        conn.executemany("INSERT INTO menu_items(restaurant_id,name,description,price,emoji) VALUES (?,?,?,?,?)", MENU)
        conn.execute("INSERT INTO users(name,email,password_hash,address) VALUES (?,?,?,?)",
                     ("Demo Customer", "demo@foodies.pk", generate_password_hash("demo123"),
                      "House 12, Block 5, Clifton, Karachi"))
    conn.commit()
    conn.close()


def now():
    return datetime.now().strftime("%H:%M:%S")


def log_event(order_id, source, message):
    run("INSERT INTO events(order_id,source,message,created_at) VALUES (?,?,?,?)", (order_id, source, message, now()))


def set_status(order_id, status):
    run("UPDATE orders SET status=? WHERE id=?", (status, order_id))


# =============================================================================
# 2. CATALOG SERVICE: menu with cache (hit/miss)
# =============================================================================
_menu_cache = {}  # restaurant_id -> (rows, expires_at)


def get_menu(restaurant_id, ttl=60):
    cached = _menu_cache.get(restaurant_id)
    if cached and cached[1] > time.time():
        return cached[0], "cache"
    rows = [dict(r) for r in query("SELECT * FROM menu_items WHERE restaurant_id=? AND available=1", (restaurant_id,))]
    _menu_cache[restaurant_id] = (rows, time.time() + ttl)
    return rows, "database"


# =============================================================================
# 3. CART BUSINESS LOGIC (prices always come from the database, never the browser)
# =============================================================================
def cart_summary(uid):
    rows = query("""SELECT m.id AS item_id, m.name, m.price, m.emoji, m.restaurant_id, c.qty
                    FROM cart_items c JOIN menu_items m ON m.id = c.item_id
                    WHERE c.user_id=? ORDER BY m.id""", (uid,))
    items = [dict(r, line_total=r["price"] * r["qty"]) for r in rows]
    subtotal = sum(i["line_total"] for i in items)
    restaurant = None
    if items:
        restaurant = dict(query("SELECT * FROM restaurants WHERE id=?", (items[0]["restaurant_id"],), one=True))
    delivery = 0 if (not items or subtotal >= FREE_DELIVERY_OVER) else DELIVERY_FEE
    tax = round(subtotal * TAX_RATE)
    return {"items": items, "restaurant": restaurant, "subtotal": subtotal, "delivery_fee": delivery,
            "tax": tax, "total": subtotal + delivery + tax,
            "min_order": restaurant["min_order"] if restaurant else 0,
            "meets_min": (subtotal >= restaurant["min_order"]) if restaurant else False,
            "free_delivery_over": FREE_DELIVERY_OVER}


# =============================================================================
# 4. PAYMENT SERVICE (simulated provider)
# =============================================================================
def charge(amount, method, card_number):
    if method == "cod":
        return True, f"Cash on delivery chosen: Rs {amount:,} to be paid on arrival"
    digits = re.sub(r"\D", "", card_number or "")
    if len(digits) != 16:
        return False, "Card number must be 16 digits"
    if digits.endswith("0000"):
        return False, "Card declined by the bank (demo rule: cards ending in 0000 are declined)"
    return True, f"Card ending {digits[-4:]} charged Rs {amount:,}"


# =============================================================================
# 5. EVENT BUS (SNS + SQS idea): publish once, each subscriber reacts on its own
# =============================================================================
class EventBus:
    def __init__(self):
        self.subscribers = []

    def subscribe(self, name, event_types, handler):
        self.subscribers.append((name, event_types, handler))

    def publish(self, event_type, order_id):
        targets = [s for s in self.subscribers if event_type in s[1]]
        log_event(order_id, "SNS", f"{event_type} published, copied to {len(targets)} queue(s)")
        for name, _, handler in targets:  # each consumer works independently
            threading.Thread(target=self._deliver, args=(name, handler, event_type, order_id), daemon=True).start()

    @staticmethod
    def _deliver(name, handler, event_type, order_id):
        for attempt in range(1, 4):
            try:
                handler(event_type, order_id)
                return
            except Exception as error:  # retry, then give up (dead-letter queue)
                log_event(order_id, "SQS", f"{name} failed ({error}). Retry {attempt} of 3")
                time.sleep(0.5)
        log_event(order_id, "Dead-letter queue", f"{name} gave up on {event_type}")


bus = EventBus()


def restaurant_consumer(event_type, order_id):
    log_event(order_id, "Restaurant", "New order appeared on the restaurant tablet")
    time.sleep(STEP_DELAY)
    set_status(order_id, "ACCEPTED")
    log_event(order_id, "Restaurant", "Restaurant accepted the order")
    bus.publish("OrderAccepted", order_id)
    time.sleep(STEP_DELAY)
    set_status(order_id, "PREPARING")
    log_event(order_id, "Restaurant", "Kitchen started preparing the food")
    time.sleep(STEP_DELAY)
    set_status(order_id, "READY")
    log_event(order_id, "Restaurant", "Food is packed and ready for pickup")
    bus.publish("OrderReady", order_id)


def dispatch_consumer(event_type, order_id):
    courier = random.choice(COURIERS)
    run("UPDATE orders SET courier=? WHERE id=?", (courier, order_id))
    set_status(order_id, "OUT_FOR_DELIVERY")
    log_event(order_id, "Dispatch", f"Courier {courier} picked up the order")
    bus.publish("CourierAssigned", order_id)
    time.sleep(STEP_DELAY)
    set_status(order_id, "DELIVERED")
    log_event(order_id, "Dispatch", "Order delivered to the customer")
    bus.publish("OrderDelivered", order_id)


SMS_TEXT = {"OrderPlaced": "Your order is confirmed.", "OrderAccepted": "The restaurant accepted your order.",
            "CourierAssigned": "Your courier is on the way.", "OrderDelivered": "Delivered. Enjoy your meal!"}


def notification_consumer(event_type, order_id):  # plays the role of an AWS Lambda
    if random.random() < 0.2:
        raise RuntimeError("SMS provider timeout")
    log_event(order_id, "Lambda (SMS)", f"SMS sent: {SMS_TEXT[event_type]}")


bus.subscribe("restaurant", {"OrderPlaced"}, restaurant_consumer)
bus.subscribe("dispatch", {"OrderReady"}, dispatch_consumer)
bus.subscribe("notifications", set(SMS_TEXT), notification_consumer)


# =============================================================================
# 6. API GATEWAY + AUTH: login, session, and the customer routes
# =============================================================================
def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if "uid" not in session:
            if request.path.startswith("/api/"):
                return jsonify(error="Please log in first"), 401
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapper


@app.context_processor
def inject_user():
    return {"user_name": session.get("name"), "app_version": APP_VERSION}


@app.template_filter("money")
def money(value):
    return f"Rs {int(value):,}"


@app.get("/health")
def health():
    """Used by the pipeline to confirm which version is live in production."""
    return jsonify(status="ok", version=APP_VERSION)


@app.route("/")
def home():
    return redirect(url_for("restaurants" if "uid" in session else "login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = query("SELECT * FROM users WHERE email=?", (request.form.get("email", "").strip().lower(),), one=True)
        if user and check_password_hash(user["password_hash"], request.form.get("password", "")):
            session.clear()
            session["uid"], session["name"] = user["id"], user["name"]
            return redirect(url_for("restaurants"))
        error = "Wrong email or password"
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/restaurants")
@login_required
def restaurants():
    term = request.args.get("q", "").strip()
    rows = query("SELECT * FROM restaurants WHERE name LIKE ? OR cuisine LIKE ? ORDER BY rating DESC",
                 (f"%{term}%", f"%{term}%"))
    return render_template("restaurants.html", restaurants=rows, term=term)


@app.route("/restaurant/<int:rid>")
@login_required
def menu(rid):
    restaurant = query("SELECT * FROM restaurants WHERE id=?", (rid,), one=True)
    if not restaurant:
        return redirect(url_for("restaurants"))
    items, source = get_menu(rid)
    return render_template("menu.html", restaurant=restaurant, items=items, source=source)


@app.route("/checkout")
@login_required
def checkout():
    user = query("SELECT address FROM users WHERE id=?", (session["uid"],), one=True)
    return render_template("checkout.html", address=user["address"])


@app.route("/orders")
@login_required
def orders():
    rows = query("""SELECT o.*, r.name AS restaurant FROM orders o JOIN restaurants r ON r.id=o.restaurant_id
                    WHERE o.user_id=? ORDER BY o.id DESC""", (session["uid"],))
    return render_template("orders.html", orders=rows)


@app.route("/orders/<int:oid>")
@login_required
def order_page(oid):
    order = query("SELECT * FROM orders WHERE id=? AND user_id=?", (oid, session["uid"]), one=True)
    if not order:
        return redirect(url_for("orders"))
    return render_template("order.html", oid=oid)


@app.get("/api/cart")
@login_required
def api_cart():
    return jsonify(cart_summary(session["uid"]))


@app.post("/api/cart/add")
@login_required
def api_cart_add():
    data, uid = request.get_json(force=True), session["uid"]
    item = query("SELECT * FROM menu_items WHERE id=? AND available=1", (int(data["item_id"]),), one=True)
    if not item:
        return jsonify(error="This item is not available"), 404
    in_cart = query("""SELECT DISTINCT m.restaurant_id AS rid FROM cart_items c
                       JOIN menu_items m ON m.id=c.item_id WHERE c.user_id=?""", (uid,))
    if in_cart and in_cart[0]["rid"] != item["restaurant_id"]:  # business rule: one restaurant per order
        if not data.get("replace"):
            return jsonify(error="Your cart has items from another restaurant. Start a new cart?", conflict=True), 409
        run("DELETE FROM cart_items WHERE user_id=?", (uid,))
    run("""INSERT INTO cart_items(user_id,item_id,qty) VALUES (?,?,1)
           ON CONFLICT(user_id,item_id) DO UPDATE SET qty=MIN(qty+1, ?)""", (uid, item["id"], MAX_QTY))
    return jsonify(cart_summary(uid))


@app.post("/api/cart/update")
@login_required
def api_cart_update():
    data, uid = request.get_json(force=True), session["uid"]
    qty = int(data["qty"])
    if qty <= 0:
        run("DELETE FROM cart_items WHERE user_id=? AND item_id=?", (uid, int(data["item_id"])))
    else:
        run("UPDATE cart_items SET qty=? WHERE user_id=? AND item_id=?", (min(qty, MAX_QTY), uid, int(data["item_id"])))
    return jsonify(cart_summary(uid))


@app.post("/api/checkout")
@login_required
def api_checkout():
    """ORDER SERVICE: validate, save as PENDING, charge (synchronous), then publish the event (asynchronous)."""
    data, uid = request.get_json(force=True), session["uid"]
    key = str(data.get("idempotency_key", "")).strip()
    address = str(data.get("address", "")).strip()
    method = data.get("payment_method", "cod")
    if not key:
        return jsonify(error="Missing request key"), 400
    existing = query("SELECT id, status FROM orders WHERE idempotency_key=?", (key,), one=True)
    if existing:  # same click sent twice: return the same order, never charge twice
        return jsonify(order_id=existing["id"], status=existing["status"], duplicate=True)
    cart = cart_summary(uid)
    if not cart["items"]:
        return jsonify(error="Your cart is empty"), 400
    if len(address) < 8:
        return jsonify(error="Please enter a full delivery address"), 400
    if not cart["meets_min"]:
        return jsonify(error=f"Minimum order for {cart['restaurant']['name']} is Rs {cart['min_order']:,}"), 400

    conn = db()
    try:
        cur = conn.execute("""INSERT INTO orders(user_id,restaurant_id,status,subtotal,delivery_fee,tax,total,
                              payment_method,address,idempotency_key,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                           (uid, cart["restaurant"]["id"], "PENDING", cart["subtotal"], cart["delivery_fee"],
                            cart["tax"], cart["total"], method, address, key,
                            datetime.now().strftime("%Y-%m-%d %H:%M")))
        oid = cur.lastrowid
        conn.executemany("INSERT INTO order_items VALUES (?,?,?,?,?)",
                         [(oid, i["item_id"], i["name"], i["price"], i["qty"]) for i in cart["items"]])
        conn.commit()
    finally:
        conn.close()
    log_event(oid, "Order service", f"Order #{oid} saved as PENDING (total Rs {cart['total']:,})")

    ok, message = charge(cart["total"], method, data.get("card_number", ""))  # SYNCHRONOUS call
    log_event(oid, "Payment service", message)
    if not ok:
        run("UPDATE orders SET status='PAYMENT_FAILED', idempotency_key=NULL WHERE id=?", (oid,))
        log_event(oid, "Order service", "Payment failed. No events published. Cart kept so you can retry.")
        return jsonify(error=message, order_id=oid), 402

    set_status(oid, "PLACED")
    run("DELETE FROM cart_items WHERE user_id=?", (uid,))
    log_event(oid, "Order service", "Order is PLACED")
    bus.publish("OrderPlaced", oid)  # ASYNCHRONOUS from here: the customer does not wait for any of it
    return jsonify(order_id=oid, status="PLACED")


@app.get("/api/orders/<int:oid>")
@login_required
def api_order(oid):
    order = query("""SELECT o.*, r.name AS restaurant FROM orders o JOIN restaurants r ON r.id=o.restaurant_id
                     WHERE o.id=? AND o.user_id=?""", (oid, session["uid"]), one=True)
    if not order:
        return jsonify(error="Order not found"), 404
    items = [dict(r) for r in query("SELECT name, price, qty FROM order_items WHERE order_id=?", (oid,))]
    events = [dict(r) for r in query("SELECT source, message, created_at FROM events WHERE order_id=? ORDER BY id", (oid,))]
    return jsonify(order=dict(order), items=items, events=events)


# =============================================================================
# 7. FRONT END (HTML + CSS + JavaScript templates)
# =============================================================================
BASE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Foodies</title>
<style>
:root{--accent:#d9480f;--ink:#1f2933;--muted:#5b6770;--line:#e4e7ea;--bg:#f6f7f8;--card:#fff;--ok:#1a7f4b}
*{box-sizing:border-box}body{margin:0;font:16px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--ink)}
a{color:inherit;text-decoration:none}
nav{background:var(--card);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:5}
.nav{max-width:1100px;margin:auto;display:flex;align-items:center;gap:20px;padding:12px 20px}
.logo{font:700 24px Georgia,serif;color:var(--accent)}.sp{flex:1}.nav a.l:hover{color:var(--accent)}
.badge{background:var(--accent);color:#fff;border-radius:99px;padding:1px 9px;font-size:13px;margin-left:4px}
main{max-width:1100px;margin:auto;padding:24px 20px 60px}
h1{margin:0 0 4px;font:700 28px Georgia,serif}.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:16px;margin-top:18px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.rest:hover{border-color:var(--accent)}.big{font-size:44px}
.btn{display:inline-block;border:0;border-radius:8px;padding:9px 14px;font:inherit;font-weight:600;cursor:pointer;background:var(--accent);color:#fff}
.btn.alt{background:#fff;color:var(--accent);border:1px solid var(--accent)}.btn:disabled{opacity:.5;cursor:not-allowed}.btn.full{width:100%}
.row{display:flex;gap:10px;align-items:center}.between{justify-content:space-between}
input[type=text],input[type=email],input[type=password],textarea{width:100%;padding:10px 12px;border:1px solid var(--line);border-radius:8px;font:inherit}
input:focus,textarea:focus,button:focus-visible{outline:3px solid #f4a77b;outline-offset:1px}
label{display:block;font-weight:600;margin:12px 0 4px}
.layout{display:grid;grid-template-columns:1fr 340px;gap:20px;margin-top:18px}
@media(max-width:860px){.layout{grid-template-columns:1fr}}
.item{display:flex;gap:12px;align-items:center;justify-content:space-between;margin-bottom:12px}
.item .big{font-size:34px}.price{font-weight:700}
.cart{position:sticky;top:76px;align-self:start}
.qty button{width:28px;height:28px;border:1px solid var(--line);background:#fff;border-radius:6px;cursor:pointer;font-size:16px}
.tot{display:flex;justify-content:space-between;padding:3px 0}.tot.g{font-weight:700;font-size:18px;border-top:1px solid var(--line);margin-top:6px;padding-top:8px}
.warn{background:#fff4e6;border:1px solid #ffd8a8;border-radius:8px;padding:8px 10px;font-size:14px;margin:8px 0}
.err{color:#b42318;margin:8px 0;font-weight:600}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:var(--ink);color:#fff;padding:10px 18px;border-radius:8px;opacity:0;transition:.3s;pointer-events:none}.toast.on{opacity:1}
.steps{display:flex;gap:6px;flex-wrap:wrap;margin:14px 0}.step{flex:1;min-width:92px;text-align:center;padding:8px 4px;border-radius:8px;background:#eceff1;color:var(--muted);font-size:13px}
.step.done{background:#dcf3e5;color:var(--ok);font-weight:600}.step.now{background:var(--accent);color:#fff;font-weight:700}
.log{max-height:340px;overflow:auto}.ev{padding:6px 0 6px 10px;border-left:3px solid var(--line);font-size:14px;margin-bottom:4px}
.ev b{margin-right:6px}.pill{padding:2px 10px;border-radius:99px;background:#eceff1;font-size:13px;font-weight:600}
.pill.DELIVERED{background:#dcf3e5;color:var(--ok)}.pill.PAYMENT_FAILED{background:#fde7e7;color:#b42318}
.hint{background:#eef4ff;border:1px solid #cfe0ff;border-radius:8px;padding:8px 12px;font-size:14px;margin-top:14px}
</style></head><body>
{% if user_name %}<nav><div class="nav"><a class="logo" href="/restaurants">Foodies</a>
<a class="l" href="/restaurants">Restaurants</a><a class="l" href="/orders">My orders</a><span class="sp"></span>
<a class="l" href="/checkout">Cart<span class="badge" id="badge">0</span></a>
<span class="muted">{{ user_name }}</span><a class="l" href="/logout">Log out</a></div></nav>{% endif %}
<main>{% block content %}{% endblock %}</main>
<footer class="muted" style="text-align:center;padding:0 0 24px;font-size:13px">Foodies v{{ app_version }}</footer><div class="toast" id="toast"></div>
<script>
const money = v => 'Rs ' + Number(v).toLocaleString('en-PK');
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
async function api(url, body){
  const r = await fetch(url, {method: body ? 'POST' : 'GET', headers: {'Content-Type': 'application/json'}, body: body ? JSON.stringify(body) : undefined});
  let data = {}; try { data = await r.json(); } catch (e) {}
  if (r.status === 401) { location.href = '/login'; }
  return {ok: r.ok, status: r.status, data};
}
function toast(t){ const el = document.getElementById('toast'); el.textContent = t; el.classList.add('on'); setTimeout(() => el.classList.remove('on'), 1600); }
function renderCart(el, cart, withButton){
  if (!cart.items.length){ el.innerHTML = '<p class="muted">Your cart is empty. Pick a restaurant and add something tasty.</p>'; return; }
  let h = '<div class="muted" style="margin-bottom:8px">From ' + esc(cart.restaurant.name) + '</div>';
  cart.items.forEach(i => {
    h += '<div class="item"><div>' + i.emoji + ' ' + esc(i.name) + '<div class="muted">' + money(i.line_total) + '</div></div>' +
      '<div class="qty row"><button aria-label="Less" onclick="setQty(' + i.item_id + ',' + (i.qty - 1) + ')">-</button><b>' + i.qty +
      '</b><button aria-label="More" onclick="setQty(' + i.item_id + ',' + (i.qty + 1) + ')">+</button></div></div>';
  });
  h += '<div class="tot"><span>Subtotal</span><span>' + money(cart.subtotal) + '</span></div>' +
       '<div class="tot"><span>Delivery fee</span><span>' + (cart.delivery_fee ? money(cart.delivery_fee) : 'Free') + '</span></div>' +
       '<div class="tot"><span>Tax (5%)</span><span>' + money(cart.tax) + '</span></div>' +
       '<div class="tot g"><span>Total</span><span>' + money(cart.total) + '</span></div>';
  if (!cart.meets_min) h += '<div class="warn">Minimum order is ' + money(cart.min_order) + '. Add ' + money(cart.min_order - cart.subtotal) + ' more.</div>';
  else if (cart.delivery_fee) h += '<div class="muted" style="font-size:14px;margin-top:6px">Free delivery above ' + money(cart.free_delivery_over) + '</div>';
  if (withButton) h += '<a class="btn full" style="margin-top:12px;text-align:center" href="/checkout">Order now</a>';
  el.innerHTML = h;
}
async function refreshCart(){
  const res = await api('/api/cart'); if (!res.ok) return;
  window.CART = res.data; const b = document.getElementById('badge');
  if (b) b.textContent = res.data.items.reduce((n, i) => n + i.qty, 0);
  if (window.onCart) window.onCart(res.data);
}
async function addToCart(id, goto){
  let res = await api('/api/cart/add', {item_id: id});
  if (res.status === 409 && confirm(res.data.error)) res = await api('/api/cart/add', {item_id: id, replace: true});
  if (!res.ok){ if (res.status !== 409) toast(res.data.error || 'Something went wrong'); return; }
  if (goto){ location.href = goto; return; }
  toast('Added to cart'); refreshCart();
}
async function setQty(id, qty){ await api('/api/cart/update', {item_id: id, qty}); refreshCart(); }
if (document.getElementById('badge')) refreshCart();
</script>
{% block scripts %}{% endblock %}</body></html>"""

LOGIN = """{% extends "base.html" %}{% block content %}
<div style="max-width:400px;margin:50px auto"><div class="card">
<div class="logo" style="text-align:center;font-size:36px">Foodies</div>
<p class="muted" style="text-align:center;margin-top:0">Order from your favourite restaurants</p>
<form method="post"><label for="email">Email</label><input id="email" name="email" type="email" value="demo@foodies.pk" required>
<label for="password">Password</label><input id="password" name="password" type="password" value="demo123" required>
{% if error %}<div class="err" role="alert">{{ error }}</div>{% endif %}
<button class="btn full" style="margin-top:16px" type="submit">Log in as customer</button></form>
<div class="hint">Demo account is pre-filled: demo@foodies.pk / demo123</div></div></div>{% endblock %}"""

RESTAURANTS_PAGE = """{% extends "base.html" %}{% block content %}
<h1>Hungry?we got you!</h1><p class="muted">Choose a restaurant to see its menu.</p>
<form method="get" class="row" style="margin-top:12px;max-width:480px"><input type="text" name="q" value="{{ term }}" placeholder="Search by name or cuisine" aria-label="Search">
<button class="btn" type="submit">Search</button></form>
<div class="grid">{% for r in restaurants %}<a class="card rest" href="/restaurant/{{ r.id }}">
<div class="big">{{ r.emoji }}</div><h2 style="margin:6px 0 0;font-size:20px">{{ r.name }}</h2><div class="muted">{{ r.cuisine }}</div>
<div class="row between" style="margin-top:10px"><span>★ {{ r.rating }}</span><span>{{ r.delivery_minutes }} min</span><span class="muted">Min {{ r.min_order|money }}</span></div></a>
{% else %}<p class="muted">No restaurants match “{{ term }}”.</p>{% endfor %}</div>{% endblock %}"""

MENU_PAGE = """{% extends "base.html" %}{% block content %}
<a class="muted" href="/restaurants">Back to restaurants</a>
<h1>{{ restaurant.emoji }} {{ restaurant.name }}</h1>
<div class="muted">{{ restaurant.cuisine }} · ★ {{ restaurant.rating }} · {{ restaurant.delivery_minutes }} min · Minimum order {{ restaurant.min_order|money }}</div>
<div class="muted" style="font-size:13px">Menu served from the {{ source }}{% if source == 'cache' %} (fast path){% endif %}</div>
<div class="layout"><div>{% for m in items %}<div class="card item"><div class="row"><span class="big">{{ m.emoji }}</span>
<div><b>{{ m.name }}</b><div class="muted">{{ m.description }}</div><div class="price">{{ m.price|money }}</div></div></div>
<div><button class="btn alt" onclick="addToCart({{ m.id }})">Add to cart</button>
<button class="btn" style="margin-top:6px" onclick="addToCart({{ m.id }}, '/checkout')">Order now</button></div></div>{% endfor %}</div>
<aside class="card cart"><h2 style="margin-top:0;font-size:20px">Your cart</h2><div id="cart"></div></aside></div>{% endblock %}
{% block scripts %}<script>window.onCart = c => renderCart(document.getElementById('cart'), c, true);</script>{% endblock %}"""

CHECKOUT_PAGE = """{% extends "base.html" %}{% block content %}
<h1>Checkout</h1><div class="layout"><div class="card">
<label for="address">Delivery address</label><textarea id="address" rows="3">{{ address }}</textarea>
<label>Payment method</label>
<label class="row" style="font-weight:400"><input type="radio" name="pay" value="cod" checked> Cash on delivery</label>
<label class="row" style="font-weight:400"><input type="radio" name="pay" value="card"> Credit or debit card (dummy)</label>
<div id="cardbox" style="display:none"><label for="card">Card number</label><input id="card" type="text" inputmode="numeric" placeholder="4242 4242 4242 4242">
<div class="muted" style="font-size:13px">Demo: any 16 digits works. A number ending in 0000 is declined.</div></div>
<div class="err" id="error" role="alert"></div>
<button class="btn full" id="place" style="margin-top:14px">Place order</button></div>
<aside class="card cart"><h2 style="margin-top:0;font-size:20px">Order summary</h2><div id="cart"></div></aside></div>{% endblock %}
{% block scripts %}<script>
let key = crypto.randomUUID();
window.onCart = c => { renderCart(document.getElementById('cart'), c, false); document.getElementById('place').disabled = !c.items.length || !c.meets_min; };
document.querySelectorAll('input[name=pay]').forEach(r => r.onchange = () => document.getElementById('cardbox').style.display = r.value === 'card' && r.checked ? 'block' : 'none');
document.getElementById('place').onclick = async function(){
  const btn = this, err = document.getElementById('error'); err.textContent = ''; btn.disabled = true; btn.textContent = 'Placing order...';
  const res = await api('/api/checkout', {idempotency_key: key, address: document.getElementById('address').value,
    payment_method: document.querySelector('input[name=pay]:checked').value, card_number: document.getElementById('card').value});
  if (res.ok){ location.href = '/orders/' + res.data.order_id; return; }
  err.textContent = res.data.error || 'Something went wrong'; key = crypto.randomUUID(); btn.disabled = false; btn.textContent = 'Place order';
};
</script>{% endblock %}"""

ORDERS_PAGE = """{% extends "base.html" %}{% block content %}<h1>My orders</h1>
{% for o in orders %}<a class="card row between" style="margin-top:12px" href="/orders/{{ o.id }}"><div><b>Order #{{ o.id }}</b> · {{ o.restaurant }}
<div class="muted">{{ o.created_at }} · {{ o.total|money }}</div></div><span class="pill {{ o.status }}">{{ o.status.replace('_', ' ').title() }}</span></a>
{% else %}<p class="muted">No orders yet. <a href="/restaurants" style="color:var(--accent)">Browse restaurants</a></p>{% endfor %}{% endblock %}"""

ORDER_PAGE = """{% extends "base.html" %}{% block content %}
<div class="row between"><h1 id="title">Order #{{ oid }}</h1><span class="pill" id="status"></span></div><div class="muted" id="sub"></div>
<div class="steps" id="steps"></div>
<div class="layout"><div class="card"><h2 style="margin-top:0;font-size:20px">Behind the scenes</h2>
<div class="muted" style="font-size:14px;margin-bottom:8px">Each line is a service reacting to an event. The page refreshes by itself.</div><div class="log" id="log"></div></div>
<aside class="card cart"><h2 style="margin-top:0;font-size:20px">Your order</h2><div id="items"></div></aside></div>{% endblock %}
{% block scripts %}<script>
const STEPS = [['PLACED','Placed'],['ACCEPTED','Accepted'],['PREPARING','Preparing'],['READY','Ready'],['OUT_FOR_DELIVERY','On the way'],['DELIVERED','Delivered']];
let timer;
async function load(){
  const res = await api('/api/orders/{{ oid }}'); if (!res.ok) return;
  const o = res.data.order, idx = STEPS.findIndex(s => s[0] === o.status);
  const st = document.getElementById('status'); st.textContent = o.status.replace(/_/g, ' ').toLowerCase(); st.className = 'pill ' + o.status;
  document.getElementById('sub').textContent = o.restaurant + ' · ' + o.created_at + (o.courier ? ' · Courier: ' + o.courier : '');
  document.getElementById('steps').innerHTML = STEPS.map((s, i) => '<div class="step ' + (i < idx || o.status === 'DELIVERED' ? 'done' : i === idx ? 'now' : '') + '">' + s[1] + '</div>').join('');
  document.getElementById('log').innerHTML = res.data.events.map(e => '<div class="ev"><b>' + esc(e.source) + '</b>' + esc(e.message) + ' <span class="muted">' + e.created_at + '</span></div>').join('');
  document.getElementById('items').innerHTML = res.data.items.map(i => '<div class="tot"><span>' + i.qty + ' x ' + esc(i.name) + '</span><span>' + money(i.price * i.qty) + '</span></div>').join('') +
    '<div class="tot"><span>Delivery fee</span><span>' + (o.delivery_fee ? money(o.delivery_fee) : 'Free') + '</span></div><div class="tot"><span>Tax</span><span>' + money(o.tax) + '</span></div>' +
    '<div class="tot g"><span>Total</span><span>' + money(o.total) + '</span></div>';
  if (o.status === 'DELIVERED' || o.status === 'PAYMENT_FAILED') clearInterval(timer);
}
load(); timer = setInterval(load, 2000);
</script>{% endblock %}"""

app.jinja_loader = DictLoader({"base.html": BASE, "login.html": LOGIN, "restaurants.html": RESTAURANTS_PAGE,
                               "menu.html": MENU_PAGE, "checkout.html": CHECKOUT_PAGE,
                               "orders.html": ORDERS_PAGE, "order.html": ORDER_PAGE})

init_db()

if __name__ == "__main__":
    print("Foodies is running at http://127.0.0.1:5000  (login: demo@foodies.pk / demo123)")
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
