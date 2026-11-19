import uuid

import app as foodies

ADDRESS = "House 12, Block 5, Clifton, Karachi"


def add(client, item_id, times=1):
    for _ in range(times):
        client.post("/api/cart/add", json={"item_id": item_id})


# ---------- version and health (this is what the pipeline checks in production) ----------
def test_health_reports_version(client):
    data = client.get("/health").get_json()
    assert data["status"] == "ok"
    assert data["version"] == foodies.APP_VERSION


def test_version_is_shown_on_login_page(client):
    assert f"Foodies v{foodies.APP_VERSION}".encode() in client.get("/login").data


# ---------- authentication ----------
def test_login_page_loads(client):
    assert client.get("/login").status_code == 200


def test_wrong_password_is_rejected(client):
    page = client.post("/login", data={"email": "demo@foodies.pk", "password": "wrong"})
    assert b"Wrong email or password" in page.data


def test_correct_login_redirects_to_restaurants(client):
    res = client.post("/login", data={"email": "demo@foodies.pk", "password": "demo123"})
    assert res.status_code == 302 and "/restaurants" in res.headers["Location"]


def test_api_requires_login(client):
    assert client.get("/api/cart").status_code == 401


# ---------- payment rules ----------
def test_cash_on_delivery_always_succeeds():
    assert foodies.charge(500, "cod", "")[0] is True


def test_card_rules():
    assert foodies.charge(500, "card", "1234")[0] is False                 # too short
    assert foodies.charge(500, "card", "4111 1111 1111 0000")[0] is False  # declined by the bank
    assert foodies.charge(500, "card", "4242 4242 4242 4242")[0] is True


# ---------- cart business logic ----------
def test_cart_totals(logged_in):
    add(logged_in, 1, times=2)  # 2 x Chicken Biryani (450)
    cart = logged_in.get("/api/cart").get_json()
    assert (cart["subtotal"], cart["delivery_fee"], cart["tax"], cart["total"]) == (900, 120, 45, 1065)


def test_free_delivery_above_threshold(logged_in):
    add(logged_in, 3, times=3)  # 3 x Mutton Biryani (750) = 2250
    assert logged_in.get("/api/cart").get_json()["delivery_fee"] == 0


def test_cart_accepts_one_restaurant_only(logged_in):
    add(logged_in, 1)
    res = logged_in.post("/api/cart/add", json={"item_id": 6})  # item from Pizza Corner
    assert res.status_code == 409


def test_minimum_order_is_enforced(logged_in):
    add(logged_in, 4)  # Raita (60) is below the Rs 500 minimum
    res = logged_in.post("/api/checkout", json={"idempotency_key": str(uuid.uuid4()), "address": ADDRESS})
    assert res.status_code == 400 and "Minimum order" in res.get_json()["error"]


# ---------- checkout ----------
def test_checkout_is_idempotent(logged_in):
    add(logged_in, 1, times=2)
    body = {"idempotency_key": str(uuid.uuid4()), "address": ADDRESS, "payment_method": "cod"}
    first = logged_in.post("/api/checkout", json=body).get_json()
    second = logged_in.post("/api/checkout", json=body).get_json()
    assert second["order_id"] == first["order_id"] and second["duplicate"] is True


def test_declined_card_fails_and_keeps_cart(logged_in):
    add(logged_in, 1, times=2)
    res = logged_in.post("/api/checkout", json={"idempotency_key": str(uuid.uuid4()), "address": ADDRESS,
                                                "payment_method": "card", "card_number": "4111111111110000"})
    assert res.status_code == 402
    assert len(logged_in.get("/api/cart").get_json()["items"]) == 1
