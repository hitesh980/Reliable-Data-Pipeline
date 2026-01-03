from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)


def generate_orders(num_records=500):
    rows = []

    base_time = datetime.now() - timedelta(hours=2)

    for i in range(num_records):
        order_id = 100000 + i

        # Don't create duplicates - each order should be unique
        # Remove the problematic duplicate logic:
        # if i % 120 == 0:
        #     order_id = 100000

        user_id = fake.random_int(min=1, max=5000)

        # Inject missing user_id
        if i % 90 == 0:
            user_id = None

        amount = round(random.uniform(5, 500), 2)

        # Inject null amount
        if i % 75 == 0:
            amount = None

        rows.append({
            "order_id": order_id,
            "user_id": user_id,
            "order_amount": amount,
            "currency": "USD",
            "order_ts": base_time + timedelta(seconds=i * 5),
            "status": random.choice(["COMPLETED", "FAILED", "PENDING"])
        })
    df = pd.DataFrame(rows)
    df.to_csv("/opt/airflow/data/raw/orders.csv", index=False)


if __name__ == "__main__":
    generate_orders()
