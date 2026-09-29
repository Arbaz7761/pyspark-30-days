"""Generates small sample datasets used across the 30 days. Run once: python generate_data.py"""
import csv, json, random, pathlib
from datetime import date, timedelta

random.seed(42)
D = pathlib.Path(__file__).parent / "data"
D.mkdir(exist_ok=True)

# departments
depts = [(10, "Engineering"), (20, "Sales"), (30, "HR"), (40, "Finance"), (50, "Marketing")]
with open(D / "departments.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["dept_id", "dept_name"]); w.writerows(depts)

# employees (with nulls + duplicates on purpose)
names = ["Arbaz", "Aman", "Riya", "Neha", "Rahul", "Sana", "Vikram", "Pooja", "Karan", "Isha",
         "Zoya", "Dev", "Meera", "Farhan", "Tara", "Nikhil", "Ayesha", "Rohan", "Kiran", "Sameer"]
cities = ["Patna", "Bhopal", "Delhi", "Mumbai", "Pune", "Bangalore"]
rows = []
for i, n in enumerate(names, start=1):
    dept = random.choice([10, 10, 20, 30, 40, 50, 60 if i == 20 else 10])  # 60 -> no matching dept
    salary = random.choice([35000, 42000, 50000, 65000, 80000, 95000, ""]) if i % 7 else ""
    join = date(2020, 1, 1) + timedelta(days=random.randint(0, 1800))
    email = f"{n.lower()}@example.com" if i % 5 else ""
    manager = "" if i <= 3 else random.randint(1, 3)
    rows.append([i, n, dept, salary, random.choice(cities), join.isoformat(), email, manager])
rows.append(rows[4][:])  # exact duplicate row
with open(D / "employees.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["emp_id", "name", "dept_id", "salary", "city", "join_date", "email", "manager_id"])
    w.writerows(rows)

# customers
with open(D / "customers.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["customer_id", "customer_name", "city", "segment"])
    for i in range(1, 31):
        w.writerow([i, f"Customer_{i}", random.choice(cities), random.choice(["Retail", "Corporate", "SME"])])

# sales
products = {"Laptop": ("Electronics", 55000), "Phone": ("Electronics", 25000), "Desk": ("Furniture", 9000),
            "Chair": ("Furniture", 4500), "Notebook": ("Stationery", 120), "Pen": ("Stationery", 20)}
with open(D / "sales.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_id", "product", "category", "qty", "price", "order_date", "region", "status"])
    for i in range(1, 501):
        p = random.choice(list(products))
        cat, price = products[p]
        d = date(2025, 1, 1) + timedelta(days=random.randint(0, 364))
        w.writerow([i, random.randint(1, 35), p, cat, random.randint(1, 5), price, d.isoformat(),
                    random.choice(["North", "South", "East", "West"]),
                    random.choice(["Completed", "Completed", "Completed", "Cancelled", "Pending"])])

# nested json (arrays + structs)
orders = []
for i in range(1, 11):
    orders.append({
        "order_id": i,
        "customer": {"id": random.randint(1, 30), "name": f"Customer_{i}", "address": {"city": random.choice(cities), "pin": str(800000 + i)}},
        "items": [{"sku": f"SKU{random.randint(1, 9)}", "qty": random.randint(1, 4), "price": random.choice([100, 250, 500])}
                  for _ in range(random.randint(1, 3))],
        "tags": random.sample(["gift", "express", "fragile", "bulk", "online"], k=random.randint(1, 3)),
    })
with open(D / "orders_nested.json", "w") as f:
    for o in orders:
        f.write(json.dumps(o) + "\n")

# messy raw file for ETL / data quality days
with open(D / "raw_orders_dirty.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "customer_email", "amount", "order_date", "country"])
    w.writerows([
        [1, "  ARBAZ@Example.com ", "1200.50", "2025-01-05", "india"],
        [2, "riya@example.com", "-50", "2025-01-06", "India"],
        [3, "", "300", "2025-01-07", "INDIA"],
        [3, "", "300", "2025-01-07", "INDIA"],
        [4, "neha@example", "abc", "2025-13-45", "india"],
        [5, "sana@example.com", "800", "2025-02-01", " India "],
        [6, "rahul@example.com", "450.75", "2025-02-03", "USA"],
    ])
print("Sample data written to", D)
