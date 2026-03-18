# # Copyright (c) 2026, Vaibhav and contributors
# # For license information, please see license.txt

# # import frappe


# def execute(filters=None):
# 	columns, data = [], []
# 	return columns, data


# Copyright (c) 2026
# For license information, please see license.txt

# Copyright (c) 2026, Vaibhav and contributors
# For license information, please see license.txt

import frappe
import json
from collections import defaultdict
from datetime import datetime, time, timedelta

WORKING_HOURS_PER_DAY = 8.5
DAY_START_HOUR = 9
DAY_START_MINUTE = 0


def execute(filters=None):
    filters = filters or {}

    tickets = get_tickets(filters)
    schedule_entries, all_dates = build_schedule(tickets)

    columns = get_columns(all_dates)
    data = get_data(schedule_entries, all_dates)

    return columns, data


def get_tickets(filters):
    conditions = [
        "status NOT IN ('Resolved', 'Closed', 'Hold')",
        "ifnull(custom_expected_working_hours, 0) > 0",
        "ifnull(_assign, '') != ''",
    ]
    values = {}

    if filters.get("customer"):
        conditions.append("customer = %(customer)s")
        values["customer"] = filters.get("customer")

    if filters.get("from_date"):
        conditions.append("date(creation) >= %(from_date)s")
        values["from_date"] = filters.get("from_date")

    if filters.get("to_date"):
        conditions.append("date(creation) <= %(to_date)s")
        values["to_date"] = filters.get("to_date")

    sql = f"""
        SELECT
            name,
            customer,
            creation,
            _assign,
            custom_expected_working_hours
        FROM `tabHD Ticket`
        WHERE {' AND '.join(conditions)}
        ORDER BY creation ASC, name ASC
    """

    rows = frappe.db.sql(sql, values, as_dict=True)

    cleaned = []
    for row in rows:
        assigned_users = parse_assign(row.get("_assign"))
        if not assigned_users:
            continue

        if filters.get("assigned_to") and filters.get("assigned_to") not in assigned_users:
            continue

        cleaned.append({
            "ticket": row.name,
            "customer": row.customer or "Not Set",
            "creation": row.creation,
            "hours": float(row.custom_expected_working_hours or 0),
            "assigned_users": assigned_users,
        })

    return cleaned


def parse_assign(assign_value):
    if not assign_value:
        return []

    try:
        users = json.loads(assign_value)
        if isinstance(users, list):
            return [u for u in users if u]
    except Exception:
        pass

    return []


def build_schedule(tickets):
    """
    Final business rules:
    1. Sort tickets by creation ascending
    2. For multi-user assignment, EACH assigned user gets full task hours
    3. Each user has own queue
    4. User must finish current task before moving to next
    5. After finishing a task, remaining same-day hours can be used for next task
    6. Task cannot start before its creation datetime
    """

    user_queues = defaultdict(list)

    for ticket in tickets:
        for user in ticket["assigned_users"]:
            user_queues[user].append({
                "ticket": ticket["ticket"],
                "customer": ticket["customer"],
                "creation": ensure_datetime(ticket["creation"]),
                "hours": float(ticket["hours"]),
            })

    # schedule_entries[customer][user][date] = ["T1", "T2"]
    schedule_entries = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    all_dates = set()

    for user, queue in user_queues.items():
        queue.sort(key=lambda x: (x["creation"], x["ticket"]))
        current_dt = None

        for task in queue:
            remaining_hours = float(task["hours"])

            if current_dt is None:
                current_dt = normalize_start_datetime(task["creation"])
            else:
                current_dt = max(current_dt, normalize_start_datetime(task["creation"]))

            while remaining_hours > 0:
                current_dt = move_to_working_time(current_dt)

                day_end = get_day_end(current_dt)
                available_today = hours_between(current_dt, day_end)

                if available_today <= 0:
                    current_dt = next_workday_start(current_dt)
                    continue

                allocated = min(remaining_hours, available_today)

                date_key = current_dt.date().strftime("%Y-%m-%d")

                # show ticket no in date column
                schedule_entries[task["customer"]][user][date_key].append(task["ticket"])
                all_dates.add(date_key)

                current_dt = add_hours(current_dt, allocated)
                remaining_hours -= allocated

    return schedule_entries, sorted(all_dates)


def ensure_datetime(value):
    if isinstance(value, datetime):
        return value
    return frappe.utils.get_datetime(value)


def normalize_start_datetime(dt):
    dt = ensure_datetime(dt)
    day_start = datetime.combine(dt.date(), time(DAY_START_HOUR, DAY_START_MINUTE))
    day_end = get_day_end(day_start)

    if dt < day_start:
        return day_start

    if dt >= day_end:
        return next_workday_start(dt)

    return dt


def move_to_working_time(dt):
    dt = ensure_datetime(dt)
    day_start = datetime.combine(dt.date(), time(DAY_START_HOUR, DAY_START_MINUTE))
    day_end = get_day_end(day_start)

    if dt < day_start:
        return day_start

    if dt >= day_end:
        return next_workday_start(dt)

    return dt


def next_workday_start(dt):
    next_day = ensure_datetime(dt).date() + timedelta(days=1)
    return datetime.combine(next_day, time(DAY_START_HOUR, DAY_START_MINUTE))


def get_day_end(dt):
    day_start = datetime.combine(ensure_datetime(dt).date(), time(DAY_START_HOUR, DAY_START_MINUTE))
    return add_hours(day_start, WORKING_HOURS_PER_DAY)


def hours_between(start_dt, end_dt):
    seconds = (end_dt - start_dt).total_seconds()
    return max(seconds / 3600.0, 0)


def add_hours(dt, hours):
    return ensure_datetime(dt) + timedelta(hours=hours)


def scrub_fieldname(value):
    return value.replace("-", "_")


def get_columns(all_dates):
    columns = [
        # helper fields for tree/collapse
        {
            "label": "Row ID",
            "fieldname": "row_id",
            "fieldtype": "Data",
            "hidden": 1,
        },
        {
            "label": "Parent Row",
            "fieldname": "parent_row",
            "fieldtype": "Data",
            "hidden": 1,
        },
        {
            "label": "Indent",
            "fieldname": "indent",
            "fieldtype": "Int",
            "hidden": 1,
        },
        {
            "label": "Customer",
            "fieldname": "customer",
            "fieldtype": "Link",
            "options": "HD Customer",
            "width": 220,
        },
        {
            "label": "User",
            "fieldname": "user",
            "fieldtype": "Link",
            "options": "User",
            "width": 220,
        },
    ]

    for date_str in all_dates:
        columns.append({
            "label": frappe.utils.formatdate(date_str),
            "fieldname": scrub_fieldname(date_str),
            "fieldtype": "Data",
            "width": 180,
        })

    return columns


def get_data(schedule_entries, all_dates):
    data = []

    for customer in sorted(schedule_entries.keys()):
        # parent row = customer
        parent_row_id = f"CUST::{customer}"
        parent_row = {
            "row_id": parent_row_id,
            "parent_row": "",
            "indent": 0,
            "customer": customer,
            "user": "",
        }

        # optional summary on parent row
        for date_str in all_dates:
            fieldname = scrub_fieldname(date_str)
            summary_tickets = []
            for user in sorted(schedule_entries[customer].keys()):
                summary_tickets.extend(schedule_entries[customer][user].get(date_str, []))
            parent_row[fieldname] = ", ".join(summary_tickets)

        data.append(parent_row)

        # child rows = users under customer
        for user in sorted(schedule_entries[customer].keys()):
            child_row = {
                "row_id": f"{parent_row_id}::USER::{user}",
                "parent_row": parent_row_id,
                "indent": 1,
                "customer": "",
                "user": user,
            }

            for date_str in all_dates:
                fieldname = scrub_fieldname(date_str)
                tickets = schedule_entries[customer][user].get(date_str, [])
                child_row[fieldname] = ", ".join(tickets)

            data.append(child_row)

    return data