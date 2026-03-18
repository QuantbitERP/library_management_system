# # Copyright (c) 2026, Vaibhav and contributors
# # For license information, please see license.txt

# # import frappe


# def execute(filters=None):
# 	columns, data = [], []
# 	return columns, data


# Copyright (c) 2026
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
    schedule_entries, all_dates = build_schedule(tickets, filters)

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

        if filters.get("assigned_to"):
            if filters.get("assigned_to") not in assigned_users:
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


def build_schedule(tickets, filters):
    """
    Final business rules implemented:
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

    # schedule_entries[customer][date] = ["x(T1:2.5)", "y(T2:4)"]
    schedule_entries = defaultdict(lambda: defaultdict(list))
    all_dates = set()

    for user, queue in user_queues.items():
        queue.sort(key=lambda x: (x["creation"], x["ticket"]))

        current_dt = None

        for task in queue:
            remaining_hours = float(task["hours"])

            # user cannot start task before task creation
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
                hours_text = format_hours(allocated)
                schedule_entries[task["customer"]][date_key].append(
                    f"{user}({task['ticket']}:{hours_text})"
                )
                all_dates.add(date_key)

                current_dt = add_hours(current_dt, allocated)
                remaining_hours -= allocated

    return schedule_entries, sorted(all_dates)


def ensure_datetime(value):
    if isinstance(value, datetime):
        return value
    return frappe.utils.get_datetime(value)


def normalize_start_datetime(dt):
    """
    If task created before work start, schedule from day start.
    If created during work hours, use actual creation time.
    If created after work hours, move to next workday start.
    """
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


def format_hours(hours):
    if float(hours).is_integer():
        return str(int(hours))
    return f"{hours:.2f}".rstrip("0").rstrip(".")


def get_columns(all_dates):
    columns = [
        {
            "label": "Customer",
            "fieldname": "customer",
            "fieldtype": "Link",
            "options": "Customer",
            "width": 180,
        }
    ]

    for date_str in all_dates:
        columns.append({
            "label": frappe.utils.formatdate(date_str),
            "fieldname": scrub_fieldname(date_str),
            "fieldtype": "Data",
            "width": 220,
        })

    return columns


def get_data(schedule_entries, all_dates):
    data = []

    for customer in sorted(schedule_entries.keys()):
        row = {"customer": customer}

        for date_str in all_dates:
            fieldname = scrub_fieldname(date_str)
            values = schedule_entries[customer].get(date_str, [])
            row[fieldname] = ", ".join(values)

        data.append(row)

    return data


def scrub_fieldname(value):
    return value.replace("-", "_")