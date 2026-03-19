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

    holiday_map = get_holiday_map(filters)
    tickets = get_tickets(filters)
    schedule_entries, all_dates = build_schedule(tickets, holiday_map)

    columns = get_columns(all_dates, holiday_map)
    data = get_data(schedule_entries, all_dates, filters, holiday_map)

    return columns, data


def get_holiday_map(filters):
    holiday_map = {}

    company = frappe.defaults.get_user_default("Company") or frappe.defaults.get_user_default("company")
    if not company:
        return holiday_map

    holiday_list = frappe.db.get_value("Company", company, "default_holiday_list")
    if not holiday_list:
        return holiday_map

    holidays = frappe.get_all(
        "Holiday",
        filters={"parent": holiday_list},
        fields=["holiday_date", "description"],
        order_by="holiday_date asc"
    )

    for row in holidays:
        if row.get("holiday_date"):
            holiday_map[str(row["holiday_date"])] = row.get("description") or "Holiday"

    return holiday_map


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

    sql = f"""
        SELECT
            name,
            customer,
            creation,
            status,
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
            "status": row.status or "",
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


def build_schedule(tickets, holiday_map):
    """
    Rules:
    1. Plan from today onward
    2. Status 'Working' gets highest priority
    3. After that, priority by creation ASC
    4. If multiple users assigned, each user gets full task hours separately
    5. User finishes current task first, then next
    6. Remaining same-day hours can be used for next task
    7. Holidays are skipped for planning
    """

    user_queues = defaultdict(list)

    for ticket in tickets:
        for user in ticket["assigned_users"]:
            user_queues[user].append({
                "ticket": ticket["ticket"],
                "customer": ticket["customer"],
                "creation": ensure_datetime(ticket["creation"]),
                "status": ticket["status"],
                "hours": float(ticket["hours"]),
            })

    # schedule_entries[customer][user][date] = [{"ticket": "HDT-0001", "hours": 6}]
    schedule_entries = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    all_dates = set()

    planning_start = get_today_work_start()

    for user, queue in user_queues.items():
        queue.sort(
            key=lambda x: (
                0 if str(x.get("status") or "").strip().lower() == "working" else 1,
                x["creation"],
                x["ticket"]
            )
        )

        current_dt = planning_start

        for task in queue:
            remaining_hours = float(task["hours"])

            while remaining_hours > 0:
                current_dt = move_to_working_time(current_dt, holiday_map)

                day_end = get_day_end(current_dt)
                available_today = hours_between(current_dt, day_end)

                if available_today <= 0:
                    current_dt = next_workday_start(current_dt, holiday_map)
                    continue

                allocated = min(remaining_hours, available_today)

                date_key = current_dt.date().strftime("%Y-%m-%d")
                schedule_entries[task["customer"]][user][date_key].append({
                    "ticket": task["ticket"],
                    "hours": allocated
                })
                all_dates.add(date_key)

                current_dt = add_hours(current_dt, allocated)
                remaining_hours -= allocated

    all_dates = add_holiday_dates_in_range(all_dates, holiday_map)

    return schedule_entries, sorted(all_dates)


def ensure_datetime(value):
    if isinstance(value, datetime):
        return value
    return frappe.utils.get_datetime(value)


def get_today_work_start():
    today = frappe.utils.nowdate()
    return frappe.utils.get_datetime(
        today + " " + str(DAY_START_HOUR).zfill(2) + ":" + str(DAY_START_MINUTE).zfill(2) + ":00"
    )


def add_hours(dt, hours):
    return ensure_datetime(dt) + timedelta(hours=hours)


def get_day_end(dt):
    day_start = datetime.combine(ensure_datetime(dt).date(), time(DAY_START_HOUR, DAY_START_MINUTE))
    return add_hours(day_start, WORKING_HOURS_PER_DAY)


def is_holiday(dt, holiday_map):
    date_str = ensure_datetime(dt).date().strftime("%Y-%m-%d")
    return date_str in holiday_map


def move_to_working_time(dt, holiday_map):
    dt = ensure_datetime(dt)

    while True:
        day_start = datetime.combine(dt.date(), time(DAY_START_HOUR, DAY_START_MINUTE))
        day_end = get_day_end(day_start)

        if is_holiday(dt, holiday_map):
            dt = next_workday_start(dt, holiday_map)
            continue

        if dt < day_start:
            return day_start

        if dt >= day_end:
            dt = next_workday_start(dt, holiday_map)
            continue

        return dt


def next_workday_start(dt, holiday_map):
    next_day = ensure_datetime(dt).date() + timedelta(days=1)

    while str(next_day) in holiday_map:
        next_day = next_day + timedelta(days=1)

    return datetime.combine(next_day, time(DAY_START_HOUR, DAY_START_MINUTE))


def hours_between(start_dt, end_dt):
    seconds = (end_dt - start_dt).total_seconds()
    return max(seconds / 3600.0, 0)


def scrub_fieldname(value):
    return value.replace("-", "_")


def format_hours(hours):
    if float(hours).is_integer():
        return str(int(hours))
    return f"{hours:.2f}".rstrip("0").rstrip(".")


def format_items(items):
    return ", ".join([
        item["ticket"] + "(" + format_hours(item["hours"]) + "h)"
        for item in items
    ])


def add_holiday_dates_in_range(all_dates, holiday_map):
    if not all_dates:
        return sorted(list(set(holiday_map.keys())))

    all_date_list = sorted(list(all_dates))
    start_date = frappe.utils.getdate(all_date_list[0])
    end_date = frappe.utils.getdate(all_date_list[-1])

    date_set = set(all_dates)

    current = start_date
    while current <= end_date:
        date_str = str(current)
        if date_str in holiday_map:
            date_set.add(date_str)
        current = current + timedelta(days=1)

    return sorted(list(date_set))


def get_columns(all_dates, holiday_map):
    columns = [
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
        label = frappe.utils.formatdate(date_str)
        if date_str in holiday_map:
            label = label + " (Holiday)"

        columns.append({
            "label": label,
            "fieldname": scrub_fieldname(date_str),
            "fieldtype": "Data",
            "width": 220,
        })

    return columns


def get_data(schedule_entries, all_dates, filters, holiday_map):
    view_by = filters.get("view_by") or "Customer"

    if view_by == "User":
        return get_user_wise_data(schedule_entries, all_dates, holiday_map)

    return get_customer_wise_data(schedule_entries, all_dates, holiday_map)


def get_customer_wise_data(schedule_entries, all_dates, holiday_map):
    data = []

    for customer in sorted(schedule_entries.keys()):
        parent_row_id = f"CUST::{customer}"

        parent_row = {
            "row_id": parent_row_id,
            "parent_row": "",
            "indent": 0,
            "customer": customer,
            "user": "",
        }

        for date_str in all_dates:
            fieldname = scrub_fieldname(date_str)

            if date_str in holiday_map:
                parent_row[fieldname] = "Holiday"
            else:
                summary_items = []
                for user in sorted(schedule_entries[customer].keys()):
                    summary_items.extend(schedule_entries[customer][user].get(date_str, []))
                parent_row[fieldname] = format_items(summary_items)

        data.append(parent_row)

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

                if date_str in holiday_map:
                    child_row[fieldname] = "Holiday"
                else:
                    items = schedule_entries[customer][user].get(date_str, [])
                    child_row[fieldname] = format_items(items)

            data.append(child_row)

    return data


def get_user_wise_data(schedule_entries, all_dates, holiday_map):
    data = []

    user_map = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for customer in schedule_entries:
        for user in schedule_entries[customer]:
            for date_str in schedule_entries[customer][user]:
                user_map[user][customer][date_str].extend(
                    schedule_entries[customer][user][date_str]
                )

    for user in sorted(user_map.keys()):
        parent_row_id = f"USER::{user}"

        parent_row = {
            "row_id": parent_row_id,
            "parent_row": "",
            "indent": 0,
            "customer": "",
            "user": user,
        }

        for date_str in all_dates:
            fieldname = scrub_fieldname(date_str)

            if date_str in holiday_map:
                parent_row[fieldname] = "Holiday"
            else:
                summary_items = []
                for customer in sorted(user_map[user].keys()):
                    summary_items.extend(user_map[user][customer].get(date_str, []))
                parent_row[fieldname] = format_items(summary_items)

        data.append(parent_row)

        for customer in sorted(user_map[user].keys()):
            child_row = {
                "row_id": f"{parent_row_id}::CUST::{customer}",
                "parent_row": parent_row_id,
                "indent": 1,
                "customer": customer,
                "user": "",
            }

            for date_str in all_dates:
                fieldname = scrub_fieldname(date_str)

                if date_str in holiday_map:
                    child_row[fieldname] = "Holiday"
                else:
                    items = user_map[user][customer].get(date_str, [])
                    child_row[fieldname] = format_items(items)

            data.append(child_row)

    return data