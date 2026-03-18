// Copyright (c) 2026, Vaibhav and contributors
// For license information, please see license.txt

// frappe.query_reports["Project Timeline"] = {
// 	"filters": [

// 	]
// };

frappe.query_reports["Project Timeline"] = {
	filters: [
		{
			fieldname: "from_date",
			label: "From Date",
			fieldtype: "Date"
		},
		{
			fieldname: "to_date",
			label: "To Date",
			fieldtype: "Date"
		},
		{
			fieldname: "customer",
			label: "Customer",
			fieldtype: "Link",
			options: "HD Customer"
		},
		{
			fieldname: "assigned_to",
			label: "Assigned To",
			fieldtype: "Link",
			options: "User"
		}
	],

	tree: true,
	name_field: "row_id",
	parent_field: "parent_row",
	initial_depth: 1
};