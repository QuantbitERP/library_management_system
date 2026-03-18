frappe.query_reports["Project Timeline"] = {
	filters: [
		{
			fieldname: "view_by",
			label: "View By",
			fieldtype: "Select",
			options: "Customer\nUser",
			default: "Customer"
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