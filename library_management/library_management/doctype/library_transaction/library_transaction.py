# import frappe
from frappe.model.document import Document
from frappe.utils import add_days, getdate
import frappe

class LibraryTransaction(Document):
    # This method runs before the document is saved
    def before_save(self):
        # Set the return date 14 days from the issue date
        self.return_date = add_days(self.issue_date, 14)

    # This method runs every time the document is submitted
    def on_submit(self):
        # Get the related Book document
        book = frappe.get_doc("Book", self.book)
        
        # Check if the book's status is "Available"
        if book.status == "Available":
            # If it is available, set its status to "Issued" and save the book document
            book.status = "Issued"
            book.save()
        else:
            # If the book is not available, stop the transaction and show an error
            frappe.throw("This book has already been issued.")

    # This method runs when the transaction is cancelled (returned)
    def on_cancel(self):
        # Get the related Book document
        book = frappe.get_doc("Book", self.book)
        
        # Set the book's status back to "Available"
        book.status = "Available"
        book.save()