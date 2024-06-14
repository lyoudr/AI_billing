from flask_cqlalchemy import Model, columns


class Company(Model):
    __table__ = 'company'

    id = columns.Integer(primary_key=True)
    name = columns.Text()
    reports = columns.List(columns.Integer) # Store a list of report IDs associated with the company