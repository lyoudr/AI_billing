from flask_cqlalchemy import Model, columns


class Report(Model):
    __table__ = 'report'

    id = columns.Integer(primary_key=True)
    company_id = columns.Integer() # ForeignKey referencing the company ID
    month = columns.Text()
    service = columns.Text()
    cost = columns.Decimal()