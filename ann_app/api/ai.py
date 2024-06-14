from ann_app.utils.api_code import ApiCode
from ann_app.utils.api_response import gen_api_response
from ann_app.utils.db_session import session_scope
from ann_app.db_models import GCPBill
from ann_app.services.ai_service import predict_future_cost
from . import api 

from sqlalchemy import func
from flasgger import swag_from


@api.route(
    '/prediction',
    methods=['GET']
)
@swag_from({
    "tags": ["ai"],
    "responses": {
        "200": {
            "description": "predic cost in the future"
        }
    }
})
def do_prediction():
    """
    Predict cost in the future
    """
    with session_scope() as session:
        # 1. We read data from database, service name: sum(cost)
        results = session.query(
            GCPBill.service_name,
            func.sum(GCPBill.rounded_fee)
        ).group_by(
            GCPBill.service_name
        ).all()
    
        # 2. We turn service name to number 
        mappings = {
            'App Engine': 1,
            'Compute Engine': 2,
            'Networking': 3,
            'BigQuery': 4
        }
    
        # 3. We put data to x, y axis in order for future predictions
        x = []
        y = []
        for item in results:
            x.append(mappings.get(item[0]))
            y.append(float(item[1]))
    
        # For relationship between service and cost, please refer to "prediction.ipynb"
        future_cost, r2 = predict_future_cost(x, y)

    return gen_api_response(
        ApiCode.SUCCESS, 
        {
            "future_cost_for_Compute_Engine": future_cost,
            "accuracy": r2
        }
    )
