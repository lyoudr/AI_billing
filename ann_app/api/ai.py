from ann_app.utils.api_code import ApiCode
from ann_app.utils.api_response import gen_api_response
from ann_app.utils.db_session import session_scope
from ann_app.db_models import BillingReport
from ann_app.services.ai_service import AnnLinearRegression
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
    Predict company cost in the future
    """
    with session_scope() as session:
        # 1. We read data from database, service name: sum(cost)
        results = session.query(BillingReport).all()
    
        # 2. We turn service name to number 
        mappings = {
            'A': 1,
            'B': 2,
            'C': 3,
            'D': 4
        }

        # 3. We put data to x, y axis in order for future predictions
        x = []
        y = []
        for item in results:
            x.append(mappings.get(item.company))
            y.append(float(item.total_cost))

        l_model = AnnLinearRegression(x=x, y=y)
        l_model.define_m_b()
        
        # first loss value
        loss_value = l_model.loss_func(
            l_model.X,
            l_model.Y,
            l_model.b,
            l_model.m
        )
        print(f"first loss value is {loss_value}")

        # gradient descent to optimize m, b
        new_m, new_b = l_model.gradient_descent()

        # optimized model, and its loss value
        new_loss_value = l_model.loss_func(
            l_model.X,
            l_model.Y,
            new_b,
            new_m,
        )
        print(f"new loss value is {new_loss_value}")

    return gen_api_response(
        ApiCode.SUCCESS, 
        {
            "old_loss_value": loss_value,
            "loss_value": new_loss_value
        }
    )
