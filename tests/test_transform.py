import pandas as pd
from etl.transform import transform_orders

def test_transform_basic():
    raw = pd.DataFrame({
        'order_id':[1,2],
        'customer_id':[10,10],
        'order_date':['2025-09-01','2025-09-02'],
        'item':['Pen','Pencil'],
        'quantity':[2,3],
        'price':[1.5, 2.0],
        'city':['London','London'],
        'country':['UK','UK'],
    })
    fact, dim = transform_orders(raw)
    assert len(fact) == 2
    assert len(dim) == 1
    # total_amount checks
    assert fact.loc[fact['order_id']==1, 'total_amount'].iloc[0] == 3.0
    assert dim['order_count'].iloc[0] == 2
    assert dim['lifetime_value'].iloc[0] == 3.0 + 6.0

def test_min_date_filter():
    raw = pd.DataFrame({
        'order_id':[1,2,3],
        'customer_id':[1,1,1],
        'order_date':['2025-09-01','2025-09-03','2025-08-31'],
        'item':['A','B','C'],
        'quantity':[1,1,1],
        'price':[1,1,1],
        'city':['X','X','X'],
        'country':['Y','Y','Y'],
    })
    fact, dim = transform_orders(raw, min_date='2025-09-01')
    # order on 2025-08-31 should be filtered out
    assert set(fact['order_id']) == {1,2}
    assert dim['order_count'].iloc[0] == 2
