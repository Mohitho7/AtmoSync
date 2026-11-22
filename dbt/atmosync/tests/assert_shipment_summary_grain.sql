select shipment_id, container_id
from {{ ref('mart_shipment_risk_summary') }}
group by shipment_id, container_id
having count(*) <> 1
