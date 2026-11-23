select 
    count(*) 
from 
    {{source('walmart_databricks', 'customers')}}

