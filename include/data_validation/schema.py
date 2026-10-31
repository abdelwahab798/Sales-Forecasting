import pandera as pa
from pandera import Column, Check, DataFrameSchema
import pandas as pd


PANDERA_TYPE_MAP = {
    "int": pa.Int,
    "float": pa.Float64,
    "datetime": pa.DateTime,
    "str": pa.String,
    "object": pa.String,
    "bool": pa.Bool,
}

def build_pandera_schema(table_rules: dict, allow_nulls: bool = False) -> DataFrameSchema:
    columns_spec = {}
    
    for col_name, col_type in table_rules.items():
        pa_type = PANDERA_TYPE_MAP.get(str(col_type).lower(), pa.String)
        
        checks = []
        if any(k in col_name for k in ["revenue", "cost", "profit", "quantity", "traffic", "inventory"]):
            checks.append(Check.ge(0, error=f"{col_name} must be non-negative"))
            
        columns_spec[col_name] = Column(
            dtype=pa_type,
            checks=checks if checks else None,
            nullable=allow_nulls,
            required=True
        )
        
    return DataFrameSchema(
        columns=columns_spec,
        coerce=True,      
        strict=False      
)