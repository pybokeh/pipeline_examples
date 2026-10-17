To understand some aspects of dagster code (ie configurable resources), you must be familiar with Pydantic "models"
In a sense, Pydantic makes a class behave or look like a function.

Basic Pydantic example:

```python
from pydantic import BaseModel

class DbConfig(BaseModel):
    host: str
    port: int = 5432

DbConfig(host="localhost")            # fine, port defaults to 5432
DbConfig(host="localhost", port="5433")  # "5433" is coerced to int 5433
DbConfig()                            # ValidationError: host is required
```