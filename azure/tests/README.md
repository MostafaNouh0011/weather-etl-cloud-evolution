# PySpark unit tests

Optional `pytest` + `chispa` setup for the Silver and Gold transformation
logic. Out of scope for the first cut but the folder is reserved so it's
easy to add later.

## Planned layout

```
tests/
├── conftest.py                  # Spark session fixture
├── test_weather_client.py       # Mocked HTTP calls
├── test_silver_schema.py        # Verifies the cleansing rules produce the expected schema
└── test_gold_aggregations.py    # Verifies the hourly + daily MERGE produces the right values
```

## How it will run

```bash
cd azure
pip install pytest chispa
pytest tests/
```

## Reference

- [`../databricks/README.md`](../databricks/README.md) — the code under test
- [`../../../docs/evolution.md`](../../../docs/evolution.md#open-questions-for-the-future) — tests are a deliberate not-now
