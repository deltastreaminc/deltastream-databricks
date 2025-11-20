import asyncio
import json
import pandas as pd
from deltastream.api.conn import APIConnection
from pyspark.sql.functions import explode, col, from_json, schema_of_json, lit

def query_deltastream(
    spark,
    sql_query,
    api_scope="myscope",
    api_key_name="ds_api",
    token_key_name="ds_token"
):
    try:
        dbutils  # noqa: F821
    except NameError:
        raise RuntimeError("dbutils is not available. Must run inside Databricks.")

    def get_secret_or_fail(scope, key):
        try:
            value = dbutils.secrets.get(scope=scope, key=key)
        except Exception as e:
            raise RuntimeError(
                f"Unable to retrieve secret '{key}' from scope '{scope}'. Error: {e}"
            )
        if value is None or value.strip() == "":
            raise RuntimeError(
                f"Secret '{key}' in scope '{scope}' is empty or not set."
            )
        return value

    api_url = get_secret_or_fail(api_scope, api_key_name)
    api_key = get_secret_or_fail(api_scope, token_key_name)

    def async_query_partition(iterator):

        async def execute_query(sql_query, api, key):
            dsn = f"https://:{key}@{api}"
            conn = APIConnection.from_dsn(dsn)

            try:
                rows = await conn.query(sql_query)
                results = []
                columns = rows.columns()

                async for row in rows:
                    data = {}
                    for idx, colinfo in enumerate(columns):
                        value = row[idx]

                        # Normalize non-JSON types
                        if hasattr(value, "isoformat"):
                            value = value.isoformat()
                        else:
                            try:
                                json.dumps(value)
                            except Exception:
                                value = str(value)

                        data[colinfo.name] = value

                    results.append(json.dumps(data, default=str))

                return results

            except Exception as e:
                return [json.dumps({"error": str(e), "query": sql_query})]

        async def process_batch(queries, api, key):
            tasks = [execute_query(q, api, key) for q in queries]
            return await asyncio.gather(*tasks)

        for pdf in iterator:
            queries = pdf["sql_query"].tolist()
            results = asyncio.run(process_batch(queries, api_url, api_key))
            pdf["results"] = results
            yield pdf

    queries_df = spark.createDataFrame([(sql_query,)], ["sql_query"])

    result_df = queries_df.mapInPandas(
        async_query_partition,
        schema="sql_query string, results array<string>"
    )

    exploded_df = result_df.select(explode(col("results")).alias("json_result"))

    sample_json = exploded_df.select("json_result").first()[0]
    json_schema = schema_of_json(lit(sample_json))

    parsed_df = (
        exploded_df
        .select(from_json(col("json_result"), json_schema).alias("data"))
        .select("data.*")
    )

    return parsed_df
