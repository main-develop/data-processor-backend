import dask.dataframe as dd
import pandas as pd
import numpy as np
import pyparsing as pp
from pandas.api.types import is_numeric_dtype
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from ..models.dataset_form import GeneralDatasetForm, ProcessingType
from ..utils.try_parse_datetime import try_parse_datetime
from ..websocket_manager import websocket_send_progress
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

process_dataset_router = APIRouter()

@process_dataset_router.post("/process-dataset")
async def process_dataset(dataset_form: GeneralDatasetForm):
    filename_matches = list(Path("uploads").glob(f"{dataset_form.session_id}*"))
    if not filename_matches:
        raise HTTPException(status_code=400, detail="No file found for session")

    file_path = filename_matches[0]
    file_extension = file_path.suffix

    logger.info(f"Loading dataset for {dataset_form.session_id}")
    await websocket_send_progress(dataset_form.session_id, 0, "Loading dataset...")

    try:
        if file_extension in {".csv", ".txt"}:
            df: dd.DataFrame = dd.read_csv(file_path, encoding="utf-8", assume_missing=True, sample=250000)
        elif file_extension in {".parquet", ".pq"}:
            df = dd.read_parquet(file_path)
        elif file_extension in {".xls", ".xlsx"}:
            pandas_df = pd.read_excel(file_path, nrows=10000 if dataset_form.is_sample_data else None)
            df = dd.from_pandas(pandas_df, npartitions=4)

        logger.info(f"Dataset loaded for {dataset_form.session_id}")
        await websocket_send_progress(dataset_form.session_id, 20, "Dataset loaded, processing...")

        numeric_cols = [col for col in df.columns if is_numeric_dtype(df.dtypes[col])]
        result = None
        graphs = None

        if dataset_form.processing_type == ProcessingType.FILTER_DATA:
            column = pp.Word(pp.alphas + '_' + pp.nums)
            operator = pp.oneOf("> < == >= <= !=")
            value = pp.Word(pp.alphanums + '.')
            condition_expr = column + operator + value

            try:
                parsed = condition_expr.parseString(dataset_form.condition)
                column, operator, value = parsed

                if column not in df.columns:
                    raise ValueError("Invalid column")
                
                logger.info(f"Applying filter for {dataset_form.session_id}")
                await websocket_send_progress(dataset_form.session_id, 40, "Applying filter...")

                match operator:
                    case ">":
                        df_filtered = df[df[column] > float(value)]
                    case "<":
                        df_filtered = df[df[column] < float(value)]
                    case "==":
                        df_filtered = df[df[column] == float(value)]
                    case ">=":
                        df_filtered = df[df[column] >= float(value)]
                    case "<=":
                        df_filtered = df[df[column] <= float(value)]
                    case "!=":
                        df_filtered = df[df[column] != float(value)]
                
                logger.info(f"Generating charts for {dataset_form.session_id}")
                await websocket_send_progress(dataset_form.session_id, 60, "Generating charts...")

                if is_numeric_dtype(df.dtypes[column]):
                    filtered_values = df_filtered[column].compute()
                    counts, bins = np.histogram(filtered_values, bins=10)
                    value_key = f"{column}_count"
                    chart_data = [{"bin": round(float(bins[i]), 2), value_key: int(counts[i])} for i in range(len(counts))]
                    graphs = [{"chart_type": "histogram", "chart_data": chart_data, "value_key": value_key}]

                    datetime_cols = [col for col in df.columns if "datetime" in str(df.dtypes[col])]
                    if datetime_cols and len(numeric_cols) > 1:
                        time_col = datetime_cols[0]
                        other_numeric = [col for col in numeric_cols if col != column][0]
                        time_series = df_filtered.groupby(time_col)[other_numeric].mean().compute()
                        line_data = [
                            {"date": idx.strftime("%Y-%m-%d"), other_numeric: round(float(val), 2)}
                            for idx, val in time_series.items()
                        ]
                        graphs.append({"chart_type": "line", "chart_data": line_data, "value_key": other_numeric})
                else:
                    value_counts = df_filtered[column].value_counts().compute()
                    chart_data = [{"name": str(cat), "value": int(cnt)} for cat, cnt in value_counts.items()]
                    graphs = [{"chart_type": "bar", "chart_data": chart_data, "value_key": "value"}]

                result = df_filtered.head(5).to_dict(orient="records")
                # Convert Timestamps to strings
                for row in result:
                    for key, value in row.items():
                        if isinstance(value, pd.Timestamp):
                            row[key] = value.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"Finalizing filter results for {dataset_form.session_id}")
                await websocket_send_progress(dataset_form.session_id, 80, "Finalizing results...")
            except Exception as e:
                logger.error(f"Filter error for {dataset_form.session_id}: {str(e)}")
                await websocket_send_progress(dataset_form.session_id, 0, f"Error: {str(e)}")
                raise HTTPException(status_code=400, detail=str(e))

        elif dataset_form.processing_type == ProcessingType.AGGREGATE_DATA:
            if dataset_form.group_by_column not in df.columns or dataset_form.aggregation_column not in df.columns:
                raise HTTPException(status_code=400, detail="Invalid column")
            
            logger.info(f"Aggregating data for {dataset_form.session_id}")
            await websocket_send_progress(dataset_form.session_id, 40, "Aggregating data...")
            agg_result = df.groupby(dataset_form.group_by_column)[dataset_form.aggregation_column].agg(dataset_form.aggregation_function.value).compute()
            chart_data = [{"name": idx.strftime("%Y-%m-%d") if isinstance(idx, pd.Timestamp) else str(idx), dataset_form.aggregation_column: round(float(val), 2)} for idx, val in agg_result.items()]
            chart_type = "bar"
            if "datetime" in str(df.dtypes[dataset_form.group_by_column]):
                chart_type = "area"

            top_5_columns = agg_result.head(5)
            result_column = f"{dataset_form.aggregation_column} ({dataset_form.aggregation_function.value})"
            graphs = [{"chart_type": chart_type, "chart_data": chart_data, "value_key": dataset_form.aggregation_column}]
            result = [{dataset_form.group_by_column: idx.strftime("%Y-%m-%d") if isinstance(idx, pd.Timestamp) else str(idx), result_column: round(float(val), 2)} for idx, val in top_5_columns.items()]
            logger.info(f"Finalizing aggregation results for {dataset_form.session_id}")
            await websocket_send_progress(dataset_form.session_id, 80, "Finalizing results...")

        elif dataset_form.processing_type == ProcessingType.GENERATE_SUMMARY:
            logger.info(f"Generating summary for {dataset_form.session_id}")
            await websocket_send_progress(dataset_form.session_id, 40, "Generating summary...")
            means = df[numeric_cols].mean().compute()
            medians = df[numeric_cols].median_approximate().compute()
            chart_data = [{"name": col, "mean": round(float(means[col]), 2), "median": round(float(medians[col]))} for col in numeric_cols]
            graphs = [{"chart_type": "two_bars", "chart_data": chart_data}]
            result = None
            logger.info(f"Finalizing summary for {dataset_form.session_id}")
            await websocket_send_progress(dataset_form.session_id, 80, "Finalizing results...")

        logger.info(f"Processing complete for {dataset_form.session_id}")
        await websocket_send_progress(dataset_form.session_id, 100, "Completed!")
        try:
            file_path.unlink()
        except Exception as e:
            logger.warning(f"Failed to delete file {file_path}: {str(e)}")
        return JSONResponse(content={"result": result, "graphs": graphs})
    except Exception as e:
        logger.error(f"Processing error for {dataset_form.session_id}: {str(e)}")
        await websocket_send_progress(dataset_form.session_id, 0, f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
