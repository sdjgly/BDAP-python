import fastapi
from fastapi import FastAPI
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
import atexit

app = FastAPI()


# 添加服务启动和关闭事件
@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    service_id = start_tool_functions_service()
    if service_id:
        app.state.service_id = service_id
        print(f"tool_functions服务已注册到Consul，服务ID: {service_id}")


@app.on_event("shutdown")
async def shutdown_event():
    """服务关闭时从Consul注销"""
    if hasattr(app.state, 'service_id'):
        deregister_service(app.state.service_id)


def start_tool_functions_service():
    SERVICE_PORT = 8002
    tags = ['tools', 'data-processing', 'pandas']
    service_id = register_service(SERVICE_PORT, tags)
    return service_id


# 健康检查端点
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "tool-functions"}


@app.post("/tools/drop-empty-rows")
def drop_empty_rows(file_path: str, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        cleaned_df = df.dropna()
        cleaned_df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已删除所有空白行",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"去除空白行处理失败{str(e)}"
        }


@app.post("/tools/fill-missing-with-mean")
def fill_missing_with_mean(file_path: str, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        df = df.fillna(df.mean(numeric_only = True))
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用平均值填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用平均值填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/fill-missing-with-median")
def fill_missing_with_median(file_path: str, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        df = df.fillna(df.median(numeric_only = True))
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用中位数填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用中位数填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/fill-missing-with-constant")
def fill_missing_with_constant(file_path: str, con: int, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        df = df.fillna(con)
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用常数填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用常数填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/fill-missing-with-mode")
def fill_missing_with_mode(file_path: str, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        df = df.fillna(df.mode().iloc[0])
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已使用众数填补缺失值",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"使用众数填补缺失值处理失败:{str(e)}"
        }


@app.post("/tools/filter-by-column")
def filter_by_column(file_path: str, column: str, condition, value, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        if condition == '==':
            df = df[df[column] == value]
        elif condition == '!=':
            df = df[df[column] != value]
        elif condition == '>':
            df = df[df[column] > value]
        elif condition == '<':
            df = df[df[column] < value]
        elif condition == '>=':
            df = df[df[column] >= value]
        elif condition == '<=':
            df = df[df[column] <= value]
        else:
            raise ValueError("不支持的条件")
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成筛选",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"筛选处理失败{str(e)}"
        }


@app.post("/tools/rename-column")
def rename_column(file_path: str, old_name: str, new_name: str, output_path: str) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        if old_name not in df.columns:
            raise ValueError(f"列{old_name}不存在")
        df = df.rename(columns = {old_name:new_name})
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成重命名",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"重命名处理失败{str(e)}"
        }


@app.post("/tools/convert-column-type")
def convert_column_type(file_path: str, column: str, target_type: str, output_path: str) -> dict:
    import pandas as pd
    df = pd.read_csv(file_path)
    if column not in df.columns:
        raise ValueError(f"列{column}不存在")
    try:
        if target_type == "int":
            df[column] = df[column].astype(int)
        elif target_type == "float":
            df[column] = df[column].astype(float)
        elif target_type == "str":
            df[column] = df[column].astype(str)
        elif target_type == "bool":
            df[column] = df[column].astype(bool)
        else:
            raise ValueError("不支持的目标类型")
    except Exception as e:
        return {
            "status":"error",
            "message":f"类型转换处理失败{str(e)}"
        }
    return {
        "status":"success",
        "message":"已完成类型转换",
        "output_file":output_path
    }


@app.post("/tools/aggregate-column")
def aggregate_column(file_path: str, group_by: str, target_column: str,
                     agg_func: str, output_path: str) -> dict:
    import pandas as pd
    df = pd.read_csv(file_path)
    if agg_func not in ['sum', 'mean', 'max', 'min', 'count']:
        raise ValueError("不支持此类聚合")
    try:
        grouped = df.groupby(group_by)[target_column].agg(agg_func).reset_index()
        grouped.to_csv(output_path)
        return {
            "status":"success",
            "message":"已完成聚合",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"聚合处理失败{str(e)}"
        }


@app.post("/tools/sort-by-column")
def sort_by_column(file_path: str, column: str, output_path: str, ascending: bool = True) -> dict:
    import pandas as pd
    try:
        df = pd.read_csv(file_path)
        if column not in df.columns:
            raise ValueError(f"列{column}不存在")
        df = df.sort_values(by = column, ascending = ascending)
        df.to_csv(output_path, index = False)
        return {
            "status":"success",
            "message":"已完成排序",
            "output_file":output_path
        }
    except Exception as e:
        return {
            "status":"error",
            "message":f"排序处理失败{str(e)}"
        }
