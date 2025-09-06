import fastapi
from fastapi import FastAPI
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
from typing import Optional, Union, Any
from pydantic import BaseModel
import atexit
import os
import json

app = FastAPI()

# 数据处理请求模型
class DataProcessRequest(BaseModel):
    input_path: str
    output_path: Optional[str] = None
    operation: str  # 操作类型
    parameters: Optional[dict] = {}  # 操作参数

# 数据处理响应模型
class DataProcessResponse(BaseModel):
    status: str  # success 或 error
    message: str
    output_file: Optional[str] = None
    error_details: Optional[str] = None

# 添加服务启动和关闭事件
@app.on_event("startup")
async def startup_event():
    """服务启动时注册到Consul"""
    service_id = start_tool_functions_service()
    if service_id:
        app.state.service_id = service_id
        print(f"data_process服务已注册到Consul，服务ID: {service_id}")


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
    return {"status": "healthy", "service": "data-process"}


# 统一的数据处理接口
@app.post("/data-process/execute", response_model=DataProcessResponse)
async def execute_data_process(request: DataProcessRequest) -> DataProcessResponse:
    """
    统一的数据处理接口，根据operation类型执行不同的数据处理操作
    """
    try:
        # 检查输入文件是否存在
        if not os.path.exists(request.input_path):
            return DataProcessResponse(
                status="error",
                message="输入文件不存在",
                error_details=f"文件路径: {request.input_path}"
            )
        
        # 如果没有指定输出路径，自动生成
        if not request.output_path:
            base_name = os.path.splitext(request.input_path)[0]
            request.output_path = f"{base_name}_{request.operation}_processed.csv"
        
        # 确保输出目录存在
        output_dir = os.path.dirname(request.output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 根据操作类型执行相应的处理
        if request.operation == "drop_empty_rows":
            return _drop_empty_rows(request)
        elif request.operation == "fill_missing_mean":
            return _fill_missing_with_mean(request)
        elif request.operation == "fill_missing_median":
            return _fill_missing_with_median(request)
        elif request.operation == "fill_missing_constant":
            return _fill_missing_with_constant(request)
        elif request.operation == "fill_missing_mode":
            return _fill_missing_with_mode(request)
        elif request.operation == "filter_by_column":
            return _filter_by_column(request)
        elif request.operation == "rename_column":
            return _rename_column(request)
        elif request.operation == "convert_column_type":
            return _convert_column_type(request)
        elif request.operation == "aggregate_column":
            return _aggregate_column(request)
        elif request.operation == "sort_by_column":
            return _sort_by_column(request)
        else:
            return DataProcessResponse(
                status="error",
                message="不支持的操作类型",
                error_details=f"操作类型: {request.operation}"
            )
    
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="处理过程中发生错误",
            error_details=str(e)
        )


def _drop_empty_rows(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        cleaned_df = df.dropna()
        cleaned_df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已删除所有空白行",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="去除空白行处理失败",
            error_details=str(e)
        )


def _fill_missing_with_mean(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        df = df.fillna(df.mean(numeric_only=True))
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用平均值填补缺失值",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用平均值填补缺失值处理失败",
            error_details=str(e)
        )


def _fill_missing_with_median(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        df = df.fillna(df.median(numeric_only=True))
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用中位数填补缺失值",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用中位数填补缺失值处理失败",
            error_details=str(e)
        )


def _fill_missing_with_constant(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        constant_value = request.parameters.get('constant_value')
        if constant_value is None:
            return DataProcessResponse(
                status="error",
                message="需要提供constant_value参数",
                error_details="parameters中需要包含constant_value字段"
            )
        
        df = df.fillna(constant_value)
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用常数填补缺失值",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用常数填补缺失值处理失败",
            error_details=str(e)
        )


def _fill_missing_with_mode(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        df = df.fillna(df.mode().iloc[0])
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用众数填补缺失值",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用众数填补缺失值处理失败",
            error_details=str(e)
        )


def _filter_by_column(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        column = request.parameters.get('column')
        condition = request.parameters.get('condition')
        value = request.parameters.get('value')
        
        if not column or not condition or value is None:
            return DataProcessResponse(
                status="error",
                message="需要提供column、condition和value参数",
                error_details="parameters中需要包含column、condition和value字段"
            )
        
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
            return DataProcessResponse(
                status="error",
                message="不支持的条件",
                error_details=f"条件: {condition}"
            )
        
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已完成筛选",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="筛选处理失败",
            error_details=str(e)
        )


def _rename_column(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        old_name = request.parameters.get('old_name')
        new_name = request.parameters.get('new_name')
        
        if not old_name or not new_name:
            return DataProcessResponse(
                status="error",
                message="需要提供old_name和new_name参数",
                error_details="parameters中需要包含old_name和new_name字段"
            )
        
        if old_name not in df.columns:
            return DataProcessResponse(
                status="error",
                message=f"列{old_name}不存在",
                error_details=f"可用列: {list(df.columns)}"
            )
        
        df = df.rename(columns={old_name: new_name})
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已完成重命名",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="重命名处理失败",
            error_details=str(e)
        )


def _convert_column_type(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        column = request.parameters.get('column')
        target_type = request.parameters.get('target_type')
        
        if not column or not target_type:
            return DataProcessResponse(
                status="error",
                message="需要提供column和target_type参数",
                error_details="parameters中需要包含column和target_type字段"
            )
        
        if column not in df.columns:
            return DataProcessResponse(
                status="error",
                message=f"列{column}不存在",
                error_details=f"可用列: {list(df.columns)}"
            )
        
        if target_type == "int":
            df[column] = df[column].astype(int)
        elif target_type == "float":
            df[column] = df[column].astype(float)
        elif target_type == "str":
            df[column] = df[column].astype(str)
        elif target_type == "bool":
            df[column] = df[column].astype(bool)
        else:
            return DataProcessResponse(
                status="error",
                message="不支持的目标类型",
                error_details=f"目标类型: {target_type}, 支持的类型: int, float, str, bool"
            )
        
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已完成类型转换",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="类型转换处理失败",
            error_details=str(e)
        )


def _aggregate_column(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        group_by = request.parameters.get('group_by')
        target_column = request.parameters.get('target_column')
        agg_func = request.parameters.get('agg_func')
        
        if not group_by or not target_column or not agg_func:
            return DataProcessResponse(
                status="error",
                message="需要提供group_by、target_column和agg_func参数",
                error_details="parameters中需要包含group_by、target_column和agg_func字段"
            )
        
        if agg_func not in ['sum', 'mean', 'max', 'min', 'count']:
            return DataProcessResponse(
                status="error",
                message="不支持此类聚合",
                error_details=f"聚合函数: {agg_func}, 支持的函数: sum, mean, max, min, count"
            )
        
        grouped = df.groupby(group_by)[target_column].agg(agg_func).reset_index()
        grouped.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已完成聚合",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="聚合处理失败",
            error_details=str(e)
        )


def _sort_by_column(request: DataProcessRequest) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        column = request.parameters.get('column')
        ascending = request.parameters.get('ascending', True)
        
        if not column:
            return DataProcessResponse(
                status="error",
                message="需要提供column参数",
                error_details="parameters中需要包含column字段"
            )
        
        if column not in df.columns:
            return DataProcessResponse(
                status="error",
                message=f"列{column}不存在",
                error_details=f"可用列: {list(df.columns)}"
            )
        
        df = df.sort_values(by=column, ascending=ascending)
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已完成排序",
            output_file=request.output_path
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="排序处理失败",
            error_details=str(e)
        )
