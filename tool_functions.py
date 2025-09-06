import fastapi
from fastapi import FastAPI
from consul_utils import register_service, deregister_service
from config import SERVICE_NAME
from typing import Optional, Union, Any, Dict, Tuple
from pydantic import BaseModel
import atexit
import os
import json
import re

app = FastAPI()

# 修改后的数据处理请求模型
class DataProcessRequest(BaseModel):
    model: str                          # 模型名称
    requestId: str                      # 请求ID
    user_id: Optional[str] = None       # 用户ID
    conversation_id: Optional[str] = None # 对话ID
    user_prompt: str                    # 用户需求描述
    input_path: str                     # 输入文件路径
    output_path: Optional[str] = None   # 输出文件路径

# 数据处理响应模型
class DataProcessResponse(BaseModel):
    status: str  # success 或 error
    message: str
    output_file: Optional[str] = None
    error_details: Optional[str] = None
    requestId: str  # 返回请求ID

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


def parse_user_prompt(user_prompt: str) -> Tuple[Optional[str], Optional[Dict]]:
    """
    解析用户自然语言描述，提取操作类型和参数
    返回 (操作类型, 参数字典)
    """
    user_prompt_lower = user_prompt.lower()
    
    # 删除空行相关的关键词
    if any(keyword in user_prompt_lower for keyword in ['删除空行', '去除空行', '删除缺失', 'drop empty', 'remove empty']):
        return 'drop_empty_rows', {}
    
    # 填充缺失值相关
    if '平均值' in user_prompt_lower or 'mean' in user_prompt_lower:
        return 'fill_missing_mean', {}
    
    if '中位数' in user_prompt_lower or 'median' in user_prompt_lower:
        return 'fill_missing_median', {}
    
    if '众数' in user_prompt_lower or 'mode' in user_prompt_lower:
        return 'fill_missing_mode', {}
    
    # 常数填充 - 需要提取常数值
    constant_match = re.search(r'用.?(\d+|零|一|二|三|四|五|六|七|八|九).?填充', user_prompt_lower)
    if constant_match or '常数填充' in user_prompt_lower:
        constant_value = 0  # 默认值
        if constant_match:
            try:
                constant_value = int(constant_match.group(1))
            except:
                # 处理中文数字或其他情况
                pass
        return 'fill_missing_constant', {'constant_value': constant_value}
    
    # 筛选操作
    if '筛选' in user_prompt_lower or 'filter' in user_prompt_lower:
        # 尝试解析筛选条件
        # 例如: "筛选年龄大于30的数据"
        column_match = re.search(r'筛选.?(\w+).?(大于|小于|等于|不等于|>=|<=|>|<|==|!=).?(\d+|\w+)', user_prompt)
        if column_match:
            column = column_match.group(1)
            condition_text = column_match.group(2)
            value = column_match.group(3)
            
            # 转换条件
            condition_map = {
                '大于': '>', '小于': '<', '等于': '==', '不等于': '!=',
                '>=': '>=', '<=': '<=', '>': '>', '<': '<', '==': '==', '!=': '!='
            }
            condition = condition_map.get(condition_text, '==')
            
            # 尝试转换值的类型
            try:
                value = float(value)
                if value.is_integer():
                    value = int(value)
            except:
                pass  # 保持字符串类型
            
            return 'filter_by_column', {'column': column, 'condition': condition, 'value': value}
        return 'filter_by_column', {}  # 需要更多参数信息
    
    # 重命名列
    rename_match = re.search(r'重命名.?(\w+).?(为|成).?(\w+)', user_prompt)
    if rename_match or '重命名' in user_prompt_lower:
        if rename_match:
            old_name = rename_match.group(1)
            new_name = rename_match.group(3)
            return 'rename_column', {'old_name': old_name, 'new_name': new_name}
        return 'rename_column', {}  # 需要更多参数信息
    
    # 类型转换
    type_match = re.search(r'转换.?(\w+).?(为|成).?(整数|浮点数|字符串|布尔|int|float|str|bool)', user_prompt_lower)
    if type_match or '类型转换' in user_prompt_lower:
        if type_match:
            column = type_match.group(1)
            target_type_text = type_match.group(3)
            
            type_map = {
                '整数': 'int', '浮点数': 'float', '字符串': 'str', '布尔': 'bool',
                'int': 'int', 'float': 'float', 'str': 'str', 'bool': 'bool'
            }
            target_type = type_map.get(target_type_text, 'str')
            
            return 'convert_column_type', {'column': column, 'target_type': target_type}
        return 'convert_column_type', {}  # 需要更多参数信息
    
    # 聚合操作
    agg_match = re.search(r'按.?(\w+).?分组.?(求和|平均值|最大值|最小值|计数|sum|mean|max|min|count).?(\w+)', user_prompt_lower)
    if agg_match or '聚合' in user_prompt_lower or '分组' in user_prompt_lower:
        if agg_match:
            group_by = agg_match.group(1)
            agg_func_text = agg_match.group(2)
            target_column = agg_match.group(3)
            
            agg_map = {
                '求和': 'sum', '平均值': 'mean', '最大值': 'max', '最小值': 'min', '计数': 'count',
                'sum': 'sum', 'mean': 'mean', 'max': 'max', 'min': 'min', 'count': 'count'
            }
            agg_func = agg_map.get(agg_func_text, 'sum')
            
            return 'aggregate_column', {'group_by': group_by, 'target_column': target_column, 'agg_func': agg_func}
        return 'aggregate_column', {}  # 需要更多参数信息
    
    # 排序操作
    sort_match = re.search(r'按.?(\w+).?(升序|降序|排序)', user_prompt_lower)
    if sort_match or '排序' in user_prompt_lower:
        if sort_match:
            column = sort_match.group(1)
            order = sort_match.group(2)
            ascending = order != '降序'
            return 'sort_by_column', {'column': column, 'ascending': ascending}
        return 'sort_by_column', {}  # 需要更多参数信息
    
    return None, {}


# 统一的数据处理接口
@app.post("/data-process/execute", response_model=DataProcessResponse)
async def execute_data_process(request: DataProcessRequest) -> DataProcessResponse:
    """
    统一的数据处理接口，根据用户描述解析操作类型并执行相应的数据处理操作
    """
    try:
        # 检查输入文件是否存在
        if not os.path.exists(request.input_path):
            return DataProcessResponse(
                status="error",
                message="输入文件不存在",
                error_details=f"文件路径: {request.input_path}",
                requestId=request.requestId
            )
        
        # 解析用户需求描述
        operation, parameters = parse_user_prompt(request.user_prompt)
        
        if not operation:
            return DataProcessResponse(
                status="error",
                message="无法理解用户需求描述",
                error_details=f"用户描述: {request.user_prompt}。请尝试使用更具体的描述，如'删除空行'、'用平均值填充缺失值'等",
                requestId=request.requestId
            )
        
        # 如果没有指定输出路径，自动生成
        if not request.output_path:
            base_name = os.path.splitext(request.input_path)[0]
            request.output_path = f"{base_name}_{operation}_processed.csv"
        
        # 确保输出目录存在
        output_dir = os.path.dirname(request.output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 创建一个包含解析参数的请求对象
        class LegacyRequest:
            def __init__(self, input_path, output_path, operation, parameters):
                self.input_path = input_path
                self.output_path = output_path
                self.operation = operation
                self.parameters = parameters
        
        legacy_request = LegacyRequest(request.input_path, request.output_path, operation, parameters)
        
        # 根据操作类型执行相应的处理
        if operation == "drop_empty_rows":
            result = _drop_empty_rows(legacy_request)
        elif operation == "fill_missing_mean":
            result = _fill_missing_with_mean(legacy_request)
        elif operation == "fill_missing_median":
            result = _fill_missing_with_median(legacy_request)
        elif operation == "fill_missing_constant":
            result = _fill_missing_with_constant(legacy_request)
        elif operation == "fill_missing_mode":
            result = _fill_missing_with_mode(legacy_request)
        elif operation == "filter_by_column":
            result = _filter_by_column(legacy_request)
        elif operation == "rename_column":
            result = _rename_column(legacy_request)
        elif operation == "convert_column_type":
            result = _convert_column_type(legacy_request)
        elif operation == "aggregate_column":
            result = _aggregate_column(legacy_request)
        elif operation == "sort_by_column":
            result = _sort_by_column(legacy_request)
        else:
            return DataProcessResponse(
                status="error",
                message="不支持的操作类型",
                error_details=f"操作类型: {operation}",
                requestId=request.requestId
            )
        
        # 添加requestId到结果中
        result.requestId = request.requestId
        return result
    
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="处理过程中发生错误",
            error_details=str(e),
            requestId=request.requestId
        )


def _drop_empty_rows(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        cleaned_df = df.dropna()
        cleaned_df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已删除所有空白行",
            output_file=request.output_path,
            requestId=""  # 稍后会设置
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="去除空白行处理失败",
            error_details=str(e),
            requestId=""
        )


def _fill_missing_with_mean(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        df = df.fillna(df.mean(numeric_only=True))
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用平均值填补缺失值",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用平均值填补缺失值处理失败",
            error_details=str(e),
            requestId=""
        )


def _fill_missing_with_median(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        df = df.fillna(df.median(numeric_only=True))
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用中位数填补缺失值",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用中位数填补缺失值处理失败",
            error_details=str(e),
            requestId=""
        )


def _fill_missing_with_constant(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        constant_value = request.parameters.get('constant_value')
        if constant_value is None:
            return DataProcessResponse(
                status="error",
                message="需要在描述中指定填充的常数值",
                error_details="例如：'用0填充缺失值'",
                requestId=""
            )
        
        df = df.fillna(constant_value)
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message=f"已使用常数{constant_value}填补缺失值",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用常数填补缺失值处理失败",
            error_details=str(e),
            requestId=""
        )


def _fill_missing_with_mode(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        df = df.fillna(df.mode().iloc[0])
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message="已使用众数填补缺失值",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="使用众数填补缺失值处理失败",
            error_details=str(e),
            requestId=""
        )


def _filter_by_column(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        column = request.parameters.get('column')
        condition = request.parameters.get('condition')
        value = request.parameters.get('value')
        
        if not column or not condition or value is None:
            return DataProcessResponse(
                status="error",
                message="需要在描述中指定列名、条件和值",
                error_details="例如：'筛选年龄大于30的数据'",
                requestId=""
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
                error_details=f"条件: {condition}",
                requestId=""
            )
        
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message=f"已完成筛选，条件：{column} {condition} {value}",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="筛选处理失败",
            error_details=str(e),
            requestId=""
        )


def _rename_column(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        old_name = request.parameters.get('old_name')
        new_name = request.parameters.get('new_name')
        
        if not old_name or not new_name:
            return DataProcessResponse(
                status="error",
                message="需要在描述中指定原列名和新列名",
                error_details="例如：'重命名age为年龄'",
                requestId=""
            )
        
        if old_name not in df.columns:
            return DataProcessResponse(
                status="error",
                message=f"列{old_name}不存在",
                error_details=f"可用列: {list(df.columns)}",
                requestId=""
            )
        
        df = df.rename(columns={old_name: new_name})
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message=f"已将列'{old_name}'重命名为'{new_name}'",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="重命名处理失败",
            error_details=str(e),
            requestId=""
        )


def _convert_column_type(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        column = request.parameters.get('column')
        target_type = request.parameters.get('target_type')
        
        if not column or not target_type:
            return DataProcessResponse(
                status="error",
                message="需要在描述中指定列名和目标类型",
                error_details="例如：'转换age为整数'",
                requestId=""
            )
        
        if column not in df.columns:
            return DataProcessResponse(
                status="error",
                message=f"列{column}不存在",
                error_details=f"可用列: {list(df.columns)}",
                requestId=""
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
                error_details=f"目标类型: {target_type}, 支持的类型: int, float, str, bool",
                requestId=""
            )
        
        df.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message=f"已将列'{column}'转换为{target_type}类型",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="类型转换处理失败",
            error_details=str(e),
            requestId=""
        )


def _aggregate_column(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        group_by = request.parameters.get('group_by')
        target_column = request.parameters.get('target_column')
        agg_func = request.parameters.get('agg_func')
        
        if not group_by or not target_column or not agg_func:
            return DataProcessResponse(
                status="error",
                message="需要在描述中指定分组列、目标列和聚合函数",
                error_details="例如：'按部门分组求销售额的平均值'",
                requestId=""
            )
        
        if agg_func not in ['sum', 'mean', 'max', 'min', 'count']:
            return DataProcessResponse(
                status="error",
                message="不支持此类聚合",
                error_details=f"聚合函数: {agg_func}, 支持的函数: sum, mean, max, min, count",
                requestId=""
            )
        
        grouped = df.groupby(group_by)[target_column].agg(agg_func).reset_index()
        grouped.to_csv(request.output_path, index=False)
        
        return DataProcessResponse(
            status="success",
            message=f"已按'{group_by}'分组对'{target_column}'进行{agg_func}聚合",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="聚合处理失败",
            error_details=str(e),
            requestId=""
        )


def _sort_by_column(request) -> DataProcessResponse:
    import pandas as pd
    try:
        df = pd.read_csv(request.input_path)
        column = request.parameters.get('column')
        ascending = request.parameters.get('ascending', True)
        
        if not column:
            return DataProcessResponse(
                status="error",
                message="需要在描述中指定排序的列名",
                error_details="例如：'按年龄升序排序'",
                requestId=""
            )
        
        if column not in df.columns:
            return DataProcessResponse(
                status="error",
                message=f"列{column}不存在",
                error_details=f"可用列: {list(df.columns)}",
                requestId=""
            )
        
        df = df.sort_values(by=column, ascending=ascending)
        df.to_csv(request.output_path, index=False)
        
        order_text = "升序" if ascending else "降序"
        return DataProcessResponse(
            status="success",
            message=f"已按'{column}'进行{order_text}排序",
            output_file=request.output_path,
            requestId=""
        )
    except Exception as e:
        return DataProcessResponse(
            status="error",
            message="排序处理失败",
            error_details=str(e),
            requestId=""
        )
