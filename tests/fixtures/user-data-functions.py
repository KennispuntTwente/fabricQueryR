"""Publish these side-effect-free functions in the marked integration workspace."""
import fabric.functions as fn

udf = fn.UserDataFunctions()


@udf.function()
def scalar(value: str) -> str:
    return value


@udf.function()
def structured(label: str, values: list, metadata: dict) -> dict:
    return {
        "label": label,
        "values": values,
        "total": sum(values),
        "metadata": metadata,
    }


@udf.function()
def error(value: int) -> int:
    if value == -2:
        raise RuntimeError("FABRICQUERYR_INTENTIONAL_FUNCTION_FAILURE")
    if value < 0:
        raise fn.UserThrownError("value must be non-negative", {"value": value})
    return value
