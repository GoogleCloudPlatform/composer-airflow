# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import collections.abc
import functools
import inspect
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    Generic,
    Iterator,
    Mapping,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    cast,
    overload,
)

import attr
import typing_extensions
from sqlalchemy.orm import Session

from airflow.compat.functools import cache, cached_property
from airflow.exceptions import AirflowException
from airflow.models.abstractoperator import DEFAULT_RETRIES, DEFAULT_RETRY_DELAY
from airflow.models.baseoperator import (
    BaseOperator,
    coerce_resources,
    coerce_retry_delay,
    get_merged_defaults,
    parse_retries,
)
from airflow.models.dag import DAG, DagContext
from airflow.models.mappedoperator import (
    MappedOperator,
    ValidationSource,
    ensure_xcomarg_return_value,
    get_mappable_types,
    prevent_duplicates,
)
from airflow.models.pool import Pool
from airflow.models.xcom_arg import XComArg
from airflow.typing_compat import Protocol
from airflow.utils import timezone
from airflow.utils.context import KNOWN_CONTEXT_KEYS, Context
from airflow.utils.task_group import TaskGroup, TaskGroupContext
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from airflow.models.mappedoperator import Mappable


def validate_python_callable(python_callable: Any) -> None:
    """
    Validate that python callable can be wrapped by operator.
    Raises exception if invalid.

    :param python_callable: Python object to be validated
    :raises: TypeError, AirflowException
    """
    if not callable(python_callable):
        raise TypeError('`python_callable` param must be callable')
    if 'self' in inspect.signature(python_callable).parameters.keys():
        raise AirflowException('@task does not support methods')


def get_unique_task_id(
    task_id: str,
    dag: Optional[DAG] = None,
    task_group: Optional[TaskGroup] = None,
) -> str:
    """
    Generate unique task id given a DAG (or if run in a DAG context)
    Ids are generated by appending a unique number to the end of
    the original task id.

    Example:
      task_id
      task_id__1
      task_id__2
      ...
      task_id__20
    """
    dag = dag or DagContext.get_current_dag()
    if not dag:
        return task_id

    # We need to check if we are in the context of TaskGroup as the task_id may
    # already be altered
    task_group = task_group or TaskGroupContext.get_current_task_group(dag)
    tg_task_id = task_group.child_id(task_id) if task_group else task_id

    if tg_task_id not in dag.task_ids:
        return task_id

    def _find_id_suffixes(dag: DAG) -> Iterator[int]:
        prefix = re.split(r"__\d+$", tg_task_id)[0]
        for task_id in dag.task_ids:
            match = re.match(rf"^{prefix}__(\d+)$", task_id)
            if match is None:
                continue
            yield int(match.group(1))
        yield 0  # Default if there's no matching task ID.

    core = re.split(r"__\d+$", task_id)[0]
    return f"{core}__{max(_find_id_suffixes(dag)) + 1}"


class DecoratedOperator(BaseOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    :param kwargs_to_upstream: For certain operators, we might need to upstream certain arguments
        that would otherwise be absorbed by the DecoratedOperator (for example python_callable for the
        PythonOperator). This gives a user the option to upstream kwargs as needed.
    """

    template_fields: Sequence[str] = ('op_args', 'op_kwargs')
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ('python_callable',)

    def __init__(
        self,
        *,
        python_callable: Callable,
        task_id: str,
        op_args: Optional[Collection[Any]] = None,
        op_kwargs: Optional[Mapping[str, Any]] = None,
        multiple_outputs: bool = False,
        kwargs_to_upstream: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        task_id = get_unique_task_id(task_id, kwargs.get('dag'), kwargs.get('task_group'))
        self.python_callable = python_callable
        kwargs_to_upstream = kwargs_to_upstream or {}
        op_args = op_args or []
        op_kwargs = op_kwargs or {}

        # Check that arguments can be binded
        inspect.signature(python_callable).bind(*op_args, **op_kwargs)
        self.multiple_outputs = multiple_outputs
        self.op_args = op_args
        self.op_kwargs = op_kwargs
        super().__init__(task_id=task_id, **kwargs_to_upstream, **kwargs)

    def execute(self, context: Context):
        return_value = super().execute(context)
        return self._handle_output(return_value=return_value, context=context, xcom_push=self.xcom_push)

    def _handle_output(self, return_value: Any, context: Context, xcom_push: Callable):
        """
        Handles logic for whether a decorator needs to push a single return value or multiple return values.

        :param return_value:
        :param context:
        :param xcom_push:
        """
        if not self.multiple_outputs:
            return return_value
        if isinstance(return_value, dict):
            for key in return_value.keys():
                if not isinstance(key, str):
                    raise AirflowException(
                        'Returned dictionary keys must be strings when using '
                        f'multiple_outputs, found {key} ({type(key)}) instead'
                    )
            for key, value in return_value.items():
                xcom_push(context, key, value)
        else:
            raise AirflowException(
                f'Returned output was type {type(return_value)} expected dictionary for multiple_outputs'
            )
        return return_value

    def _hook_apply_defaults(self, *args, **kwargs):
        if 'python_callable' not in kwargs:
            return args, kwargs

        python_callable = kwargs['python_callable']
        default_args = kwargs.get('default_args') or {}
        op_kwargs = kwargs.get('op_kwargs') or {}
        f_sig = inspect.signature(python_callable)
        for arg in f_sig.parameters:
            if arg not in op_kwargs and arg in default_args:
                op_kwargs[arg] = default_args[arg]
        kwargs['op_kwargs'] = op_kwargs
        return args, kwargs


Function = TypeVar("Function", bound=Callable)

OperatorSubclass = TypeVar("OperatorSubclass", bound="BaseOperator")


@attr.define(slots=False)
class _TaskDecorator(Generic[Function, OperatorSubclass]):
    """
    Helper class for providing dynamic task mapping to decorated functions.

    ``task_decorator_factory`` returns an instance of this, instead of just a plain wrapped function.

    :meta private:
    """

    function: Function = attr.ib()
    operator_class: Type[OperatorSubclass]
    multiple_outputs: bool = attr.ib()
    kwargs: Dict[str, Any] = attr.ib(factory=dict)

    decorator_name: str = attr.ib(repr=False, default="task")

    @multiple_outputs.default
    def _infer_multiple_outputs(self):
        try:
            return_type = typing_extensions.get_type_hints(self.function).get("return", Any)
        except Exception:  # Can't evaluate retrurn type.
            return False
        ttype = getattr(return_type, "__origin__", return_type)
        return ttype == dict or ttype == Dict

    def __attrs_post_init__(self):
        if "self" in self.function_signature.parameters:
            raise TypeError(f"@{self.decorator_name} does not support methods")
        self.kwargs.setdefault('task_id', self.function.__name__)

    def __call__(self, *args, **kwargs) -> XComArg:
        op = self.operator_class(
            python_callable=self.function,
            op_args=args,
            op_kwargs=kwargs,
            multiple_outputs=self.multiple_outputs,
            **self.kwargs,
        )
        if self.function.__doc__:
            op.doc_md = self.function.__doc__
        return XComArg(op)

    @cached_property
    def function_signature(self):
        return inspect.signature(self.function)

    @cached_property
    def _function_is_vararg(self):
        parameters = self.function_signature.parameters
        return any(v.kind == inspect.Parameter.VAR_KEYWORD for v in parameters.values())

    @cached_property
    def _mappable_function_argument_names(self) -> Set[str]:
        """Arguments that can be mapped against."""
        return set(self.function_signature.parameters)

    def _validate_arg_names(self, func: ValidationSource, kwargs: Dict[str, Any]):
        # Ensure that context variables are not shadowed.
        context_keys_being_mapped = KNOWN_CONTEXT_KEYS.intersection(kwargs)
        if len(context_keys_being_mapped) == 1:
            (name,) = context_keys_being_mapped
            raise ValueError(f"cannot call {func}() on task context variable {name!r}")
        elif context_keys_being_mapped:
            names = ", ".join(repr(n) for n in context_keys_being_mapped)
            raise ValueError(f"cannot call {func}() on task context variables {names}")

        # Ensure that all arguments passed in are accounted for.
        if self._function_is_vararg:
            return
        kwargs_left = kwargs.copy()
        for arg_name in self._mappable_function_argument_names:
            value = kwargs_left.pop(arg_name, NOTSET)
            if func != "expand" or value is NOTSET or isinstance(value, get_mappable_types()):
                continue
            tname = type(value).__name__
            raise ValueError(f"expand() got an unexpected type {tname!r} for keyword argument {arg_name!r}")
        if len(kwargs_left) == 1:
            raise TypeError(f"{func}() got an unexpected keyword argument {next(iter(kwargs_left))!r}")
        elif kwargs_left:
            names = ", ".join(repr(n) for n in kwargs_left)
            raise TypeError(f"{func}() got unexpected keyword arguments {names}")

    def expand(self, **map_kwargs: "Mappable") -> XComArg:
        self._validate_arg_names("expand", map_kwargs)
        prevent_duplicates(self.kwargs, map_kwargs, fail_reason="mapping already partial")
        ensure_xcomarg_return_value(map_kwargs)

        task_kwargs = self.kwargs.copy()
        dag = task_kwargs.pop("dag", None) or DagContext.get_current_dag()
        task_group = task_kwargs.pop("task_group", None) or TaskGroupContext.get_current_task_group(dag)

        partial_kwargs, default_params = get_merged_defaults(
            dag=dag,
            task_group=task_group,
            task_params=task_kwargs.pop("params", None),
            task_default_args=task_kwargs.pop("default_args", None),
        )
        partial_kwargs.update(task_kwargs)

        task_id = get_unique_task_id(partial_kwargs.pop("task_id"), dag, task_group)
        params = partial_kwargs.pop("params", None) or default_params

        # Logic here should be kept in sync with BaseOperatorMeta.partial().
        if "task_concurrency" in partial_kwargs:
            raise TypeError("unexpected argument: task_concurrency")
        if partial_kwargs.get("wait_for_downstream"):
            partial_kwargs["depends_on_past"] = True
        start_date = timezone.convert_to_utc(partial_kwargs.pop("start_date", None))
        end_date = timezone.convert_to_utc(partial_kwargs.pop("end_date", None))
        if partial_kwargs.get("pool") is None:
            partial_kwargs["pool"] = Pool.DEFAULT_POOL_NAME
        partial_kwargs["retries"] = parse_retries(partial_kwargs.get("retries", DEFAULT_RETRIES))
        partial_kwargs["retry_delay"] = coerce_retry_delay(
            partial_kwargs.get("retry_delay", DEFAULT_RETRY_DELAY),
        )
        partial_kwargs["resources"] = coerce_resources(partial_kwargs.get("resources"))
        partial_kwargs.setdefault("executor_config", {})
        partial_kwargs.setdefault("op_args", [])
        partial_kwargs.setdefault("op_kwargs", {})

        # Mypy does not work well with a subclassed attrs class :(
        _MappedOperator = cast(Any, DecoratedMappedOperator)
        operator = _MappedOperator(
            operator_class=self.operator_class,
            mapped_kwargs={},
            partial_kwargs=partial_kwargs,
            task_id=task_id,
            params=params,
            deps=MappedOperator.deps_for(self.operator_class),
            operator_extra_links=self.operator_class.operator_extra_links,
            template_ext=self.operator_class.template_ext,
            template_fields=self.operator_class.template_fields,
            template_fields_renderers=self.operator_class.template_fields_renderers,
            ui_color=self.operator_class.ui_color,
            ui_fgcolor=self.operator_class.ui_fgcolor,
            is_dummy=False,
            task_module=self.operator_class.__module__,
            task_type=self.operator_class.__name__,
            dag=dag,
            task_group=task_group,
            start_date=start_date,
            end_date=end_date,
            multiple_outputs=self.multiple_outputs,
            python_callable=self.function,
            mapped_op_kwargs=map_kwargs,
            # Different from classic operators, kwargs passed to a taskflow
            # task's expand() contribute to the op_kwargs operator argument, not
            # the operator arguments themselves, and should expand against it.
            expansion_kwargs_attr="mapped_op_kwargs",
        )
        return XComArg(operator=operator)

    def partial(self, **kwargs) -> "_TaskDecorator[Function, OperatorSubclass]":
        self._validate_arg_names("partial", kwargs)

        op_kwargs = self.kwargs.get("op_kwargs", {})
        op_kwargs = _merge_kwargs(op_kwargs, kwargs, fail_reason="duplicate partial")

        return attr.evolve(self, kwargs={**self.kwargs, "op_kwargs": op_kwargs})


def _merge_kwargs(kwargs1: Dict[str, Any], kwargs2: Dict[str, Any], *, fail_reason: str) -> Dict[str, Any]:
    duplicated_keys = set(kwargs1).intersection(kwargs2)
    if len(duplicated_keys) == 1:
        raise TypeError(f"{fail_reason} argument: {duplicated_keys.pop()}")
    elif duplicated_keys:
        duplicated_keys_display = ", ".join(sorted(duplicated_keys))
        raise TypeError(f"{fail_reason} arguments: {duplicated_keys_display}")
    return {**kwargs1, **kwargs2}


@attr.define(kw_only=True, repr=False)
class DecoratedMappedOperator(MappedOperator):
    """MappedOperator implementation for @task-decorated task function."""

    multiple_outputs: bool
    python_callable: Callable

    # We can't save these in mapped_kwargs because op_kwargs need to be present
    # in partial_kwargs, and MappedOperator prevents duplication.
    mapped_op_kwargs: Dict[str, "Mappable"]

    @classmethod
    @cache
    def get_serialized_fields(cls):
        # The magic super() doesn't work here, so we use the explicit form.
        # Not using super(..., cls) to work around pyupgrade bug.
        sup = super(DecoratedMappedOperator, DecoratedMappedOperator)
        return sup.get_serialized_fields() | {"mapped_op_kwargs"}

    def __attrs_post_init__(self):
        # The magic super() doesn't work here, so we use the explicit form.
        # Not using super(..., self) to work around pyupgrade bug.
        super(DecoratedMappedOperator, DecoratedMappedOperator).__attrs_post_init__(self)
        XComArg.apply_upstream_relationship(self, self.mapped_op_kwargs)

    def _get_unmap_kwargs(self) -> Dict[str, Any]:
        partial_kwargs = self.partial_kwargs.copy()
        op_kwargs = _merge_kwargs(
            partial_kwargs.pop("op_kwargs"),
            self.mapped_op_kwargs,
            fail_reason="mapping already partial",
        )
        return {
            "dag": self.dag,
            "task_group": self.task_group,
            "task_id": self.task_id,
            "op_kwargs": op_kwargs,
            "multiple_outputs": self.multiple_outputs,
            "python_callable": self.python_callable,
            **partial_kwargs,
            **self.mapped_kwargs,
        }

    def _expand_mapped_field(self, key: str, content: Any, context: Context, *, session: Session) -> Any:
        if key != "op_kwargs" or not isinstance(content, collections.abc.Mapping):
            return content
        # The magic super() doesn't work here, so we use the explicit form.
        # Not using super(..., self) to work around pyupgrade bug.
        sup: Any = super(DecoratedMappedOperator, DecoratedMappedOperator)
        return {k: sup._expand_mapped_field(self, k, v, context, session=session) for k, v in content.items()}


class Task(Generic[Function]):
    """Declaration of a @task-decorated callable for type-checking.

    An instance of this type inherits the call signature of the decorated
    function wrapped in it (not *exactly* since it actually returns an XComArg,
    but there's no way to express that right now), and provides two additional
    methods for task-mapping.

    This type is implemented by ``_TaskDecorator`` at runtime.
    """

    __call__: Function

    function: Function

    def expand(self, **kwargs: "Mappable") -> XComArg:
        ...

    def partial(self, **kwargs: Any) -> "Task[Function]":
        ...


class TaskDecorator(Protocol):
    """Type declaration for ``task_decorator_factory`` return type."""

    @overload
    def __call__(self, python_callable: Function) -> Task[Function]:
        """For the "bare decorator" ``@task`` case."""

    @overload
    def __call__(
        self,
        *,
        multiple_outputs: Optional[bool] = None,
        **kwargs: Any,
    ) -> Callable[[Function], Task[Function]]:
        """For the decorator factory ``@task()`` case."""


def task_decorator_factory(
    python_callable: Optional[Callable] = None,
    *,
    multiple_outputs: Optional[bool] = None,
    decorated_operator_class: Type[BaseOperator],
    **kwargs,
) -> TaskDecorator:
    """
    A factory that generates a wrapper that raps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    :param decorated_operator_class: The operator that executes the logic needed to run the python function in
        the correct environment

    """
    if multiple_outputs is None:
        multiple_outputs = cast(bool, attr.NOTHING)
    if python_callable:
        decorator = _TaskDecorator(
            function=python_callable,
            multiple_outputs=multiple_outputs,
            operator_class=decorated_operator_class,
            kwargs=kwargs,
        )
        return cast(TaskDecorator, decorator)
    elif python_callable is not None:
        raise TypeError('No args allowed while using @task, use kwargs instead')
    decorator_factory = functools.partial(
        _TaskDecorator,
        multiple_outputs=multiple_outputs,
        operator_class=decorated_operator_class,
        kwargs=kwargs,
    )
    return cast(TaskDecorator, decorator_factory)
