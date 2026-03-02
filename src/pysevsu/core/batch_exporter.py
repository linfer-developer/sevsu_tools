import asyncio
import json
from dataclasses import dataclass
from io import BytesIO
from typing import (Any, Coroutine, Dict, FrozenSet, List, Optional, Set,
                    Tuple, Type)

import aiohttp
from sqlalchemy import Table, UniqueConstraint, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (AsyncSession, async_sessionmaker,
                                    create_async_engine)

from src.pysevsu.core import batch_exporter, web, xls
from src.pysevsu.models import tables
from src.pysevsu.utils import log


class _Model:
    def __init__(self, obj: object) -> None:
        self.obj = obj
        self.model = obj.__class__
        self.table = self.model.__table__
        self._mapper = inspect(type(obj))

    @property
    def has_relationship(self) -> bool:
        for column in self._mapper.columns:
            if column.foreign_keys and not column.nullable:
                return True
        return False

    @property
    def relationships(self) -> iter:
        for column in self._mapper.columns:
            if column.foreign_keys and not column.nullable:
                yield column.name

    @property
    def related_data(self) -> iter:
        for rel in self._mapper.relationships:
            yield getattr(self.obj, rel.key)

    @property
    def editable_fields(self) -> List[str]:
        editable: List[str] = []
        for column in self._mapper.columns:
            if not column.primary_key:
                editable.append(column.name)
        return editable

    @property
    def tablename(self) -> str:
        return self._mapper.local_table.name

    @property
    def unique_fields(self) -> List[str]:
        for constraint in self.table.constraints:
            if isinstance(constraint, UniqueConstraint):
                return {col.name for col in constraint.columns}
        return {col.name for col in self._mapper.columns if col.unique}

    @property
    def record_instances(self) -> Dict[str, Any]:
        record_instances = dict(vars(self.obj))
        del record_instances["_sa_instance_state"]
        return record_instances

    @property
    def any_not_unique_field(self) -> str:
        unique_fields: List[str] = self.unique_fields
        for field in self.record_instances.keys():
            if field not in unique_fields and field != "_sa_instance_state":
                return field

    @property
    def any_editable_field(self) -> str:
        for column in self._mapper.columns:
            if not column.primary_key:
                return column.name


class _GraphOfTableDependencies:
    def __init__(self, fix_order: Optional[int] = None) -> None:
        self.fix_order = fix_order
        self._adjacency_list: Dict[_Model, Set[_Model]] = {}

    def add(self, model: _Model) -> bool:
        if self.fix_order > len(self._adjacency_list):
            self._adjacency_list[model] = {model.related_data}
            return True
        return False
    
    def sort(self) -> bool:
        ...

    def is_the_order_limited(self) -> bool:
        if self.fix_order >= len(self._adjacency_list):
            return True
        return False

    def destroy(self) -> None:
        self._adjacency_list.clear()
