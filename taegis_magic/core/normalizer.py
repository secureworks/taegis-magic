"""Taegis Base Normalizer."""

import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Union

import jinja2
import pandas as pd
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class TaegisResultsNormalizer:
    """Taegis Results normalizer."""

    service: str
    tenant_id: str
    region: str
    raw_results: List[Dict[str, Any]] = field(default_factory=list)
    arguments: Dict[str, Any] = field(default_factory=dict)

    @property
    def normalizer(self) -> str:
        """Normalizer class name."""
        return str(self.__class__.__name__)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results."""
        return self.raw_results

    @property
    def total_results(self) -> int:
        """Total Results."""
        return len(self.results)

    @property
    def results_returned(self) -> int:
        """Results Returned."""
        return len(self.results)

    def _repr_markdown_(self):
        """Represent as markdown."""
        return self._display_template("taegis_results.md.jinja")

    def _display_template(self, template_name):  # pragma: no cover
        """Setup Jinja templating for markdown representation."""
        # template_name = "xdr_search_results.md.jinja"
        jinja_env = jinja2.Environment(
            loader=jinja2.PackageLoader("taegis_magic", "templates"),
            autoescape=jinja2.select_autoescape(["html", "xml"]),
        )

        def validate_int(value: int) -> Union[int, str]:
            """Validate normalizer integer values.

            Parameters
            ----------
            value : int
                Value to validate

            Returns
            -------
            Union[int, str]
                Return value or N/A
            """
            return value if value >= 0 else "N/A"

        jinja_env.filters["validate_int"] = validate_int

        template = jinja_env.get_template(template_name)
        return template.render(obj=self)


class TaegisResult(TaegisResultsNormalizer):
    """Generic single result normalizer."""

    raw_results: Any = field(default=None)

    @property
    def results(self):
        return [asdict(self.raw_results)]


class TaegisResults(TaegisResultsNormalizer):
    """Generic multiple results normalizer."""

    raw_results: List[Any] = field(default_factory=list)

    @property
    def results(self):
        return [asdict(r) for r in self.raw_results]


log = logging.getLogger(__name__)


def merge_normalizer_results(
    results: List[TaegisResultsNormalizer],
) -> TaegisResultsNormalizer:
    """Merge multiple normalizer results from a multi-tenant macro execution.

    Each result's individual records are tagged with ``_macro_tenant_id``
    so the originating tenant is preserved after merging.

    Parameters
    ----------
    results
        Normalizer instances to merge, one per tenant execution.

    Returns
    -------
    TaegisResultsNormalizer
        A single normalizer with all results combined.
    """
    if not results:
        return TaegisResultsNormalizer(
            service="unknown",
            tenant_id="None",
            region="None",
        )

    if len(results) == 1:
        return results[0]

    merged_raw: List[Dict[str, Any]] = []
    tenant_ids: List[str] = []

    for normalizer in results:
        tid = normalizer.tenant_id
        tenant_ids.append(tid)
        for record in normalizer.results:
            tagged = dict(record)
            tagged["_macro_tenant_id"] = tid
            merged_raw.append(tagged)

    first = results[0]

    merged = TaegisResultsNormalizer(
        service=first.service,
        tenant_id=", ".join(tenant_ids),
        region=first.region,
        raw_results=merged_raw,
        arguments=first.arguments,
    )

    log.info(
        "Merged results from %d tenants (%d total records)",
        len(results),
        len(merged_raw),
    )

    return merged


@dataclass_json
@dataclass
class DataFrameNormalizer(TaegisResultsNormalizer):
    raw_results: pd.DataFrame

    @property
    def results(self):
        return self.raw_results.to_dict(orient="records")

    def _repr_markdown_(self):
        """Represent as markdown."""
        return self.raw_results.to_html(index=False)

    def _repr_html_(self):
        """Represent as HTML."""
        return self.raw_results.to_html(index=False)
