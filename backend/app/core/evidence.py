"""采集计量证据：把“处理/跳过/解析尝试”的计数口径收拢到唯一一份事实。

背景
----
原先采集指标和质量报告各自解释一遍采集计数：指标用
`processed_lines / skipped_lines` 算错误率和积压，报告又用另一组
`processed / skipped / parse_ok / parse_total` 参数算完整率和解析成功率。
两组计数一旦不同源，同一批日志会得出互相矛盾的比例。

本模块把计数解释收敛为“采集计量证据”：

* 证据按 (来源, 批次, 评估周期) 分组，每条物理日志行以 (来源, 行号, 行内容
  指纹) 绑定成不可换位的行键；
* 证据有版本号。补记、复核更正、补采结果确认都会追加一个新版本，
  旧版本原样保留，因此历史报告仍能按旧版本复算原结论；
* 发布点 (release) 记录“本期正式报告采用的是哪一版证据”。生成期间发生
  补记会让当前证据版本越过发布点，生成方必须显式选择沿用本期原证据还是
  确认前的新版本，不会悄悄混用；
* 同一条失败物理行只保留一份结论。复核更正绑定原行与原结论做替换；
  补采作业领走的失败行在更正时保持原任务引用不变；
* 补记回执按回执号幂等，重传不会把同一次失败重复写入计数。

计数模型
--------
一条证据版本同时容纳两类事实：

* ``lines``：逐个物理行的结论，新采集路径全部走这里；
* ``extra``：没有物理行可绑定的汇总计数（旧入口只有数字时使用）。

版本的总计数 = 逐行计数 + extra 计数。两种来源不会互相覆盖，所以旧入口
按原分母算出来的数字能被原样复现，新路径又能对每条失败原行负责。

入口
----
`IngestPipeline.compute_ingest_metrics` 与
`IngestPipeline.generate_quality_report` 仍然保持原有参数与返回值，
内部改为向本模块登记/读取证据，保证两个入口对同一批计数同源。
"""
from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.core.models import IngestMetric, QualityReport

# 行结果
LINE_PROCESSED = "processed"        # 格式有效，计入分母
LINE_SKIPPED = "skipped"            # 格式不匹配/被跳过，计入分母
# 解析结论
PARSE_OK = "ok"
PARSE_FAILED = "failed"
PARSE_NOT_ATTEMPTED = "not_attempted"

# 报告生成流程
REPORT_GENERATING = "生成中"
REPORT_GENERATED = "已生成"

COUNTER_KEYS = (
    "processed", "skipped", "parse_attempted", "parse_ok", "failure_lines",
)


def line_key(source_id: str, line_number: int, raw_line: str) -> str:
    """物理行键：来源 + 行号 + 行内容指纹。

    不依赖可变计数，同一行在补记、更正、重启后仍是同一个键。
    """
    digest = hashlib.sha256(raw_line.encode("utf-8")).hexdigest()[:24]
    return f"{source_id}#{line_number}#{digest}"


def make_line_fact(
    source_id: str,
    line_number: int,
    raw_line: str,
    *,
    outcome: str = LINE_PROCESSED,
    parse_attempted: bool | None = None,
    parse_result: str | None = None,
    origin: str = "补记",
    rework_task: str = "",
) -> "LineFact":
    """构造一条可绑定到证据的物理行事实。"""
    if parse_attempted is None:
        parse_attempted = outcome == LINE_PROCESSED
    if parse_result is None:
        parse_result = PARSE_OK if (parse_attempted and outcome == LINE_PROCESSED) \
            else PARSE_FAILED if parse_attempted else PARSE_NOT_ATTEMPTED
    return LineFact(
        key=line_key(source_id, line_number, raw_line),
        source_id=source_id, line_number=line_number, raw_line=raw_line,
        outcome=outcome, parse_attempted=parse_attempted,
        parse_result=parse_result, origin=origin, rework_task=rework_task,
    )


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator > 0 else 0.0


@dataclass
class LineFact:
    """一条物理行的当前结论。"""
    key: str
    source_id: str
    line_number: int
    raw_line: str
    outcome: str = LINE_PROCESSED
    parse_result: str = PARSE_NOT_ATTEMPTED
    parse_attempted: bool = False
    origin: str = "采集"          # 采集 / 补记 / 复核更正 / 补采确认
    receipt_id: str = ""          # 补记回执号，用于幂等
    rework_task: str = ""         # 补采作业领取时写入，更正不得换掉
    superseded: str = ""          # 被替换掉的旧结论（保留可追溯）
    revision: int = 1

    def clone(self, **changes: Any) -> "LineFact":
        values = {
            "key": self.key,
            "source_id": self.source_id,
            "line_number": self.line_number,
            "raw_line": self.raw_line,
            "outcome": self.outcome,
            "parse_result": self.parse_result,
            "parse_attempted": self.parse_attempted,
            "origin": self.origin,
            "receipt_id": self.receipt_id,
            "rework_task": self.rework_task,
            "superseded": self.superseded,
            "revision": self.revision,
        }
        values.update(changes)
        return LineFact(**values)

    def as_record(self) -> dict[str, Any]:
        return {
            "line_key": self.key,
            "source_id": self.source_id,
            "line_number": self.line_number,
            "raw_line": self.raw_line,
            "outcome": self.outcome,
            "parse_result": self.parse_result,
            "parse_attempted": self.parse_attempted,
            "origin": self.origin,
            "receipt_id": self.receipt_id,
            "rework_task": self.rework_task,
            "superseded": self.superseded,
            "revision": self.revision,
        }

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "LineFact":
        return cls(
            key=record["line_key"], source_id=record["source_id"],
            line_number=int(record["line_number"]),
            raw_line=record.get("raw_line", ""),
            outcome=record.get("outcome", LINE_PROCESSED),
            parse_result=record.get("parse_result", PARSE_NOT_ATTEMPTED),
            parse_attempted=bool(record.get("parse_attempted")),
            origin=record.get("origin", "采集"),
            receipt_id=record.get("receipt_id", ""),
            rework_task=record.get("rework_task", ""),
            superseded=record.get("superseded", ""),
            revision=int(record.get("revision", 1)),
        )


@dataclass
class EvidenceVersion:
    """一份不可变的证据快照。"""
    evidence_id: str
    version: int
    period: str
    batch_id: str
    created_at: datetime
    lines: dict[str, LineFact]
    note: str = ""
    extra: dict[str, int] = field(default_factory=dict)

    # -- 逐行计数 --

    def _line_counters(self) -> dict[str, int]:
        counters = {name: 0 for name in COUNTER_KEYS}
        for fact in self.lines.values():
            counters[LINE_PROCESSED] += 1 if fact.outcome == LINE_PROCESSED else 0
            counters[LINE_SKIPPED] += 1 if fact.outcome == LINE_SKIPPED else 0
            counters["parse_attempted"] += 1 if fact.parse_attempted else 0
            counters["parse_ok"] += 1 if (
                fact.parse_attempted and fact.parse_result == PARSE_OK) else 0
            counters["failure_lines"] += 1 if (
                fact.parse_attempted and fact.parse_result == PARSE_FAILED) else 0
        return counters

    def counters(self) -> dict[str, int]:
        total = self._line_counters()
        for name in COUNTER_KEYS:
            total[name] += int(self.extra.get(name, 0))
        return total

    @property
    def processed(self) -> int:
        return self.counters()["processed"]

    @property
    def skipped(self) -> int:
        return self.counters()["skipped"]

    @property
    def total(self) -> int:
        return self.processed + self.skipped

    @property
    def parse_attempted(self) -> int:
        return self.counters()["parse_attempted"]

    @property
    def parse_ok(self) -> int:
        return self.counters()["parse_ok"]

    @property
    def failure_lines(self) -> int:
        """当前仍以失败结论存在的物理行数（一条行只算一次）。"""
        return self.counters()["failure_lines"]

    def completeness(self) -> float:
        return _rate(self.processed, self.total)

    def parse_success(self) -> float:
        return _rate(self.parse_ok, self.parse_attempted)

    def snapshot(self) -> list[dict[str, Any]]:
        return [fact.as_record() for fact in sorted(
            self.lines.values(), key=lambda f: (f.source_id, f.line_number))]

    def as_state(self) -> dict[str, Any]:
        return {
            "evidence_id": self.evidence_id,
            "version": self.version,
            "period": self.period,
            "batch_id": self.batch_id,
            "created_at": self.created_at.isoformat(),
            "note": self.note,
            "extra": dict(self.extra),
            "lines": self.snapshot(),
        }

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "EvidenceVersion":
        return cls(
            evidence_id=state["evidence_id"],
            version=int(state["version"]),
            period=state.get("period", ""),
            batch_id=state.get("batch_id", ""),
            created_at=datetime.fromisoformat(state["created_at"]),
            lines={record["line_key"]: LineFact.from_record(record)
                   for record in state.get("lines", [])},
            note=state.get("note", ""),
            extra={key: int(value) for key, value in state.get("extra", {}).items()},
        )


@dataclass
class EvidenceRelease:
    """发布点：本期正式报告采用哪一版证据。"""
    evidence_id: str
    period: str
    batch_id: str
    version: int
    note: str = ""


@dataclass
class ReportDraft:
    """生成中但尚未确认的报告，持有开始生成时的证据版本。"""
    draft_id: str
    evidence_id: str
    period: str
    batch_id: str
    source_id: str
    base_version: int
    metric_id: str
    fields_coverage: float
    process_delay_ms: float = 0.0
    started_at: datetime = field(default_factory=datetime.now)


def _report_to_state(report: QualityReport) -> dict[str, Any]:
    return {
        "report_id": report.report_id,
        "source_id": report.source_id,
        "completeness_rate": report.completeness_rate,
        "parse_success_rate": report.parse_success_rate,
        "field_coverage_rate": report.field_coverage_rate,
        "time_delay_seconds": report.time_delay_seconds,
        "eval_period": report.eval_period,
        "state": report.state,
        "evidence_id": report.evidence_id,
        "evidence_version": report.evidence_version,
        "period": report.period,
        "batch_id": report.batch_id,
        "metric_id": report.metric_id,
        "processed_lines": report.processed_lines,
        "skipped_lines": report.skipped_lines,
        "parse_attempted": report.parse_attempted,
        "parse_ok": report.parse_ok,
        "failure_lines": report.failure_lines,
    }


class IngestEvidenceLedger:
    """采集计量证据账本：登记行结论、追加版本、发布与并发确认。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._versions: dict[str, list[EvidenceVersion]] = {}
        self._releases: dict[str, EvidenceRelease] = {}
        self._drafts: dict[str, ReportDraft] = {}
        self._receipts: dict[str, str] = {}
        self._published: dict[tuple[str, str, str], QualityReport] = {}

    # ---------- 基础 ----------

    @staticmethod
    def evidence_id_for(source_id: str, batch_id: str, period: str) -> str:
        return f"EVID-{source_id}-{batch_id or 'default'}-{period or 'default'}"

    def _current(self, evidence_id: str) -> EvidenceVersion:
        return self._versions[evidence_id][-1]

    def _append(
        self,
        evidence_id: str,
        note: str,
        mutate: Any = None,
        extra: dict[str, int] | None = None,
    ) -> EvidenceVersion:
        """复制当前版本，按 mutate 改副本行、替换 extra，再追加为新版本。"""
        current = self._current(evidence_id)
        lines = {key: fact.clone() for key, fact in current.lines.items()}
        if mutate is not None:
            mutate(lines)
        version = EvidenceVersion(
            evidence_id=evidence_id,
            version=current.version + 1,
            period=current.period,
            batch_id=current.batch_id,
            created_at=datetime.now(),
            lines=lines,
            note=note,
            extra=dict(current.extra if extra is None else extra),
        )
        self._versions[evidence_id].append(version)
        return version

    def ensure(self, source_id: str, batch_id: str, period: str) -> EvidenceVersion:
        """取得或建立证据的第一版。"""
        evidence_id = self.evidence_id_for(source_id, batch_id, period)
        with self._lock:
            if evidence_id not in self._versions:
                self._versions[evidence_id] = [EvidenceVersion(
                    evidence_id=evidence_id, version=1, period=period,
                    batch_id=batch_id, created_at=datetime.now(), lines={},
                )]
            return self._current(evidence_id)

    # ---------- 计数登记 ----------

    def bind_counters(
        self,
        *,
        evidence_id: str,
        processed: int | None = None,
        skipped: int | None = None,
        parse_attempted: int | None = None,
        parse_ok: int | None = None,
        note: str = "统计提交",
    ) -> EvidenceVersion:
        """把汇总计数绑定到证据。

        传入的 processed/skipped/parse_* 是“本批应有的总数”，不是增量：
        先扣除逐行事实已经提供的部分，差额写进 extra。因此同一批计数
        重复提交不会翻倍，这也是旧入口幂等的关键。
        """
        with self._lock:
            current = self._current(evidence_id)
            derived = current._line_counters()
            extra = {name: int(current.extra.get(name, 0)) for name in COUNTER_KEYS}

            def set_total(name: str, desired: int | None) -> None:
                if desired is None:
                    return
                extra[name] = max(int(desired) - derived[name], 0)

            set_total("processed", processed)
            set_total("skipped", skipped)
            if parse_attempted is not None:
                set_total("parse_attempted", parse_attempted)
            if parse_ok is not None:
                set_total("parse_ok", parse_ok)
            if parse_attempted is not None or parse_ok is not None:
                desired_failures = max(
                    (parse_attempted if parse_attempted is not None
                     else current.parse_attempted)
                    - (parse_ok if parse_ok is not None else current.parse_ok),
                    0)
                extra["failure_lines"] = max(
                    desired_failures - derived["failure_lines"], 0)

            candidate = EvidenceVersion(
                evidence_id=evidence_id, version=current.version,
                period=current.period, batch_id=current.batch_id,
                created_at=current.created_at, lines=current.lines,
                note=current.note, extra=extra,
            )
            if candidate.counters() == current.counters():
                return current
            return self._append(evidence_id, note, extra=extra)

    def can_represent(
        self,
        *,
        evidence_id: str,
        processed: int | None = None,
        skipped: int | None = None,
        parse_attempted: int | None = None,
        parse_ok: int | None = None,
    ) -> bool:
        """检查一组总数是否与现有证据描述同一批计数。

        规则：处理/跳过计数必须与已登记总数完全一致（否则就是另一套口径，
        不能改写物理行）；解析计数允许从指标端向报告端扩展。
        """
        with self._lock:
            current = self._current(evidence_id)
            if not current.lines and not current.extra:
                return True
            existing = current.counters()
            if processed is not None and int(processed) != existing["processed"]:
                return False
            if skipped is not None and int(skipped) != existing["skipped"]:
                return False
            derived = current._line_counters()
            if parse_attempted is not None and int(parse_attempted) < derived["parse_attempted"]:
                return False
            if parse_ok is not None and int(parse_ok) < derived["parse_ok"]:
                return False
            if parse_attempted is not None or parse_ok is not None:
                desired_attempts = (int(parse_attempted) if parse_attempted is not None
                                    else existing["parse_attempted"])
                desired_ok = (int(parse_ok) if parse_ok is not None
                              else existing["parse_ok"])
                if desired_ok > desired_attempts:
                    return False
                # 已知失败行不能被“多一条成功计数”抹平
                if desired_attempts - desired_ok < derived["failure_lines"]:
                    return False
            return True

    def counter_snapshot(
        self,
        *,
        base_evidence_id: str,
        counters: dict[str, int],
        note: str = "直接传参统计",
    ) -> EvidenceVersion:
        """为一组只有数字、无法叠加到逐行事实上的计数建立独立快照。

        旧入口的直接传参必须按原分母计算；当这些分母与已有逐行事实冲突时，
        用一份只含汇总计数的证据版本承载它们，报告整份取自这一版，既不
        篡改已登记的物理行，也不引入无出处的独立计算。
        """
        with self._lock:
            fingerprint = "-".join(
                f"{name}{int(counters.get(name, 0))}" for name in COUNTER_KEYS)
            evidence_id = f"{base_evidence_id}::direct::{fingerprint}"
            if evidence_id not in self._versions:
                self._versions[evidence_id] = [EvidenceVersion(
                    evidence_id=evidence_id, version=1, period="",
                    batch_id="", created_at=datetime.now(), lines={},
                    note=note,
                    extra={name: int(counters.get(name, 0)) for name in COUNTER_KEYS},
                )]
            return self._current(evidence_id)

    def record_lines(
        self,
        *,
        source_id: str,
        period: str,
        batch_id: str,
        facts: list[LineFact],
        note: str = "采集",
    ) -> EvidenceVersion:
        """登记一批带物理行键的行结论（采集主流程）。"""
        with self._lock:
            version = self.ensure(source_id, batch_id, period)
            if not facts:
                return version

            def mutate(lines: dict[str, LineFact]) -> None:
                for fact in facts:
                    lines[fact.key] = fact.clone()

            return self._append(version.evidence_id, note, mutate)

    # ---------- 补记 ----------

    def amend(
        self,
        *,
        evidence_id: str,
        receipt_id: str,
        facts: list[LineFact],
        note: str = "",
    ) -> tuple[EvidenceVersion, bool]:
        """补记回执：按回执号幂等追加一行/一批行。

        返回 (最新版本, 是否生效)；同一回执重传返回 (当前版本, False)，
        不会把这条失败再次写入计数。
        """
        if not receipt_id:
            raise ValueError("补记必须携带回执号")
        with self._lock:
            if receipt_id in self._receipts:
                return self._current(evidence_id), False

            def mutate(lines: dict[str, LineFact]) -> None:
                for fact in facts:
                    lines[fact.key] = fact.clone(receipt_id=receipt_id)

            version = self._append(
                evidence_id, note or f"补记回执 {receipt_id}", mutate)
            self._receipts[receipt_id] = evidence_id
            return version, True

    # ---------- 复核更正 ----------

    def correct(
        self,
        *,
        evidence_id: str,
        target_line_key: str,
        expected_parse_result: str,
        new_parse_result: str,
        note: str = "复核更正",
    ) -> tuple[EvidenceVersion, bool]:
        """把已登记的一次失败改判为成功（或反向）。

        必须绑定原物理行与旧结论：旧结论不匹配则拒绝，避免把更正
        当作一条新日志追加。补采作业已领取的任务引用保持不变。
        """
        with self._lock:
            current = self._current(evidence_id)
            fact = current.lines.get(target_line_key)
            if fact is None:
                raise KeyError(f"行 {target_line_key} 不在证据 {evidence_id} 中")
            if fact.parse_result != expected_parse_result:
                raise ValueError(
                    f"行 {target_line_key} 当前结论为 {fact.parse_result}，"
                    f"与期望的旧结论 {expected_parse_result} 不符")

            def mutate(lines: dict[str, LineFact]) -> None:
                target = lines[target_line_key]
                lines[target_line_key] = target.clone(
                    parse_result=new_parse_result,
                    parse_attempted=True,
                    origin=note,
                    superseded=expected_parse_result,
                    revision=target.revision + 1,
                )

            version = self._append(
                evidence_id,
                f"{note}：行 {target_line_key} {expected_parse_result}→{new_parse_result}",
                mutate,
            )
            return version, True

    # ---------- 补采作业 ----------

    def claim_failed_lines(
        self,
        *,
        evidence_id: str,
        task_id: str,
        limit: int | None = None,
        include_claimed: bool = False,
    ) -> list[LineFact]:
        """复核人从报告领出失败原行，交给补采作业。

        领取后该行的 rework_task 绑定任务号；即使随后发生计数更正，
        任务引用也不会被换掉。恢复后重领同一失败范围时传
        include_claimed=True：已经补采成功（不再是失败）的行不会再次出现；
        仍属其他作业的失败行保留原任务引用，以便回到原作业核对实际处理事实，
        而不会在换一个作业号后被重新执行一遍。
        """
        with self._lock:
            current = self._current(evidence_id)
            candidates = [
                fact for fact in sorted(
                    current.lines.values(),
                    key=lambda f: (f.source_id, f.line_number))
                if fact.parse_attempted and fact.parse_result == PARSE_FAILED
                and (include_claimed or not fact.rework_task)
            ]
            if limit is not None:
                candidates = candidates[:limit]
            if not candidates:
                return []
            keys = [fact.key for fact in candidates]
            unowned = [fact.key for fact in candidates if not fact.rework_task]

            def mutate(lines: dict[str, LineFact]) -> None:
                for key in unowned:
                    lines[key] = lines[key].clone(rework_task=task_id)

            if unowned:
                version = self._append(
                    evidence_id, f"补采作业 {task_id} 领取 {len(unowned)} 行", mutate)
            else:
                version = current
            return [version.lines[key] for key in keys]

    def confirm_rework(
        self,
        *,
        evidence_id: str,
        task_id: str,
        processed_keys: list[str],
        attempted_keys: list[str] | None = None,
        note: str = "补采确认",
    ) -> EvidenceVersion:
        """补采结果确认：只按补采实际处理过的物理行推进。

        未登记结果的失败行保持原结论，等待下次恢复；已处理过的行不会
        被再次执行，也不通过追加成功计数来掩盖旧失败。
        """
        with self._lock:
            current = self._current(evidence_id)
            processed_keys = list(processed_keys or [])
            attempted_keys = list(attempted_keys or [])
            if not processed_keys and not attempted_keys:
                return current
            for key in [*processed_keys, *attempted_keys]:
                fact = current.lines.get(key)
                if fact is None:
                    raise KeyError(f"行 {key} 不在证据 {evidence_id} 中")
                if fact.rework_task != task_id:
                    raise ValueError(f"行 {key} 不属于补采作业 {task_id}")

            def mutate(lines: dict[str, LineFact]) -> None:
                for key in processed_keys:
                    target = lines[key]
                    lines[key] = target.clone(
                        parse_result=PARSE_OK,
                        parse_attempted=True,
                        origin=note,
                        superseded=target.superseded or PARSE_FAILED,
                        revision=target.revision + 1,
                    )
                for key in attempted_keys:
                    if key in processed_keys:
                        continue
                    target = lines[key]
                    lines[key] = target.clone(
                        parse_result=PARSE_FAILED,
                        parse_attempted=True,
                        origin=note,
                        revision=target.revision + 1,
                    )

            return self._append(
                evidence_id,
                f"{note}：作业 {task_id} 完成 {len(processed_keys)} 行",
                mutate,
            )

    def recover_rework(
        self,
        *,
        evidence_id: str,
        task_id: str,
        processed_keys: list[str],
        attempted_keys: list[str] | None = None,
    ) -> EvidenceVersion:
        """重启恢复：按补采实际处理事实核对，再登记结果。

        processed_keys 是补采确实已经做成功的原行；attempted_keys 是试过
        仍然失败的原行。二者之外的失败行保持原状，不会被重复执行。
        """
        return self.confirm_rework(
            evidence_id=evidence_id, task_id=task_id,
            processed_keys=processed_keys, attempted_keys=attempted_keys,
            note="补采恢复",
        )

    # ---------- 发布点与并发 ----------

    def _report_for(
        self,
        *,
        evidence_id: str,
        version_number: int,
        source_id: str,
        report_id: str,
        fields_coverage: float,
        process_delay_ms: float,
        period: str,
        batch_id: str,
        metric_id: str,
        state: str = REPORT_GENERATED,
    ) -> QualityReport:
        evidence = self._versions[evidence_id][version_number - 1]
        counters = evidence.counters()
        return QualityReport(
            report_id=report_id,
            source_id=source_id,
            completeness_rate=evidence.completeness(),
            parse_success_rate=evidence.parse_success(),
            field_coverage_rate=round(fields_coverage, 4),
            time_delay_seconds=process_delay_ms / 1000.0,
            eval_period=period,
            state=state,
            evidence_id=evidence_id,
            evidence_version=evidence.version,
            period=period,
            batch_id=batch_id,
            metric_id=metric_id,
            processed_lines=counters["processed"],
            skipped_lines=counters["skipped"],
            parse_attempted=counters["parse_attempted"],
            parse_ok=counters["parse_ok"],
            failure_lines=counters["failure_lines"],
        )

    def start_report(
        self,
        *,
        evidence_id: str,
        period: str,
        batch_id: str,
        source_id: str,
        metric_id: str,
        fields_coverage: float,
        process_delay_ms: float = 0.0,
        release_version: int | None = None,
    ) -> ReportDraft:
        """开始生成报告：锁定本期发布点对应的证据版本。"""
        with self._lock:
            current = self._current(evidence_id)
            if release_version is None:
                release = self._releases.get(period)
                if release and release.evidence_id == evidence_id \
                        and release.version <= len(self._versions[evidence_id]):
                    release_version = release.version
                else:
                    release_version = current.version
            draft_id = f"DRAFT-{evidence_id}-{period}"
            draft = ReportDraft(
                draft_id=draft_id, evidence_id=evidence_id, period=period,
                batch_id=batch_id, source_id=source_id,
                base_version=release_version, metric_id=metric_id,
                fields_coverage=fields_coverage,
                process_delay_ms=process_delay_ms,
            )
            self._drafts[draft_id] = draft
            return draft

    def draft_report(self, draft_id: str) -> QualityReport:
        """按草稿锁定的版本产出报告内容（此时状态仍是生成中）。"""
        with self._lock:
            draft = self._drafts[draft_id]
            return self._report_for(
                evidence_id=draft.evidence_id,
                version_number=draft.base_version,
                source_id=draft.source_id,
                report_id=f"QUAL-{draft.metric_id}-{draft.period}",
                fields_coverage=draft.fields_coverage,
                process_delay_ms=draft.process_delay_ms,
                period=draft.period,
                batch_id=draft.batch_id,
                metric_id=draft.metric_id,
                state=REPORT_GENERATING,
            )

    def draft_conflict(self, draft_id: str) -> bool:
        """生成期间当前证据版本是否已越过发布点。"""
        with self._lock:
            draft = self._drafts[draft_id]
            return self._current(draft.evidence_id).version != draft.base_version

    def version_report(
        self,
        evidence_id: str,
        version: int,
        *,
        metric_id: str = "",
        source_id: str = "",
        period: str = "",
        batch_id: str = "",
        fields_coverage: float = 0.0,
        process_delay_ms: float = 0.0,
        state: str = REPORT_GENERATED,
    ) -> QualityReport:
        """按指定版本复算旧结论（旧报告复算用）。"""
        with self._lock:
            report_id = f"QUAL-{metric_id or evidence_id}-{period or 'replay'}"
            return self._report_for(
                evidence_id=evidence_id, version_number=version,
                source_id=source_id, report_id=report_id,
                fields_coverage=fields_coverage,
                process_delay_ms=process_delay_ms,
                period=period, batch_id=batch_id, metric_id=metric_id,
                state=state,
            )

    def confirm_report(
        self,
        *,
        draft_id: str,
        use_current: bool = False,
        release: bool = True,
    ) -> tuple[QualityReport, bool]:
        """确认报告：同一期报告只确认一次。

        生成期间若发生补记，`use_current=False` 沿用本期原证据，
        `use_current=True` 改用确认前最新版本，两种选择都整份取自单一版本。
        """
        with self._lock:
            draft = self._drafts.get(draft_id)
            if draft is None:
                raise KeyError(f"生成草稿 {draft_id} 不存在")
            published_key = (draft.evidence_id, draft.period, draft.batch_id)
            if published_key in self._published:
                return self._published[published_key], False
            chosen_version = (
                self._current(draft.evidence_id).version
                if use_current else draft.base_version)
            report = self._report_for(
                evidence_id=draft.evidence_id,
                version_number=chosen_version,
                source_id=draft.source_id,
                report_id=f"QUAL-{draft.metric_id}-{draft.period}",
                fields_coverage=draft.fields_coverage,
                process_delay_ms=draft.process_delay_ms,
                period=draft.period,
                batch_id=draft.batch_id,
                metric_id=draft.metric_id,
            )
            self._published[published_key] = report
            if release:
                self._releases[draft.period] = EvidenceRelease(
                    evidence_id=draft.evidence_id, period=draft.period,
                    batch_id=draft.batch_id, version=chosen_version,
                    note=report.report_id,
                )
            return report, True

    def publish(
        self,
        *,
        evidence_id: str,
        period: str,
        batch_id: str,
        source_id: str,
        report_id: str,
        fields_coverage: float,
        process_delay_ms: float,
        metric_id: str = "",
        release: bool = True,
    ) -> tuple[QualityReport, bool]:
        """按当前证据版本发布本期报告；同一期只发布一次。"""
        with self._lock:
            current = self._current(evidence_id)
            published_key = (evidence_id, period, batch_id)
            if period and published_key in self._published:
                return self._published[published_key], False
            report = self._report_for(
                evidence_id=evidence_id, version_number=current.version,
                source_id=source_id, report_id=report_id,
                fields_coverage=fields_coverage,
                process_delay_ms=process_delay_ms,
                period=period, batch_id=batch_id, metric_id=metric_id,
            )
            if period:
                self._published[published_key] = report
                if release:
                    self._releases[period] = EvidenceRelease(
                        evidence_id=evidence_id, period=period,
                        batch_id=batch_id, version=current.version,
                        note=report.report_id,
                    )
            return report, True

    # ---------- 一致性校验 ----------

    def report_matches_evidence(self, report: QualityReport) -> bool:
        """校验报告的两项比例、计数是否与其声明的证据版本一致。"""
        if not report.evidence_id or report.evidence_version is None:
            return False
        with self._lock:
            versions = self._versions.get(report.evidence_id)
            if not versions or report.evidence_version > len(versions):
                return False
            evidence = versions[report.evidence_version - 1]
            counters = evidence.counters()
            return (
                report.completeness_rate == evidence.completeness()
                and report.parse_success_rate == evidence.parse_success()
                and report.processed_lines == counters["processed"]
                and report.skipped_lines == counters["skipped"]
                and report.parse_attempted == counters["parse_attempted"]
                and report.parse_ok == counters["parse_ok"]
                and report.failure_lines == counters["failure_lines"]
            )

    # ---------- 查询 ----------

    def current(self, evidence_id: str) -> EvidenceVersion:
        with self._lock:
            return self._current(evidence_id)

    def exists(self, evidence_id: str | None) -> bool:
        with self._lock:
            return bool(evidence_id) and evidence_id in self._versions

    def version(self, evidence_id: str, version: int) -> EvidenceVersion:
        with self._lock:
            return self._versions[evidence_id][version - 1]

    def versions(self, evidence_id: str) -> list[EvidenceVersion]:
        with self._lock:
            return list(self._versions.get(evidence_id, []))

    def received(self, receipt_id: str) -> bool:
        """回执是否已被接收过（用于判断重传）。"""
        with self._lock:
            return receipt_id in self._receipts

    def release(self, period: str) -> EvidenceRelease | None:
        with self._lock:
            return self._releases.get(period)

    def published(self, evidence_id: str, period: str,
                  batch_id: str) -> QualityReport | None:
        with self._lock:
            return self._published.get((evidence_id, period, batch_id))

    # ---------- 重启恢复 ----------

    def dump_state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "versions": {
                    evidence_id: [version.as_state() for version in versions]
                    for evidence_id, versions in self._versions.items()
                },
                "releases": {
                    period: {
                        "evidence_id": release.evidence_id,
                        "period": release.period,
                        "batch_id": release.batch_id,
                        "version": release.version,
                        "note": release.note,
                    }
                    for period, release in self._releases.items()
                },
                "receipts": dict(self._receipts),
                "published": [
                    {
                        "key": list(key),
                        "report": _report_to_state(report),
                    }
                    for key, report in self._published.items()
                ],
            }

    def load_state(self, state: dict[str, Any]) -> None:
        with self._lock:
            self._versions = {
                evidence_id: [EvidenceVersion.from_state(item) for item in versions]
                for evidence_id, versions in state.get("versions", {}).items()
            }
            self._releases = {
                period: EvidenceRelease(**values)
                for period, values in state.get("releases", {}).items()
            }
            self._receipts = dict(state.get("receipts", {}))
            self._published = {
                tuple(item["key"]): QualityReport(**item["report"])
                for item in state.get("published", [])
            }
            self._drafts = {}

    @classmethod
    def from_state(cls, state: dict[str, Any]) -> "IngestEvidenceLedger":
        ledger = cls()
        ledger.load_state(state)
        return ledger


def facts_from_entries(
    source_id: str, entries: list[Any], *, origin: str = "采集",
) -> list[LineFact]:
    """把 LogEntry 列表转成带物理行键的事实（供采集主流程登记）。"""
    facts: list[LineFact] = []
    for entry in entries:
        key = line_key(source_id, entry.line_number, entry.raw_line)
        status = getattr(entry.parse_status, "value", str(entry.parse_status))
        if status == "format_mismatch":
            outcome = LINE_SKIPPED
            attempted = False
            parse_result = PARSE_NOT_ATTEMPTED
        else:
            outcome = LINE_PROCESSED
            attempted = True
            parse_result = PARSE_OK if status == "ok" else PARSE_FAILED
        facts.append(LineFact(
            key=key, source_id=source_id, line_number=entry.line_number,
            raw_line=entry.raw_line, outcome=outcome, parse_attempted=attempted,
            parse_result=parse_result, origin=origin,
        ))
    return facts


def metric_from_evidence(
    *,
    evidence_id: str,
    version: EvidenceVersion,
    source_id: str,
    metric_id: str,
    ingest_rate: float,
    process_delay_ms: float,
    total_lines: int | None = None,
    period: str = "",
    batch_id: str = "",
) -> IngestMetric:
    """采集指标仍按自身口径计算，只把计数绑定到证据版本。"""
    total = version.total if total_lines is None else total_lines
    backlog = total - version.processed - version.skipped
    return IngestMetric(
        metric_id=metric_id,
        source_id=source_id,
        ingest_rate=round(ingest_rate, 2),
        process_delay_ms=round(process_delay_ms, 2),
        error_rate=round(version.skipped / total, 4) if total > 0 else 0.0,
        backlog_lines=max(backlog, 0),
        evidence_id=evidence_id,
        evidence_version=version.version,
        period=period,
        batch_id=batch_id,
        total_lines=total,
        processed_lines=version.processed,
        skipped_lines=version.skipped,
        parse_attempted=version.parse_attempted,
        parse_ok=version.parse_ok,
    )


def report_from_evidence(
    *,
    evidence_id: str,
    version: EvidenceVersion,
    source_id: str,
    report_id: str,
    fields_coverage: float,
    process_delay_ms: float,
    period: str,
    batch_id: str,
    metric_id: str = "",
) -> QualityReport:
    """质量报告的两项比例整份取自同一证据版本。"""
    counters = version.counters()
    return QualityReport(
        report_id=report_id,
        source_id=source_id,
        completeness_rate=version.completeness(),
        parse_success_rate=version.parse_success(),
        field_coverage_rate=round(fields_coverage, 4),
        time_delay_seconds=process_delay_ms / 1000.0,
        eval_period=period,
        state=REPORT_GENERATED,
        evidence_id=evidence_id,
        evidence_version=version.version,
        period=period,
        batch_id=batch_id,
        metric_id=metric_id,
        processed_lines=counters["processed"],
        skipped_lines=counters["skipped"],
        parse_attempted=counters["parse_attempted"],
        parse_ok=counters["parse_ok"],
        failure_lines=counters["failure_lines"],
    )
