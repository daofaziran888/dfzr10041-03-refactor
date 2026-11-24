"""测试：采集计量证据——指标与质量报告同源、补记幂等、复核更正、补采恢复与并发发布。

覆盖数据流 8 的计量侧：
- 旧入口 compute_ingest_metrics / generate_quality_report 的输入输出不变；
- 两个旧入口与新计量路径对同一批日志得到同一分母；
- 补记回执幂等，复核更正绑定原物理行，补采作业不被计数更正换掉；
- 补采恢复只按实际处理事实推进，不重复执行原行；
- 生成期间插入补记时，报告整份取自单一证据版本，同期望只发布一次。
"""
from __future__ import annotations

import sys
import os
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.evidence import (
    LINE_SKIPPED, PARSE_FAILED, PARSE_OK, IngestEvidenceLedger,
    line_key, make_line_fact,
)
from app.core.models import IngestJob
from app.core.pipeline import (
    PRESET_LOG_SOURCES, PRESET_PARSE_RULES, IngestPipeline, make_pipeline,
)

SOURCE = "LOG_-0001"
PERIOD = "2026-11-W3"
BATCH = "BATCH-001"


def _evidence_id(pipeline: IngestPipeline) -> str:
    return pipeline.evidence.evidence_id_for(SOURCE, BATCH, PERIOD)


def _first_evidence(pipeline: IngestPipeline):
    """首份证据：处理一行、跳过一行，解析试了一行且成功。"""
    evidence = pipeline.evidence.ensure(SOURCE, BATCH, PERIOD)
    processed_line = '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /a HTTP/1.1" 200 1 "-" "x"'
    skipped_line = "2026-11-15 14:30:06.234 orphan line without proper format"
    version = pipeline.evidence.record_lines(
        source_id=SOURCE, period=PERIOD, batch_id=BATCH,
        facts=[
            make_line_fact(SOURCE, 1, processed_line, origin="采集"),
            make_line_fact(SOURCE, 2, skipped_line, origin="采集",
                           outcome=LINE_SKIPPED, parse_attempted=False,
                           parse_result="not_attempted"),
        ],
    )
    return evidence.evidence_id, version, processed_line, skipped_line


class TestLegacyEntriesUnchanged:
    """两个旧入口保持原有输入输出口径。"""

    def test_metric_keeps_own_rates(self):
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id=SOURCE, batch_size=20)
        job.processed_lines = 10
        job.skipped_lines = 3

        metric = pipeline.compute_ingest_metrics(job, 5000.0, 15)
        assert metric.ingest_rate > 0
        assert metric.error_rate == round(3 / 15, 4)
        assert metric.backlog_lines == 2
        assert metric.evidence_id is not None

    def test_metric_empty_input_keeps_zero(self):
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id=SOURCE)
        metric = pipeline.compute_ingest_metrics(job, 0.0, 0)
        assert metric.ingest_rate == 0.0
        assert metric.error_rate == 0.0
        assert metric.backlog_lines == 0

    def test_report_legacy_denominators(self):
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id=SOURCE, batch_size=20)
        job.processed_lines = 10
        job.skipped_lines = 2
        metric = pipeline.compute_ingest_metrics(job, 3000.0, 15)

        report = pipeline.generate_quality_report(
            metric, parse_ok=10, parse_total=15, fields_coverage=0.875,
            processed=10, skipped=2)
        assert report.completeness_rate == round(10 / 12, 4)
        assert report.parse_success_rate == round(10 / 15, 4)
        assert report.field_coverage_rate == 0.875
        assert report.state == "已生成"

    def test_report_zero_lines_keeps_zero(self):
        """空输入时报告的两项比例保持原值。"""
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0", source_id=SOURCE)
        metric = pipeline.compute_ingest_metrics(job, 0.0, 0)
        report = pipeline.generate_quality_report(
            metric, parse_ok=0, parse_total=0, fields_coverage=0.0)
        assert report.completeness_rate == 0.0
        assert report.parse_success_rate == 0.0

    def test_direct_params_keep_original_denominators_on_conflict(self):
        """直接传参与已登记逐行事实冲突时，仍按传参的原分母计算。"""
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)       # 逐行 2 行
        metric = pipeline.compute_metrics_from_evidence(
            IngestJob(job_id="INGE-1", source_id=SOURCE, batch_id=BATCH,
                      period=PERIOD), 1000.0, evidence_id)
        report = pipeline.generate_quality_report(
            metric, parse_ok=7, parse_total=10, fields_coverage=1.0,
            processed=5, skipped=5)
        # 原分母：完整率 5/10，解析成功率 7/10
        assert report.completeness_rate == 0.5
        assert report.parse_success_rate == 0.7
        assert pipeline.evidence.report_matches_evidence(report)
        # 采集证据本身没有被直接传参篡改
        current = pipeline.evidence.current(evidence_id)
        assert current.processed == 1 and current.skipped == 1

    def test_direct_params_cannot_erase_known_failure(self):
        """直接传参不能把已知失败行从采集证据里抹掉。"""
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1",
            facts=[make_line_fact(
                SOURCE, 3, "trailing garbage line", origin="补记",
                outcome=LINE_SKIPPED, parse_attempted=True,
                parse_result=PARSE_FAILED)])
        metric = pipeline.compute_metrics_from_evidence(
            IngestJob(job_id="INGE-1", source_id=SOURCE, batch_id=BATCH,
                      period=PERIOD), 1000.0, evidence_id)
        report = pipeline.generate_quality_report(
            metric, parse_ok=2, parse_total=2, fields_coverage=1.0,
            processed=1, skipped=2)
        # 直接传参单独成版，不并入采集证据，也没有改写失败行
        assert report.evidence_id != evidence_id
        current = pipeline.evidence.current(evidence_id)
        assert current.failure_lines == 1
        assert current.parse_success() == 0.5
        # 快照版本按传参原分母计算：完整率 1/3、解析成功率 2/2
        assert report.completeness_rate == round(1 / 3, 4)
        assert report.parse_success_rate == 1.0
        assert pipeline.evidence.report_matches_evidence(report)

    def test_repeated_statistics_submission_does_not_double(self):
        """同一批计数重复提交，分母保持不变。"""
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-0001", source_id=SOURCE, batch_size=20)
        job.processed_lines = 10
        job.skipped_lines = 2
        first = pipeline.compute_ingest_metrics(job, 3000.0, 15)
        second = pipeline.compute_ingest_metrics(job, 3000.0, 15)
        assert first.evidence_version == second.evidence_version
        assert second.error_rate == first.error_rate
        assert second.backlog_lines == first.backlog_lines


class TestSameEvidenceDenominators:
    """旧入口与新计量路径比较分母。"""

    def test_first_evidence_ratios(self):
        """首份证据：处理一行、跳过一行、解析试一行成功 → 1/2 与 1。"""
        pipeline = make_pipeline()
        evidence_id, version, *_ = _first_evidence(pipeline)
        assert version.total == 2
        assert version.completeness() == 0.5
        assert version.parse_success() == 1.0
        assert version.parse_attempted == 1 and version.parse_ok == 1
        assert version.failure_lines == 0

    def test_legacy_and_evidence_paths_agree(self):
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]
        rule = PRESET_PARSE_RULES[0]
        raw = [
            '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /a HTTP/1.1" 200 1 "-" "x"',
            "2026-11-15 14:30:06.234 orphan line without proper format",
        ]
        entries = pipeline.apply_parse_rules(pipeline.parse_raw_lines(source, raw), rule)
        job = IngestJob(job_id="INGE-9", source_id=SOURCE, batch_size=10,
                        batch_id=BATCH, period=PERIOD)
        processed = pipeline.run_ingest_job(entries, job)
        assert job.processed_lines == 1 and job.skipped_lines == 1

        evidence = pipeline.record_ingest_evidence(
            SOURCE, processed, batch_id=BATCH, period=PERIOD)

        # 新计量路径
        new_metric = pipeline.compute_metrics_from_evidence(
            job, 1000.0, evidence.evidence_id)
        new_report = pipeline.report_from_evidence(
            new_metric, evidence.evidence_id, fields_coverage=1.0)
        assert new_report.completeness_rate == 0.5
        assert new_report.parse_success_rate == 1.0

        # 旧入口：直接传参按原分母算，得到同样的比例
        legacy_job = IngestJob(job_id="INGE-9", source_id=SOURCE, batch_size=10)
        legacy_job.processed_lines = job.processed_lines
        legacy_job.skipped_lines = job.skipped_lines
        legacy_metric = pipeline.compute_ingest_metrics(legacy_job, 1000.0, 2)
        legacy_report = pipeline.generate_quality_report(
            legacy_metric, parse_ok=1, parse_total=1, fields_coverage=1.0,
            processed=1, skipped=1)
        assert legacy_report.completeness_rate == new_report.completeness_rate
        assert legacy_report.parse_success_rate == new_report.parse_success_rate

    def test_old_entries_same_batch_share_one_evidence(self):
        """两个旧入口按同一批计数提交时落在同一份证据上，报告不另起分母。"""
        pipeline = make_pipeline()
        job = IngestJob(job_id="INGE-2", source_id=SOURCE, batch_size=10,
                        batch_id=BATCH, period=PERIOD)
        job.processed_lines, job.skipped_lines = 10, 2
        metric = pipeline.compute_ingest_metrics(job, 3000.0, 12)
        report = pipeline.generate_quality_report(
            metric, parse_ok=10, parse_total=12, fields_coverage=0.5,
            processed=10, skipped=2)
        assert report.evidence_id == metric.evidence_id
        assert report.evidence_version >= metric.evidence_version
        assert report.completeness_rate == round(10 / 12, 4)
        assert report.parse_success_rate == round(10 / 12, 4)
        assert pipeline.evidence.report_matches_evidence(report)
        # 指标端登记的处理/跳过计数没有被报告端改写
        version = pipeline.evidence.version(
            metric.evidence_id, metric.evidence_version)
        assert (version.processed, version.skipped) == (10, 2)

    def test_old_entries_other_batch_keeps_evidence_untouched(self):
        """另一批计数单独成版，不覆盖已登记的采集证据。"""
        pipeline = make_pipeline()
        evidence_id, before, *_ = _first_evidence(pipeline)
        job = IngestJob(job_id="INGE-3", source_id=SOURCE, batch_size=10)
        job.processed_lines, job.skipped_lines = 5, 5
        metric = pipeline.compute_ingest_metrics(job, 1000.0, 10)
        assert metric.evidence_id != evidence_id
        current = pipeline.evidence.current(evidence_id)
        assert (current.version, current.processed, current.skipped) == (
            before.version, 1, 1)
        assert metric.error_rate == 0.5 and metric.backlog_lines == 0

    def test_report_ratios_come_from_one_version(self):
        pipeline = make_pipeline()
        evidence_id, version, *_ = _first_evidence(pipeline)
        metric = pipeline.compute_metrics_from_evidence(
            IngestJob(job_id="INGE-1", source_id=SOURCE, batch_id=BATCH,
                      period=PERIOD), 1000.0, evidence_id)
        report = pipeline.report_from_evidence(metric, evidence_id, 1.0)
        assert report.evidence_version == version.version
        assert pipeline.evidence.report_matches_evidence(report)


class TestAmendReceipts:
    """补记回执的幂等与版本推进。"""

    def test_amend_adds_failed_skipped_line(self):
        pipeline = make_pipeline()
        evidence_id, version, *_ = _first_evidence(pipeline)
        base = pipeline.evidence.current(evidence_id)

        amended, applied = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1",
            facts=[make_line_fact(
                SOURCE, 3, "trailing garbage line", origin="补记",
                outcome=LINE_SKIPPED, parse_attempted=True,
                parse_result=PARSE_FAILED)],
        )
        assert applied is True
        assert amended.version == base.version + 1
        # 完整率 1/3，解析成功率 1/2
        assert amended.completeness() == round(1 / 3, 4)
        assert amended.parse_success() == 0.5
        assert amended.failure_lines == 1

    def test_receipt_replay_is_idempotent(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        fact = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)

        first, applied = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1", facts=[fact])
        assert applied
        replay, applied_again = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1", facts=[fact])
        assert applied_again is False
        assert replay.version == first.version
        assert replay.total == 3            # 没有写成两条
        assert replay.failure_lines == 1
        assert pipeline.evidence.received("RCPT-1")

    def test_different_receipt_same_line_does_not_double_count(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        fact = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)
        pipeline.evidence.amend(evidence_id=evidence_id, receipt_id="RCPT-A", facts=[fact])
        second, applied = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-B", facts=[fact])
        # 同一物理行被另一回执再写一次：仍只有一行
        assert applied is True
        assert second.total == 3
        assert second.failure_lines == 1


class TestReviewCorrection:
    """复核更正替换原行旧结论，而不是新增一条日志。"""

    def test_correct_replaces_failed_with_success(self):
        pipeline = make_pipeline()
        evidence_id, version, *_ = _first_evidence(pipeline)
        failed = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)
        amended, _ = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1", facts=[failed])
        before = amended.version

        corrected, ok = pipeline.evidence.correct(
            evidence_id=evidence_id,
            target_line_key=failed.key,
            expected_parse_result=PARSE_FAILED,
            new_parse_result=PARSE_OK,
        )
        assert ok
        assert corrected.version == before + 1
        assert corrected.total == 3                  # 行数不变，没有多一条日志
        assert corrected.failure_lines == 0
        assert corrected.parse_success() == 1.0
        record = corrected.lines[failed.key]
        assert record.origin == "复核更正"
        assert record.superseded == PARSE_FAILED
        assert record.revision == 2

    def test_correction_requires_original_conclusion(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        failed = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)
        pipeline.evidence.amend(evidence_id=evidence_id, receipt_id="RCPT-1", facts=[failed])
        pipeline.evidence.correct(
            evidence_id=evidence_id, target_line_key=failed.key,
            expected_parse_result=PARSE_FAILED, new_parse_result=PARSE_OK)
        try:
            pipeline.evidence.correct(
                evidence_id=evidence_id, target_line_key=failed.key,
                expected_parse_result=PARSE_FAILED, new_parse_result=PARSE_OK)
        except ValueError:
            pass
        else:
            raise AssertionError("旧结论已不是失败，重复更正应被拒绝")

    def test_unknown_line_is_not_treated_as_new_log(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        try:
            pipeline.evidence.correct(
                evidence_id=evidence_id,
                target_line_key=line_key(SOURCE, 99, "never seen"),
                expected_parse_result=PARSE_FAILED,
                new_parse_result=PARSE_OK)
        except KeyError:
            pass
        else:
            raise AssertionError("更正绑定不到原物理行时必须拒绝")
        assert pipeline.evidence.current(evidence_id).total == 2

    def test_old_report_still_recomputes(self):
        pipeline = make_pipeline()
        evidence_id, first_version, *_ = _first_evidence(pipeline)
        failed = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)
        pipeline.evidence.amend(evidence_id=evidence_id, receipt_id="RCPT-1", facts=[failed])
        pipeline.evidence.correct(
            evidence_id=evidence_id, target_line_key=failed.key,
            expected_parse_result=PARSE_FAILED, new_parse_result=PARSE_OK)

        replayed = pipeline.evidence.version_report(
            evidence_id, first_version.version, source_id=SOURCE,
            period=PERIOD, batch_id=BATCH, metric_id="INGE-OLD")
        assert replayed.completeness_rate == 0.5
        assert replayed.parse_success_rate == 1.0


class TestReworkJobs:
    """补采作业领走失败原行后的更正与恢复。"""

    def _three_line_evidence(self, pipeline):
        evidence_id, *_ = _first_evidence(pipeline)
        facts = [
            make_line_fact(SOURCE, 3, "bad line three", origin="补记",
                           outcome=LINE_SKIPPED, parse_attempted=True,
                           parse_result=PARSE_FAILED),
            make_line_fact(SOURCE, 4, "bad line four", origin="补记",
                           outcome=LINE_SKIPPED, parse_attempted=True,
                           parse_result=PARSE_FAILED),
        ]
        version, _ = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1", facts=facts)
        return evidence_id, facts, version

    def test_correction_keeps_claimed_task(self):
        pipeline = make_pipeline()
        evidence_id, facts, _ = self._three_line_evidence(pipeline)
        claimed = pipeline.evidence.claim_failed_lines(
            evidence_id=evidence_id, task_id="REWORK-1")
        assert len(claimed) == 2
        assert all(f.rework_task == "REWORK-1" for f in claimed)

        pipeline.evidence.correct(
            evidence_id=evidence_id, target_line_key=facts[0].key,
            expected_parse_result=PARSE_FAILED, new_parse_result=PARSE_OK)
        current = pipeline.evidence.current(evidence_id)
        assert current.lines[facts[0].key].rework_task == "REWORK-1"
        assert current.lines[facts[0].key].parse_result == PARSE_OK
        # 另一条仍是失败，且任务引用不变
        assert current.lines[facts[1].key].parse_result == PARSE_FAILED
        assert current.lines[facts[1].key].rework_task == "REWORK-1"

    def test_rework_confirmation_uses_actual_processed_lines(self):
        pipeline = make_pipeline()
        evidence_id, facts, _ = self._three_line_evidence(pipeline)
        claimed = pipeline.evidence.claim_failed_lines(
            evidence_id=evidence_id, task_id="REWORK-1")
        done_key = claimed[0].key
        result = pipeline.evidence.confirm_rework(
            evidence_id=evidence_id, task_id="REWORK-1",
            processed_keys=[done_key])
        assert result.failure_lines == 1                 # 另一条没有凭空变成功
        assert result.parse_ok == 2                      # 原成功 1 + 补采成功 1
        assert result.total == 4                         # 没有追加新计数行

    def test_restart_after_rework_without_registration(self):
        """补采已成功一行、计量端还没登记结果时重启，再重领失败范围。"""
        pipeline = make_pipeline()
        evidence_id, facts, before = self._three_line_evidence(pipeline)
        claimed = pipeline.evidence.claim_failed_lines(
            evidence_id=evidence_id, task_id="REWORK-1")
        already_done = claimed[0].key

        # 重启：账本状态从持久化快照恢复，补采完成事实由作业侧带回来
        restarted = IngestEvidenceLedger.from_state(pipeline.evidence.dump_state())
        pipeline.evidence = restarted

        recovered = pipeline.evidence.recover_rework(
            evidence_id=evidence_id, task_id="REWORK-1",
            processed_keys=[already_done])
        assert recovered.lines[already_done].parse_result == PARSE_OK
        assert recovered.failure_lines == 1

        # 值班员重领同一份失败范围：已成功的行不会再次出现
        reclaimed = pipeline.evidence.claim_failed_lines(
            evidence_id=evidence_id, task_id="REWORK-2", include_claimed=True)
        keys = {f.key for f in reclaimed}
        assert already_done not in keys
        assert facts[1].key in keys
        # 重领不会把仍失败的原行换到新作业号下重复执行
        assert next(f for f in reclaimed if f.key == facts[1].key).rework_task == "REWORK-1"
        # 不能靠追加成功计数掩盖旧失败：失败行数如实为 1
        assert recovered.failure_lines == 1 and recovered.parse_ok == 2

    def test_recovery_refuses_to_rerun_unprocessed_line(self):
        pipeline = make_pipeline()
        evidence_id, facts, _ = self._three_line_evidence(pipeline)
        pipeline.evidence.claim_failed_lines(
            evidence_id=evidence_id, task_id="REWORK-1")
        try:
            pipeline.evidence.confirm_rework(
                evidence_id=evidence_id, task_id="REWORK-2",
                processed_keys=[facts[0].key])
        except ValueError:
            pass
        else:
            raise AssertionError("不是该作业领取的行不能被另一个作业确认")


class TestConcurrentPublication:
    """生成期间补记与并发确认。"""

    def _draft(self, pipeline, evidence_id):
        return pipeline.evidence.start_report(
            evidence_id=evidence_id, period=PERIOD, batch_id=BATCH,
            source_id=SOURCE, metric_id="INGE-1", fields_coverage=1.0)

    def test_original_evidence_choice(self):
        pipeline = make_pipeline()
        evidence_id, version, *_ = _first_evidence(pipeline)
        draft = self._draft(pipeline, evidence_id)

        pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1",
            facts=[make_line_fact(
                SOURCE, 3, "trailing garbage line", origin="补记",
                outcome=LINE_SKIPPED, parse_attempted=True,
                parse_result=PARSE_FAILED)])
        assert pipeline.evidence.draft_conflict(draft.draft_id)

        report, first = pipeline.evidence.confirm_report(
            draft_id=draft.draft_id, use_current=False)
        assert first
        assert report.evidence_version == version.version
        assert report.completeness_rate == 0.5
        assert report.parse_success_rate == 1.0
        assert report.failure_lines == 0

    def test_current_evidence_choice(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        draft = self._draft(pipeline, evidence_id)
        pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1",
            facts=[make_line_fact(
                SOURCE, 3, "trailing garbage line", origin="补记",
                outcome=LINE_SKIPPED, parse_attempted=True,
                parse_result=PARSE_FAILED)])
        report, _ = pipeline.evidence.confirm_report(
            draft_id=draft.draft_id, use_current=True)
        assert report.completeness_rate == round(1 / 3, 4)
        assert report.parse_success_rate == 0.5
        assert report.failure_lines == 1
        assert pipeline.evidence.report_matches_evidence(report)

    def test_same_period_confirms_once(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        draft = self._draft(pipeline, evidence_id)
        first, ok = pipeline.evidence.confirm_report(draft_id=draft.draft_id)
        assert ok
        second, ok_again = pipeline.evidence.confirm_report(draft_id=draft.draft_id)
        assert ok_again is False
        assert second is first

    def test_receipt_replay_after_generation_keeps_failure_count(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        draft = self._draft(pipeline, evidence_id)
        fact = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)
        pipeline.evidence.amend(evidence_id=evidence_id, receipt_id="RCPT-1", facts=[fact])
        report, _ = pipeline.evidence.confirm_report(
            draft_id=draft.draft_id, use_current=True)

        replay, applied = pipeline.evidence.amend(
            evidence_id=evidence_id, receipt_id="RCPT-1", facts=[fact])
        assert applied is False
        assert replay.failure_lines == report.failure_lines == 1
        assert replay.total == 3

    def test_concurrent_confirm_publishes_once(self):
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        results: list[tuple[int, bool]] = []
        barrier = threading.Barrier(4)

        def worker():
            # 并发生成同一期报告：各自开草稿，都按确认前证据确认
            draft = self._draft(pipeline, evidence_id)
            barrier.wait()
            report, ok = pipeline.evidence.confirm_report(
                draft_id=draft.draft_id, use_current=True)
            results.append((report.evidence_version, ok))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sum(1 for _, ok in results if ok) == 1
        assert len({version for version, _ in results}) == 1
        published = pipeline.evidence.published(evidence_id, PERIOD, BATCH)
        assert published is not None and published.evidence_version == results[0][0]

    def test_concurrent_statistics_submissions_do_not_duplicate(self):
        """同一批计数被多线程同时提交，证据计数不翻倍。"""
        pipeline = make_pipeline()
        evidence_id, *_ = _first_evidence(pipeline)
        barrier = threading.Barrier(4)

        def worker():
            barrier.wait()
            pipeline.evidence.bind_counters(
                evidence_id=evidence_id, processed=1, skipped=1,
                parse_attempted=1, parse_ok=1, note="并发统计提交")

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        current = pipeline.evidence.current(evidence_id)
        assert current.processed == 1 and current.skipped == 1
        assert current.parse_attempted == 1 and current.parse_ok == 1


class TestScenarioWalkthrough:
    """题目给定的完整走查：分母对照 + 生成期间补记 + 回执重传 + 实际证据校验。"""

    def test_end_to_end(self):
        pipeline = make_pipeline()
        source = PRESET_LOG_SOURCES["access-web"]
        rule = PRESET_PARSE_RULES[0]
        raw = [
            '192.168.1.10 - - [15/Nov/2026:14:30:05 +0800] "GET /a HTTP/1.1" 200 1 "-" "x"',
            "2026-11-15 14:30:06.234 orphan line without proper format",
        ]
        entries = pipeline.apply_parse_rules(pipeline.parse_raw_lines(source, raw), rule)
        job = IngestJob(job_id="INGE-1", source_id=SOURCE, batch_size=10,
                        batch_id=BATCH, period=PERIOD)
        processed = pipeline.run_ingest_job(entries, job)
        evidence = pipeline.record_ingest_evidence(
            SOURCE, processed, batch_id=BATCH, period=PERIOD)

        # 1. 三个入口比较分母
        new_metric = pipeline.compute_metrics_from_evidence(job, 1000.0, evidence.evidence_id)
        new_report = pipeline.report_from_evidence(new_metric, evidence.evidence_id, 1.0)
        legacy_job = IngestJob(job_id="INGE-1", source_id=SOURCE, batch_size=10,
                               batch_id=BATCH, period=PERIOD)
        legacy_job.processed_lines, legacy_job.skipped_lines = 1, 1
        legacy_metric = pipeline.compute_ingest_metrics(legacy_job, 1000.0, 2)
        legacy_report = pipeline.generate_quality_report(
            legacy_metric, parse_ok=1, parse_total=1, fields_coverage=1.0,
            processed=1, skipped=1)
        assert new_report.completeness_rate == legacy_report.completeness_rate == 0.5
        assert new_report.parse_success_rate == legacy_report.parse_success_rate == 1.0

        # 2. 生成期间插入补记（试过解析但失败的跳过行）
        draft = pipeline.evidence.start_report(
            evidence_id=evidence.evidence_id, period=PERIOD, batch_id=BATCH,
            source_id=SOURCE, metric_id="INGE-1", fields_coverage=1.0)
        fact = make_line_fact(
            SOURCE, 3, "trailing garbage line", origin="补记",
            outcome=LINE_SKIPPED, parse_attempted=True, parse_result=PARSE_FAILED)
        pipeline.evidence.amend(evidence_id=evidence.evidence_id,
                                receipt_id="RCPT-1", facts=[fact])
        assert pipeline.evidence.draft_conflict(draft.draft_id)

        original, _ = pipeline.evidence.confirm_report(draft_id=draft.draft_id)
        assert (original.completeness_rate, original.parse_success_rate) == (0.5, 1.0)

        # 3. 重送同一补记回执后，同一批再做一次统计提交：仍落在同一证据上
        _, applied = pipeline.evidence.amend(
            evidence_id=evidence.evidence_id, receipt_id="RCPT-1", facts=[fact])
        assert applied is False
        assert pipeline.evidence.current(evidence.evidence_id).version == \
            draft.base_version + 1                     # 重传没有新增版本
        legacy_job.processed_lines, legacy_job.skipped_lines = 1, 2
        resubmitted = pipeline.compute_ingest_metrics(legacy_job, 1000.0, 3)
        assert resubmitted.evidence_version == pipeline.evidence.current(
            evidence.evidence_id).version             # 同批报告同源
        # 已发布报告仍能按自己声明的版本复算原结论
        assert pipeline.evidence.report_matches_evidence(original)
        assert original.evidence_version == draft.base_version

        # 4. 从实际证据重算
        current = pipeline.evidence.current(evidence.evidence_id)
        recomputed = pipeline.evidence.version_report(
            evidence.evidence_id, current.version, source_id=SOURCE,
            period=PERIOD, batch_id=BATCH, metric_id="INGE-1")
        assert recomputed.completeness_rate == round(1 / 3, 4)
        assert recomputed.parse_success_rate == 0.5
        assert recomputed.failure_lines == 1
        assert pipeline.evidence.report_matches_evidence(recomputed)
        assert original.failure_lines == 0


if __name__ == "__main__":
    import traceback

    tests = {
        "legacy_metric_rates": TestLegacyEntriesUnchanged().test_metric_keeps_own_rates,
        "legacy_metric_empty": TestLegacyEntriesUnchanged().test_metric_empty_input_keeps_zero,
        "legacy_report_denominators": TestLegacyEntriesUnchanged().test_report_legacy_denominators,
        "legacy_report_empty": TestLegacyEntriesUnchanged().test_report_zero_lines_keeps_zero,
        "legacy_report_conflict": TestLegacyEntriesUnchanged().test_direct_params_keep_original_denominators_on_conflict,
        "legacy_report_no_erase": TestLegacyEntriesUnchanged().test_direct_params_cannot_erase_known_failure,
        "legacy_repeat_submit": TestLegacyEntriesUnchanged().test_repeated_statistics_submission_does_not_double,
        "same_denominators": TestSameEvidenceDenominators().test_legacy_and_evidence_paths_agree,
        "first_evidence_ratios": TestSameEvidenceDenominators().test_first_evidence_ratios,
        "one_version": TestSameEvidenceDenominators().test_report_ratios_come_from_one_version,
        "same_batch_one_evidence": TestSameEvidenceDenominators().test_old_entries_same_batch_share_one_evidence,
        "other_batch_untouched": TestSameEvidenceDenominators().test_old_entries_other_batch_keeps_evidence_untouched,
        "amend_adds_line": TestAmendReceipts().test_amend_adds_failed_skipped_line,
        "amend_replay": TestAmendReceipts().test_receipt_replay_is_idempotent,
        "amend_other_receipt": TestAmendReceipts().test_different_receipt_same_line_does_not_double_count,
        "correct_replace": TestReviewCorrection().test_correct_replaces_failed_with_success,
        "correct_needs_old": TestReviewCorrection().test_correction_requires_original_conclusion,
        "correct_unknown": TestReviewCorrection().test_unknown_line_is_not_treated_as_new_log,
        "old_report_replay": TestReviewCorrection().test_old_report_still_recomputes,
        "claim_kept": TestReworkJobs().test_correction_keeps_claimed_task,
        "rework_actual": TestReworkJobs().test_rework_confirmation_uses_actual_processed_lines,
        "rework_restart": TestReworkJobs().test_restart_after_rework_without_registration,
        "rework_owner": TestReworkJobs().test_recovery_refuses_to_rerun_unprocessed_line,
        "publish_original": TestConcurrentPublication().test_original_evidence_choice,
        "publish_current": TestConcurrentPublication().test_current_evidence_choice,
        "publish_once": TestConcurrentPublication().test_same_period_confirms_once,
        "publish_receipt_replay": TestConcurrentPublication().test_receipt_replay_after_generation_keeps_failure_count,
        "publish_threads": TestConcurrentPublication().test_concurrent_confirm_publishes_once,
        "publish_thread_submits": TestConcurrentPublication().test_concurrent_statistics_submissions_do_not_duplicate,
        "scenario": TestScenarioWalkthrough().test_end_to_end,
    }

    passed = 0
    failed = 0
    for name, test_func in tests.items():
        try:
            test_func()
            passed += 1
            print(f"  PASS  test_evidence_ledger: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_evidence_ledger: {name}")
            traceback.print_exc()

    print(f"\ntest_evidence_ledger: {passed} passed, {failed} failed, {len(tests)} total")
    sys.exit(1 if failed else 0)
