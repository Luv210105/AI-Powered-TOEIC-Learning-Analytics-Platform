"""
emit_assertion.py — emit data contract (assertions) vào DataHub cho bảng Gold chính.

Assertions (= data contract cam kết chất lượng), khớp với validate_gold.py:
  - dim_user.user_sk       : UNIQUE (surrogate key SCD2 phải duy nhất)
  - fact_question_attempt.user_sk : NOT NULL (referential/completeness)
Kèm run event kết quả SUCCESS để hiện trong tab Quality của dataset.

Chạy bằng python của tool datahub:
  ~/.local/share/uv/tools/acryl-datahub/bin/python governance/datahub/emit_assertion.py
"""

import time

import datahub.metadata.schema_classes as models
from datahub.emitter.mce_builder import make_assertion_urn, make_dataset_urn, make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter

GMS = "http://localhost:8085"
emitter = DatahubRestEmitter(GMS)


def emit_assertion(assertion_id, dataset_name, field, operator, aggregation, native_type):
    """Tạo 1 assertion (data contract) + run event PASS gắn vào dataset."""
    dataset_urn = make_dataset_urn("postgres", dataset_name, "PROD")
    assertion_urn = make_assertion_urn(assertion_id)

    info = models.AssertionInfoClass(
        type=models.AssertionTypeClass.DATASET,
        datasetAssertion=models.DatasetAssertionInfoClass(
            dataset=dataset_urn,
            scope=models.DatasetAssertionScopeClass.DATASET_COLUMN,
            fields=[make_schema_field_urn(dataset_urn, field)],
            operator=operator,
            aggregation=aggregation,
            nativeType=native_type,
        ),
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info))

    run = models.AssertionRunEventClass(
        timestampMillis=int(time.time() * 1000),
        assertionUrn=assertion_urn,
        asserteeUrn=dataset_urn,
        runId=f"run-{int(time.time())}",
        status=models.AssertionRunStatusClass.COMPLETE,
        result=models.AssertionResultClass(type=models.AssertionResultTypeClass.SUCCESS),
    )
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run))
    print(f"  emitted: {assertion_id} -> {dataset_name}.{field}")


def main():
    """Emit 2 assertion (data contract) cho dim_user + fact_question_attempt."""
    emit_assertion(
        "dim_user_user_sk_unique", "toeic.public.dim_user", "user_sk",
        models.AssertionStdOperatorClass._NATIVE_, models.AssertionStdAggregationClass.UNIQUE_PROPOTION,
        "unique",
    )
    emit_assertion(
        "fact_qa_user_sk_not_null", "toeic.public.fact_question_attempt", "user_sk",
        models.AssertionStdOperatorClass.NOT_NULL, models.AssertionStdAggregationClass.IDENTITY,
        "not_null",
    )
    print(">>> DATA CONTRACT OK: 2 assertion (PASS) emitted -> DataHub.")


if __name__ == "__main__":
    main()
