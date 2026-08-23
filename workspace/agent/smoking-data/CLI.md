# 읽기 전용 CLI

다음 명령은 대상 dataset과 metadata를 수정하지 않고 JSON·Parquet footer만 읽는다. LLM은 항상
`--project-root . --json`을 사용하고, 결과를 보존해야 할 때만 `for_agents/output/`으로 redirect한다.

```bash
smoking-data inspect dataset DATA/0103/job --project-root . --json
smoking-data inspect failure DATA/0201/job/_smoking_data/metadata.json --project-root . --json
smoking-data inspect missing DATA/0102/job --project-root . --json
smoking-data inspect profile DATA/0201/job/_smoking_data/metadata.json --project-root . --json
```

결과 저장 예시:

```bash
smoking-data inspect failure DATA/0201/job --project-root . --json \
  > for_agents/output/0201_failure.json
```

`inspect dataset`은 Parquet file 수·크기·footer row 수, manifest generation, metadata counter,
0103 catalog/source-file 상태와 0102 calculation state를 요약한다. footer가 1,000개를 넘으면 앞
1,000개만 읽고 `footer_scan_complete: false`를 반환한다.

`inspect failure`는 `error_code`, 실패 상태와 warning을, `inspect missing`은 missing dependency,
unmatched route와 삭제 입력 신호를, `inspect profile`은 elapsed·RSS·CPU·I/O metric을 JSON path와 함께 모은다.

이 명령은 원인을 단정하지 않는다. 반환된 `document`와 `json_path`를 근거로 원본 metadata를 다시 확인한다.

다음 명령은 읽기 전용 진단이 아니다. 사용자의 명시적 실행 요청 없이는 호출하지 않는다.

- `smoking-data run`
- `smoking-data chain run`
- `smoking-data schedule tick`
- `smoking-data layout migrate`
- `smoking-data pwq benchmark-dummy`
