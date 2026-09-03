# smoking-data

`smoking-data`는 고카디널리티 데이터셋을 제한된 메모리에서 수집·변환·선별·조인·게시하는
Asset 지향 데이터 처리 엔진이다. Python 실행 계층과 Rust/PyO3 커널을 하나의 패키지로 제공하며,
YAML Definition을 검증하고 실행 계획과 재현 가능한 데이터셋 산출물을 생성한다.

현재 엔진/API 버전은 `0.1.18`이며 Python 3.10 이상 3.14 미만을 지원한다.

## 주요 기능

- Parquet footer와 sidecar를 이용한 파일·row group·row coordinate 선택 읽기
- 제한된 메모리 예산을 반영하는 task 분할과 bounded execution
- Parquet 및 SBDF 데이터셋 생성과 atomic commit
- 로컬 파일시스템과 S3 호환 object storage의 선택적 동기화·게시
- 실행 Definition, manifest, checksum, 실패 원인과 profile metadata 기록
- Asset Chain의 의존성 검증, 위상 정렬과 순차 실행
- YAML 마이그레이션, 제한 task smoke run, 데이터셋 읽기 전용 진단
- 설치된 엔진의 Asset·operation·expression capability를 반환하는 API와 CLI

## Asset 계층

| Asset | 역할 | 기본 산출물 |
| --- | --- | --- |
| `0101` | HTTP JSON·NDJSON·XML 및 등록된 Source adapter를 통한 원천 데이터 수집 | Parquet Source Dataset |
| `0102` | upstream 칼럼을 이용한 schema-aware 계산 칼럼 생성 | Parquet Calculated Fact |
| `0103` | 로컬·HTTP CSV/TSV/ZIP cast, unpivot, route 처리 | Parquet Source Dataset |
| `0201` | row selection, cast, list restore, pivot 등 정제 pipeline | Parquet Curated Dataset |
| `0301` | 여러 upstream의 keyspace 구성과 coordinate 기반 join | Parquet Join Dataset |
| `0401` | filter, unnest/explode와 단일 분석 snapshot 생성 | SBDF Snapshot |

0103은 각 출력 행에 원본 상대경로인 `file_name`과 UTC `source_modified_at`을 기록한다.
파일시스템이 생성 시각을 제공하면 `source_created_at`도 기록하며, 안정적으로 조회할 수 없는
환경에서는 null을 사용한다.

## 실행 모델

Definition은 최상위 `yaml.schema_version`과 Asset의 `yaml.asset_code`를 선언한다. 엔진은 실행 전에
계약을 검증하고 논리 graph를 구성한 뒤 physical task로 낮춘다. `build_sidecar` 단계는 Parquet
metadata와 필요한 coordinate를 수집하고, 후속 materialize 단계는 계획에 포함된 파일과 행만 읽는다.

실행 성공 시 데이터셋에는 다음 추적 정보를 함께 기록한다.

- 실행 Definition 원본과 SHA-256
- dataset manifest와 artifact descriptor
- 입력·출력 파일 및 sidecar 관계
- 실행 계획, row count와 schema 정보
- 실패·skip·missing dependency를 포함한 구조화 metadata

0101부터 0301까지는 Parquet를 기본 산출물로 사용하고, 0401은 SBDF snapshot을 기본으로 사용한다.
출력은 임시 경로에 완성한 뒤 최종 경로로 교체하여 불완전한 dataset 노출을 방지한다.

## 설치

릴리스 wheel을 받은 경우 해당 파일을 직접 설치한다.

```bash
python -m pip install ./smoking_data-0.1.18-cp313-cp313-linux_x86_64.whl \
  --find-links https://github.com/SMOKE-19/smoking-sbdf/releases/expanded_assets/v0.1.6
```

소스에서 개발 환경을 구성하려면 Rust toolchain과 `uv`가 필요하다.

```bash
uv sync
uv run smoking-data --help
```

## CLI

가장 일반적인 진입점은 `validate`, `run`, `capabilities`다.

```bash
# 실행 없이 Definition 계약 검증
smoking-data validate definitions/job.0201.yaml --json

# Asset 또는 Chain 실행
smoking-data run definitions/job.0201.yaml --project-root . --json

# 설치된 엔진 capability 확인
smoking-data capabilities --json
```

`run`과 `chain run`의 일반 텍스트 출력은 실행 환경에 맞춰 자동 전환된다. 대화형 터미널에서는
현재 phase와 추정 진행률, 처리 중인 task·PID·RSS를 한 화면에서 갱신한다. 로그 리다이렉션·파이프
환경에서는 같은 정보를 줄 단위 이벤트로 남기며, `--json`에서는 기계 판독 결과를 오염시키지 않도록
진행 출력을 끄고 최종 JSON만 출력한다. 전체 진행률은 phase별 완료율을 합산한 추정치다.

에셋별 진행 단위는 실제 처리 경계에 맞춘다. 0101은 API task, 0102는 coordinate 계산 task,
0103은 입력 파일, 0201은 candidate 파일·bucketize row group·materialize task, 0301은 join task를
기준으로 표시한다. 0401은 하향된 0201 또는 0301 실행 경로의 같은 phase 계약을 사용한다.

주요 보조 명령은 다음과 같다.

```text
inspect       dataset, Parquet payload preview, failure, missing dependency, profile 조회
migrate       기존 Definition·Parquet 입력·Chain 마이그레이션
smoke         일부 task만 실행하는 bounded smoke test
layout        물리 레이아웃 분석과 마이그레이션
publication   object storage 게시 상태 조회·재시도·정리
chain         Asset Chain 검증과 실행
parquet-schema
              Parquet footer schema 조회
pwq           pipeline write quality 권고
```

wide Parquet의 실제 값을 터미널에서 확인할 때는 파일 또는 dataset 디렉터리를 전달한다. 디렉터리는
재귀 탐색한 파일을 정렬한 뒤 첫 번째 Parquet의 첫 10행을 읽으며, 모든 칼럼을 터미널 폭에 맞는
블록으로 나눠 출력한다.

```bash
smoking-data inspect DATA/0201/job \
  --repeat-columns bucket,text_ro,int_wa
```

각 명령의 세부 계약은 CLI help에서 확인한다.

```bash
smoking-data migrate --help
smoking-data publication --help
smoking-data inspect --help
```

## Python API

검증 API는 pipeline을 실행하거나 registry 상태를 변경하지 않고 구조화된 결과를 반환한다.

```python
from smoking_data import get_capabilities, validate_definition

result = validate_definition(
    "definitions/job.0201.yaml",
    project_root=".",
)

if not result.ok:
    print(result.error_code, result.error_message)

capabilities = get_capabilities()
print(capabilities["asset_schemas"])
```

`ValidationResult`에는 Definition 종류, schema version, Asset code, job name, YAML·graph SHA-256과
구조화된 오류 정보가 포함된다. `get_capabilities()`는 파일을 읽거나 pipeline을 실행하지 않는
독립 introspection API다.

## Object storage와 random access

publication 계약을 설정하면 dataset artifact와 sidecar, manifest를 S3 호환 저장소에 immutable
generation으로 게시할 수 있다. 후속 실행은 remote manifest와 sidecar를 먼저 읽어 필요한 object
range만 내려받으며, 전체 Parquet 동기화를 기본 전제로 삼지 않는다.

AWS credential은 Definition에 저장하지 않는다. 런타임은 운영체제의 AWS shared credentials와
명시된 profile을 사용한다. 게시 상태는 다음 명령으로 확인한다.

```bash
smoking-data publication inspect --help
smoking-data publication retry --help
smoking-data publication gc --help
```

## 코드 구조

```text
src/smoking_data/
  _config/             엔진 기본 설정
  assets/              0101~0401 Asset 실행 계층
  ops/                 재사용 가능한 데이터 operation
  planners/            sidecar·task·memory 계획
  core/                pipeline, graph, result와 engine 계약
  runtime/             실행, metadata, transaction, publication
  migrations/          Definition·물리 레이아웃 마이그레이션
src/smoking_data_engine_rs/
                       Rust/PyO3 커널 Python binding
native/engine/         Arrow·Parquet 기반 native 실행 커널
schemas/               공개 YAML JSON Schema
```

Python 모듈명은 숫자로 시작할 수 없으므로 Asset 구현 패키지는 `a0101_source`처럼 `a` 접두사를
사용한다.

## 개발 및 빌드

```bash
uv run ruff check .
uv run pytest -q
uv build --wheel
```

wheel에는 엔진 실행 코드, native extension과 최소 기본 config만 포함한다. 빌드 후에는
wheel metadata의 버전, `smoking-data` entry point, native module import와 금지된 개발 산출물의
미포함 여부를 확인한다.
