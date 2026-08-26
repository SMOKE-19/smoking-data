# smoking-data

현재 릴리즈: `0.1.6` (Python 3.10–3.13, Linux/Windows wheel 제공)

Source 0101·CSV Source 0103과 Engine을 하나의 `smoking-data` 배포 패키지로 통합한 Asset 생산 엔진이다. Python 실행기,
Rust/PyO3 Engine kernel, YAML 계약과 Asset Chain은 `src/`에서, 작업공간 원본은 저장소 루트
`workspace/`에서 관리한다. `workspace/templates/`는 사용자용 Definition template 원본이며,
운영 설정이나 회귀 테스트 fixture를 담지 않는다. SBDF 변환은 중복 native 코드를 포함하지 않고 외부
`smoking-sbdf` 패키지에 위임한다.

## 목표 구조

- 하나의 `smoking-data` WHL과 `smoking-data` CLI
- `0101` SQL SPI·HTTP JSON/NDJSON/XML Source Dataset과 자동 Physical Probe, `0102` Calculated Fact,
  `0103` 로컬·HTTP CSV/TSV/ZIP Source,
  `0201` Curated,
  `0301` Join, `0401` Snapshot Asset 계층
- 각 계층에 붙였다 뗄 수 있는 범용 operation
- Engine이 Asset Chain 검증·실행과 dataset-local provenance 계약 소유
- 설치 후 `init`으로 YAML 편집 환경, LLM 진단 지침·sandbox, 0101 adapter와 Asset별 기본 config 생성
- 로컬 atomic commit 뒤 선택적으로 S3 immutable Parquet·SBDF generation bundle과 random-access sidecar 게시

0103 출력 행에는 `file_name`(상대경로)과 함께 소스 파일의 UTC
`source_modified_at`이 기록된다. 파일시스템이 생성 시각을 제공하는 환경에서는
`source_created_at`도 기록하며, Linux/POSIX처럼 생성 시각을 안정적으로 조회할 수
없는 환경에서는 해당 값이 null이다.

## 개발 환경

```bash
uv sync
uv run ruff check .
uv run smoking-data
```

제품의 통합·회귀 테스트는 제품 코드와 분리된 `smoking-data-testkit` 저장소에서 실행한다.

주요 명령은 다음과 같다.

```bash
# YAML schema·snippet, schedules, Source adapter와 Asset별 config 생성
uv run smoking-data init .

# 공통 Asset/Chain 실행·검증 (YAML 종류 자동 판별)
uv run smoking-data validate templates/0201.pipeline_curated_pivot.0201.yaml --json
uv run smoking-data validate templates/chain.0101_to_0401_asset_chain.chain.yaml --json
uv run smoking-data run templates/0101.source.0101.yaml --json
uv run smoking-data run templates/0101.gdelt_doc_articles.0101.yaml --json
uv run smoking-data run templates/0102.calculated_list_facts.0102.yaml --json
uv run smoking-data run templates/0103.csv_unpivot_source.0103.yaml --json
uv run smoking-data validate templates/0101.http_ndjson.0101.yaml --json
uv run smoking-data validate templates/0101.http_xml.0101.yaml --json
uv run smoking-data validate templates/0103.http_zip_unpivot_source.0103.yaml --json
uv run smoking-data validate templates/chain.gdelt_0103_to_0401.chain.yaml --json
uv run smoking-data run templates/0201.pipeline_curated_pivot.0201.yaml --json
uv run smoking-data run templates/chain.0101_to_0401_asset_chain.chain.yaml --json
uv run smoking-data run templates/chain.gdelt_0103_to_0401.chain.yaml --trigger-type chain --json

# dataset·실패·missing dependency·profile 읽기 전용 진단
uv run smoking-data inspect dataset DATA/0103/csv_unpivot_source --project-root . --json
uv run smoking-data inspect failure DATA/0201/curated/_smoking_data/metadata.json --project-root . --json
uv run smoking-data inspect missing DATA/0102/calculated_fact --project-root . --json
uv run smoking-data inspect profile DATA/0201/curated/_smoking_data/metadata.json --project-root . --json

# 기존 명령 호환
uv run smoking-data source templates/0101.source.0101.yaml --json
uv run smoking-data templates/0201.pipeline_curated_pivot.0201.yaml --json
uv run smoking-data chain validate templates/chain.0101_to_0401_asset_chain.chain.yaml --json
uv run smoking-data chain run templates/chain.0101_to_0401_asset_chain.chain.yaml --json

# 후행 실행 이력으로 실행 가능한 물리 레이아웃 권고 YAML 생성
uv run smoking-data layout report 0101.yaml 0201.yaml --json

# migration YAML의 execution.mode는 최초 dry_run으로 검토한다.
uv run smoking-data layout migrate \
  templates/migrations/0101.physical_layout.layout-migration.yaml --json
```

`init .`은 `templates/`와 `schedules/`를 생성한다. `templates/`는 Asset·Chain 계약을 설명하는
Definition template이며, 실제 회귀 테스트 fixture는 별도 testkit에서 관리한다. `--force`로
갱신할 때 기존 init 관리 생성물은 압축하지 않고 `.history/YYMMDD_HHMMSS/` 아래에 함께
백업한다. `DATA`와 `.temp` 운영 데이터는 백업하지 않는다.
GDELT bulk 예제는 작은 `lastupdate.txt`를 우선 조회하고 실제로 게시된 최신 Events ZIP을 선택한다.
최신 슬롯이 아직 미게시 상태면 이전 성공 파일을 재사용하고, 이력이 없는 최초 실행·복구 상황에서만
전체 masterfile을 스트리밍 스캔한다. 선택 파일의 byte 크기와 MD5를 검증한 뒤 헤더 없는 TSV에
61개 컬럼명을 부여하고 0103→0201→0301→0401 체인으로 게시한다. 현재 예제의 범위는 최신 파일 한 개다.

wheel은 다음 명령으로 빌드한다. 저장소 루트 `workspace/`의 원본 리소스는 빌드 과정에서 wheel 내부
`smoking_data/_workspace/`로 함께 복사된다.

```bash
uv build --wheel
```

Asset Definition 파일은 `정렬키.설명.계약종류.yaml` 형식을 사용한다. 예를 들어
`0201.pipeline_curated_pivot.0201.yaml`은 앞의 `0201`로 정렬하고 마지막 `.0201.yaml`로
편집기 스키마를 선택한다. Chain은 `chain.설명.chain.yaml` 형식을 사용한다.
모든 Asset Definition은 최상위 `yaml` 헤더에 `schema_version`과 `asset_code`를 함께 선언한다. Chain은
같은 헤더에 `schema_version`만 선언한다.

`init`은 공통 `.smoking-data/config.yaml`과 지원 Asset별 `config.yaml`을 생성하고,
작업공간 안의 기본 실행 디렉터리인 `DATA`, `.temp`, `.temp/metadata`, `.temp/logs`도 준비한다.
또한 사용자용 `templates/`와 비활성 상태의 `schedules/` 예약 실행 템플릿을 생성한다.
`--force`는 기본값이 아니며, 지정한 경우에만 기존 `.vscode`, `.smoking-data`, `.agent`,
`for_agents`, `schedules`, `AGENTS.md`, `templates/`를 동일한 history snapshot에 보관한다.
LLM 진단을 위해 루트 `AGENTS.md`, 패키지 관리 `.agent/smoking-data/`, 사용자 관리
`.agent/local/CONTEXT.md`도 생성한다. 임시 Python 탐색 코드는 `for_agents/scripts/`, JSON·CSV·보고서는
`for_agents/output/`에만 저장하며 `for_agents/.gitignore`가 sandbox 전체를 Git에서 제외한다.
기존 루트 `AGENTS.md`가 있으면 본문을 보존하고 Smoking Data 관리 블록만 자동 추가·갱신한다.
`.agent` 링크가 이미 있는 문서는 변경하지 않는다. local context는 보존하고 패키지 관리 지침은 init
재실행 시 최신화한다.
각 config는 재귀 경로 `paths`, 런타임 `execution`, 공통 불변 조건을 담는 `contract`를 함께 소유한다.
Asset별 생성물 기본값인 `output`은 불필요한 래퍼 없이 config 최상위에 둔다.
설정은 `번들 공통 < 번들 Asset < 작업공간 공통 < 작업공간 Asset < 개별 Definition`
순서로 재귀 병합한다. 저장소에서 init 리소스를 수정할 때는
저장소 루트의 [`workspace`](workspace/README.md)를 단일 원본으로 사용한다. `vscode`,
`smoking_data`, `templates`, `schedules`가 각각 `.smoking-data`, `templates`,
`schedules`로 대응한다.
자동완성 prefix, 파일명 규칙과 검증·실행 명령은 init이 생성하는 `.smoking-data/HELP.md`에서 바로
확인할 수 있으며, 이미 존재하는 도움말은 덮어쓰지 않는다.

init은 `.smoking-data/object-stores.yaml` 예시도 생성한다. AWS credential은 이 파일에 기록하지
않고 Linux `~/.aws` 또는 Windows `%USERPROFILE%\.aws`의 target별 shared profile을 사용한다. S3
게시 계약과 `publication inspect`·`publication retry`·dry-run 기본 `publication gc` 명령은
`.smoking-data/HELP.md`를 참고한다.

설치된 adapter가 홈 경로의 토큰 파일을 직접 확인하는 API라면 0101 Definition의
`source.api_request.adapter_options.pre_query_script`에 프로젝트 루트 기준 `.py` 경로를 지정할
수 있다. 스크립트는 실행당 한 번, adapter query 전에 별도 Python 프로세스로 실행된다.
Smoking Data 코어는 스크립트의 토큰 값과 출력 스트림을 읽거나 metadata에 기록하지 않으며,
스크립트가 실패하면 데이터 query를 시작하지 않는다.

0101의 SQL 기반 Source adapter 구현과 기본 query 옵션은 별도 설치 패키지인
`smoking-data-spi`가 소유한다. 해당 adapter의 기본 Parquet writer 옵션은 adapter 패키지에서
제공하고, Definition의 `output.artifact.parquet_writer`가 같은 키를 지정하면 Asset 값이
최종 override한다. adapter별 실행 옵션은 Definition의 `source.api_request.adapter_options`에서
관리한다. HTTP 기반 Source는 코어 런타임이 직접 처리한다.

Asset 성공 결과의 `metadata.json`, 원본 `definition.yaml`, 실제 0101 `query.sql`과 실행 계획은 게시된
dataset의 `_smoking_data/`에 저장된다. `_dataset.manifest.json`은 이 파일들의 checksum도 검증한다.
실행 log와 Chain orchestration receipt만 공통 임시 경로에 분리한다. 이 구조는 브레이킹 컷오버이며
구형 외부 metadata 및 SQL template 출력 계약을 읽지 않는다.

## 디렉터리

```text
workspace/            init 원본 리소스와 예시
src/smoking_data/
  assets/
    a0101_source/    Source Dataset 생산
    a0102_calculated_fact/
                       schema-aware Calculated Fact 생산
    a0103_csv_source/  CSV cast·unpivot·route Source Dataset 생산
    a0201_curated/   정리된 partition dataset 생산
    a0301_join/      join dataset 생산
    a0401_snapshot/  분석 snapshot 생산
  ops/               범용 operation
  core/              논리·물리 계약
  migrations/        기존 dataset의 bounded-memory 물리 레이아웃 재작성
  runtime/           실행·transaction·metadata·chain·내부 Parquet probe
                     읽기 전용 dataset/failure/missing/profile inspector
  workspace_init/    init CLI 내부 Python 구현
  workspace_resources.py  개발 원본과 wheel 리소스 탐색
native/              Rust/PyO3 실행 kernel
schemas/             공개 YAML JSON Schema 원본
docs/               공개 저장소에는 포함하지 않는 프로젝트 문서 영역
```

Python 모듈은 숫자로 시작할 수 없으므로 Asset code 앞에 공통 접두사 `a`를 붙인다. 그 뒤에는
`code_description` 순서를 사용한다.

문서와 testkit은 제품 소스 저장소와 분리 관리한다. 공개 소스 배포본에는 실행에 필요한
코드·schema·workspace resource만 포함한다.

## SBDF 백엔드

`smoking_data.backends.streaming_sbdf`는 `smoking-sbdf>=0.1.6,<0.2.0`의 공개
`convert_with_result()` API를 사용한다. 기존 `export_sbdf()`는 출력 `Path`를 그대로 반환하고,
실제 worker·파일별 batch 크기·row/slice 수가 필요하면 `export_sbdf_with_result()`를 사용한다.
외부 패키지를 import하는 것만으로 pandas나 Polars DataFrame이 monkey patch되지 않는다. uv는
`v0.1.6` GitHub Release의 플랫폼별 wheel 목록을 flat source로 사용하며 lockfile에 버전을 고정한다.
소스 checkout이 아닌 `smoking-data` wheel만 직접 설치할 때는 의존성 탐색을 위해 같은 Release
자산 URL을 installer의 `--find-links`로 전달해야 한다. 향후 `smoking-sbdf`가 사용하는 Python
package index에 함께 게시되면 이 추가 옵션은 제거할 수 있다.
