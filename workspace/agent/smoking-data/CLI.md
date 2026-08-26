# smoking-data CLI

모든 명령은 `smoking-data <command> ...` 형식으로 실행한다. 경로는 현재 작업공간 기준이며,
다른 작업공간을 대상으로 할 때 `--project-root`를 지정한다. LLM이나 자동화에서는 가능한
경우 `--json`을 사용하고, 결과 파일은 `for_agents/output/`에 저장한다.

## 명령어 탐색 계약

```bash
smoking-data --help
smoking-data migrate --help
smoking-data migrate yaml --help
```

첫 번째 명령은 전체 top-level command를, 두 번째 명령은 migration 직속 명령을,
세 번째 명령은 leaf command의 인자와 옵션을 보여준다. 새 CLI 기능을 추가할 때는
dispatch, root/group/leaf help, 이 문서, 생성되는 `.smoking-data/HELP.md`와 테스트를
동시에 갱신해야 한다. 구현 함수가 존재하는 것만으로 public CLI가 된 것으로 보지 않는다.

## 작업공간 초기화

```bash
smoking-data init [TARGET]
smoking-data init [TARGET] --force
```

`init`은 `.vscode`, `templates`, `schedules`, `.smoking-data`, `.agent`, `for_agents`의
초기화 리소스를 생성한다.

`--force`는 기본값이 아니다. 지정하면 init이 관리하는 templates, schedules, Asset config,
HELP.md, agent guidance를 현재 패키지 버전으로 갱신한다. 작업공간의
runtime data, object-store 설정, `AGENTS.md`, `.agent/local/CONTEXT.md`, 사용자 정의
스케줄·스크립트는 삭제하지 않는다.

강제 갱신 전에는 기존 init 관리 생성물 전체를 `.history/YYMMDD_HHMMSS/` 아래에 압축 없이
백업한다. 동일 초에 충돌하면 `_02`, `_03` 접미사를 붙인다. `DATA`와 `.temp` 운영 데이터는
백업하지 않는다.

## 실행·검증

```bash
smoking-data source DEFINITION.yaml [--json]
smoking-data run DEFINITION.yaml [--config CONFIG] [--project-root ROOT] [--trigger-type TYPE] [--json]
smoking-data smoke run DEFINITION.yaml [--tasks N] [--isolated-root ROOT] [--project-root ROOT] [--json]
smoking-data migrate chain verify CHAIN.yaml [--isolated-root ROOT] [--project-root ROOT] [--json]
smoking-data migrate chain run CHAIN.yaml [--migration-dir DIR] [--isolated-root ROOT] [--project-root ROOT] [--json]
smoking-data validate DEFINITION.yaml [--config CONFIG] [--project-root ROOT] [--json]
smoking-data chain validate CHAIN.yaml [--config CONFIG] [--project-root ROOT] [--json]
smoking-data chain run CHAIN.yaml [--config CONFIG] [--project-root ROOT] [--json]
```

- `source`: 0101 Source YAML을 실행한다.
- `run`: Asset Definition 또는 Chain을 파일 확장자에 따라 실행한다.
- `smoke run`: 0101·0201·0301·0401 Definition을 지정 task 수만 실행하고 isolated output에 기록한다. `templates/` 아래 또는 파일명에 `template`이 포함된 Definition은 요청값과 관계없이 1 task만 실행한다.
- `migrate chain verify`: Chain 내부 YAML을 변환하지 않고 각 YAML을 validate한 뒤 task 1개씩 smoke 실행한다.
- `migrate chain run`: 0101을 제외한 0201·0301·0401을 task 1개 smoke 실행하고, 결과 Parquet로 `migration/*.0201.yaml`을 생성한 뒤 0201 migration도 task 1개 smoke 실행한다.
- `validate`: 실행 없이 0101~0401 Asset 또는 Chain 계약을 검증한다. Asset code는 파일명에
  있으면 파일명을 우선하고, `converted.yaml`처럼 파일명에 없으면 현재 YAML의
  `yaml.asset_code`를 사용하므로 migration 결과 파일명은 자유롭게 지정할 수 있다.
- `chain validate`: Asset Chain의 graph와 topological order만 검증한다.
- `chain run`: Asset Chain을 실행한다.
- `--trigger-type`: `manual`, `schedule`, `retry`, `chain` 중 실행 trigger를 기록한다.

## Legacy YAML 변환

Legacy 0101 YAML은 다음 결정론적 명령으로 변환할 수 있다.

```bash
smoking-data migrate yaml LEGACY.yaml --output converted.yaml --json
smoking-data migrate parquet INPUT_PATH --output migration.0201.yaml \
  --source-asset 0301 --job-name parquet_migration --json
```

현재 지원 대상은 구형 `source_0101`의 0101 YAML이다. 지원되지 않는 구조는 추정하지 않고
오류로 중단한다.

1. 기존 YAML과 대상 Asset schema를 함께 읽는다.
2. 의미가 같은 필드만 현재 contract로 매핑하고, 불명확한 필드는 추정하지 않는다.
3. 0101 legacy `source.api_request.spi` 옵션은 `adapter_options`로 옮긴다.
4. 0101 legacy의 사용되지 않는 `type` 필드는 변환 결과에서 제거하고 changes/warnings에 기록한다.
5. 0101 legacy의 `stage`, `stage_id`, `asset` 식별 필드는 새 `yaml.asset_code`로 대체되며 결과에 남기지 않는다.
6. 새 파일을 작성한 뒤 `smoking-data validate NEW.yaml --json`으로 검증한다.
7. 기존·변환 YAML의 job, window, filter, output 경로 차이를 보고한다.

변환 결과는 `smoking-data validate converted.yaml --json`으로 별도 검증한다.

`migrate yaml`은 legacy 0101, 01.04→0201, 02.04→0301, 03.01→0401 및 legacy Chain을
현재 계약으로 변환한다. 자동 매핑할 수 없는 legacy 연산은 결과의 warnings에 남기므로
검증 후 수동 operation 보완이 필요하다.

`migrate parquet`은 파일을 직접 변경하지 않고, 입력 Parquet 경로를 `define_dataset`으로
읽는 0201 migration YAML만 생성한다. `--source-asset`은 lineage 기록용이며 실제 실행은
일반 0201 producer가 담당한다. 완전히 동일한 duplicate row는 기본 selection 정책에 따라
축약될 수 있다.

Parquet 저장소 migration은 별도 파일 조작 CLI를 사용하지 않는다. 기존 Parquet 경로를
`define_upstream.op: define_dataset`으로 지정한 migration 목적의 0201 YAML을 작성한 뒤 다음
순서로 실행한다.

```bash
smoking-data validate migration.0201.yaml --json
smoking-data smoke run migration.0201.yaml --tasks 1 --isolated-root .temp/migration-smoke --json
smoking-data run migration.0201.yaml --json
```

0201 migration YAML의 output root는 기존 dataset과 달라야 하며, smoke 결과는 partial dataset이므로
완전한 migration 결과로 취급하지 않는다.

Chain 결과 기반 migration 예시:

```bash
smoking-data migrate chain run chain.migrated.chain.yaml \
  --migration-dir migration \
  --isolated-root .temp/chain-migration \
  --json
```

생성된 `migration/<asset-id>.0201.yaml`은 자동 삭제하지 않는다. YAML을 검토한 뒤
개별 `validate`, `smoke run`, `run`으로 후속 실행할 수 있다.

필요하면 YAML에 `migration.id`와 `migration.mode`(`pass_through` 또는 `transform`)를
추가해 목적과 원본 Definition 식별자를 남긴다. 이 블록은 일반 0201 DAG 실행의 lineage
marker이며 별도 migration 엔진을 호출하지 않는다.

## 읽기 전용 CLI 진단

다음 명령은 dataset, metadata, registry를 수정하지 않고 읽기만 수행한다.

```bash
smoking-data inspect dataset PATH [--project-root ROOT] [--json]
smoking-data inspect failure PATH [--project-root ROOT] [--json]
smoking-data inspect missing PATH [--project-root ROOT] [--json]
smoking-data inspect profile PATH [--project-root ROOT] [--json]
smoking-data parquet-schema PATH... [--project-root ROOT] [--json]
```

- `inspect dataset`: Parquet footer, dataset manifest, metadata counter를 요약한다.
- `inspect failure`: 실패 metadata의 error와 warning을 수집한다.
- `inspect missing`: missing dependency, route, 입력 삭제 신호를 수집한다.
- `inspect profile`: elapsed, RSS, CPU, I/O profile을 수집한다.
- `parquet-schema`: Parquet footer에서 schema를 읽고 payload row를 스캔하지 않는다.

예시:

```bash
smoking-data inspect failure DATA/0201/job/_smoking_data/metadata.json --project-root . --json \
  > for_agents/output/0201_failure.json
```

## 비교·fixture

```bash
smoking-data compare LEFT RIGHT --report REPORT.json [--label LABEL] [--sample-rows N] \
  [--left-metadata PATH] [--right-metadata PATH] [--fail-on-diff]
smoking-data fixture {0201|0201-pivot|0301|0301-multi-right-full} --root ROOT
```

`compare`는 두 Parquet dataset의 row/schema/sample hash parity report를 작성한다.
`--fail-on-diff`를 지정하면 차이 발견 시 종료 코드 1을 반환한다. `fixture`는 테스트용
입력 fixture를 생성한다.

## PWQ·physical layout

```bash
smoking-data pwq advise PIPELINE.yaml [--metadata PATH] [--config CONFIG] [--project-root ROOT] [--json]
smoking-data pwq benchmark-dummy --root ROOT [--repetitions N] [--max-elapsed-sec SEC] \
  [--max-input-bytes BYTES] [--json]
smoking-data layout report UPSTREAM.yaml DOWNSTREAM.yaml [--history PATH] [--output PATH] \
  [--project-root ROOT] [--json]
smoking-data layout migrate MIGRATION.yaml [--project-root ROOT] [--json]
```

- `pwq advise`: downstream 실행 이력 기반 Parquet writer/physical layout 권고를 생성한다.
- `pwq benchmark-dummy`: 0201 dummy 입력으로 후보를 benchmark한다.
- `layout report`: upstream layout 권고 YAML을 생성한다.
- `layout migrate`: YAML에 정의된 0101 physical layout migration을 실행한다.

## Publication

```bash
smoking-data publication inspect [--project-root ROOT] [--target TARGET] [--dataset-prefix PREFIX] [--json]
smoking-data publication retry RECEIPT.json [--project-root ROOT] [--json]
smoking-data publication gc --project-root ROOT --target TARGET --dataset-prefix PREFIX \
  [--retain-generations N] [--expected-generation-id ID] [--execute] [--json]
smoking-data publication read-key --project-root ROOT --target TARGET --dataset-prefix PREFIX \
  --key-json JSON --output OUTPUT [--key-types-json JSON] [--column COLUMN] [--json]
```

`publication gc`는 기본 dry-run이다. 실제 삭제에는 `--execute`와 대상 generation 확인이
필요하다. AWS credential은 작업공간 파일에 저장하지 않고 운영체제별 AWS shared profile을
사용한다.

## Schedule

```bash
smoking-data schedule validate [SCHEDULE_DIR] [--project-root ROOT] [--json]
smoking-data schedule tick [SCHEDULE_DIR] [--project-root ROOT] [--now ISO_TIMESTAMP] [--json]
```

`schedule validate`는 schedule YAML 계약을 검증한다. `schedule tick`은 due occurrence를
claim하고 대상 작업을 실행한다. 재현 가능한 테스트에서는 `--now`를 사용한다.

## Registry

```bash
smoking-data registry list [--project-root ROOT] [--op OP] [--json]
smoking-data registry record-insert SPEC_KEY [--alias ALIAS] [--project-root ROOT] [--json]
```

`registry list`는 canonical operation catalog를 조회하고, `record-insert`는 authoring 선택을
기록한다. 실행 횟수 통계에는 포함하지 않는다.

## 옵션·오류 처리

- `--json` 출력은 자동화·LLM 소비용 구조화 결과다.
- 성공은 종료 코드 0, 검증·실행·입력 오류는 종료 코드 1이다.
- 오류 JSON에는 가능한 경우 `error_code`, `error_message`, `error_context`가 포함된다.
- CLI가 인식하지 못한 명령은 `smoking-data --help`로 기본 argparse 도움말을 확인한다.
