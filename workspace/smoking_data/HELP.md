# Smoking Data 작업공간 도움말

이 문서는 `smoking-data init .`이 생성하는 YAML 작성·자동완성·실행 도움말이다.

## 명령어 탐색 계약

```bash
smoking-data --help
smoking-data migrate --help
smoking-data migrate yaml --help
smoking-data update templates --help
```

`migrate yaml`은 입력 `yaml.schema_version`을 확인한다. `smoking-data.source.v4` 또는
`smoking-data.source.v5`인
YAML이라도 `source.table_id`, `api_request.payload`, 구형 `api_request.date_window`가
남아 있으면 `smoking-data.source.v5`의 `source.api_request.sql` 구조로 정규화하고, `select` 항목의 키 순서도
`name` 다음 `expr` 순서로 정리한다. 변환 결과는 `--output` 경로에 별도로 기록한다.

root help는 전체 top-level command를, group help는 직속 subcommand를, leaf help는
해당 명령의 인자와 옵션을 보여준다. 새 CLI 기능은 dispatch, help, 이 문서와 테스트를
함께 갱신해야 한다.

`smoking-data update templates [TARGET]`은 지정 작업공간의 `templates/`만 설치된
패키지 버전으로 갱신한다. 기존 템플릿은 `.history/YYMMDD_HHMMSS/templates/`에
압축 없이 백업하며 다른 init 생성물은 변경하지 않는다.

## 처음 설정

```bash
smoking-data init .
```

VS Code에서 작업공간 루트를 열고 권장 확장 `redhat.vscode-yaml`을 설치한다. `init`이 만든
`.vscode/settings.json`, JSON Schema와 snippet은 작업공간을 다시 열면 자동으로 적용된다.

## 파일명과 자동완성 스키마

| 파일명 접미사 | YAML 종류 |
| --- | --- |
| `.0101.yaml` | Source Dataset |
| `.0102.yaml` | Calculated Fact Dataset |
| `.0103.yaml` | CSV Source Dataset |
| `.0201.yaml` | Curated Dataset |
| `.0301.yaml` | Join Dataset |
| `.0401.yaml` | Snapshot Dataset |
| `.chain.yaml` | Asset Chain |
| `.schedule.yaml` | Asset/Chain Schedule |

`0101`부터 `0401`까지 모든 Asset Definition은 최상위 `job.name`을 실행·생성물·로그의
논리적 이름으로 사용한다.

Asset Definition 파일은 `정렬키.설명.에셋코드.yaml` 형식을 권장한다.

```text
0101.source.0101.yaml
0201.daily_curated.0201.yaml
chain.daily_assets.chain.yaml
```

파일명의 마지막 Asset 코드와 `yaml.asset_code`는 같아야 한다.

```yaml
yaml:
  schema_version: smoking-data.pipeline.v7
  asset_code: "0201"
```

Chain은 개별 Asset이 아니므로 `yaml.asset_code`를 쓰지 않는다.

## Asset config

공통 `.smoking-data/config.yaml`과 `0101`, `0102`, `0103`, `0201`부터 `0401`까지
`.smoking-data/assets/{asset_code}/config.yaml`을 재귀 병합한다.

```yaml
paths:
  data_root: DATA
  temp_root: .temp
  metadata_root: .temp/metadata
  log_root: .temp/logs
```

모든 상대경로는 작업공간 루트를 기준으로 해석한다. 각 `config.yaml`은 `paths`, `execution`,
`contract`를 함께 관리한다. `paths` 값은 앞서 해석 가능한 다른 path 키를 참조할 수 있다. 실제 게시할
생성물과 Definition 기본값인 `output`은 config 최상위에 바로 둔다.

`smoking-data init .`은 config 생성 후 작업공간 안으로 해석되는 `data_root`, `temp_root`,
`metadata_root`, `log_root` 디렉터리를 함께 만든다. 기본값에서는 `DATA`, `.temp`,
`.temp/metadata`, `.temp/logs`가 생성된다. 작업공간 밖을 가리키는 절대경로는 init이 임의 생성하지
않으며, 실제 실행 전 사용자가 별도로 준비해야 한다.

캐스트 타입과 alias, 동일 타입 cast 생략 규칙은 init이 생성하는
`.smoking-data/CAST_TYPES.md`를 기준으로 확인한다. `INTEGER`는 엔진의 `INT32` alias이며,
64-bit 정수는 `INT64` 또는 `BIGINT`를 사용한다.

init은 비밀 값이 없는 `.smoking-data/object-stores.yaml` 예시도 생성한다. Linux는 `~/.aws`,
Windows는 `%USERPROFILE%\.aws`의 AWS shared profile을 SDK가 읽으며 init은 `.aws`나 credential
파일을 생성하지 않는다. 여러 profile은 object-store target을 분리해 지정한다.

`templates/`에는 Asset·Chain Definition과 0102 expression CSV, migration template이 생성된다.
기본 `init`은 기존 파일을 보존하며, `--force` 갱신 시 기존 init 관리 생성물 전체를
`.history/YYMMDD_HHMMSS/`에 압축 없이 백업한다. `DATA`와 `.temp` 운영 데이터는 백업하지 않는다.
`schedules/`에는 이 Definition을 참조하는 예약 실행 예시가 생성되며 안전을 위해 모두
`enabled: false`이다. 실제 Definition 경로를 확인·수정한 뒤 활성화한다. `init`은 이미 같은 경로의
사용자 파일이 있으면 덮어쓰지 않고 보존한다.

실효 설정은 **번들 공통 config < 번들 Asset config < 작업공간 공통 config < 작업공간 Asset config
< Asset Definition YAML** 순서로 재귀 병합한다. 따라서 Slim Definition은 `output: {}`만
두어 contract를 그대로 사용할 수 있고, 특정 항목만 YAML에 적으면 그 항목만 덮어쓴다. 편집기 JSON
Schema는 이 부분 계약을 허용하지만, 실행 직전에는 병합된 `output`을 완전한 계약으로 엄격하게 검사한다.

공통 config의 `contract`에는 모든 Asset이 공유하는 partition 기준일이 있다.

```yaml
config:
  schema_version: smoking-data.asset-config.v3
  scope: common
contract:
  partition_grid:
    anchor_date: "2021-07-26"
    step_days: 28
```

0101은 atomic publish가 모두 성공하면 source root의 `_smoking_data/physical_probe/`에 `files/`,
`row_groups/`, `pages/` 물리 인덱스를 자동 생성한다. 날짜 window가 경계를 넘으면 갱신 누락을 막기 위해
양쪽 partition에 기록하며, 완성된 generation을 만든 뒤 `latest.json`을 원자 교체한다. 0201은 0101의
physical probe를 재사용하고, 외부 `define_dataset`에 대해서만 공용 캐시에 JIT 생성한다.

```yaml
output:
  artifact:
    type: curated_dataset
    root_dir: DATA/0201/curated
    format: parquet
    compression: zstd
    write_policy: atomic_replace
    physical_layout:
      profile: curated_reuse_v1
      adaptation_scope: generation_fixed
      row_group_rows: auto
  logging:
    root_dir: .temp/logs/0201
```

`row_group_rows`는 `auto` 또는 1 이상의 정수다. 이전 공개 필드인
`execution.output_row_group_rows`는 제거되었다.

로컬 commit 뒤 S3 immutable generation bundle을 게시하려면 `output.artifact`에 다음 opt-in 계약을
추가한다. access key·secret key·session token은 YAML에 넣지 않는다.

```yaml
publication:
  enabled: true
  target: analytics_s3
  dataset_prefix: assets/0201/curated
  mode: mirror_after_local_commit
  failure_policy: required
  parquet:
    enabled: true
    random_access_index:
      level: row_group
```

게시 상태는 로컬 receipt 또는 고정된 remote generation으로 조회한다.

```bash
smoking-data publication inspect --project-root . --json
smoking-data publication inspect --project-root . --target analytics_s3 --dataset-prefix assets/0201/curated --json
smoking-data publication retry .smoking-data/registry/publications/<receipt>.json --project-root . --json
smoking-data publication gc --project-root . --target analytics_s3 --dataset-prefix assets/0201/curated --retain-generations 3 --json
smoking-data publication read-key --project-root . --target analytics_s3 --dataset-prefix assets/0201/curated --key-json '{"row_key":"A"}' --output selected.arrow --json
```

`publication gc`는 기본적으로 삭제하지 않고 후보 generation과 byte 수만 반환한다. 실제 삭제는 같은
명령에 `--execute --expected-generation-id <현재 generation>`을 함께 지정해야 한다. 계획 이후 pointer가
바뀌면 삭제를 중단한다.

`publication read-key`는 게시 당시 선언한 `key_columns`와 같은 key를 받아 manifest의 Arrow type 계약으로 해당 hash
bucket sidecar만 읽는다. hash가 같아도 원래 key 값을 다시 비교하며, 중복 key는 모두 Arrow IPC 파일에
기록한다. `--column`을 반복 지정하면 projection을 제한할 수 있다. 과거 manifest처럼 type 계약이 없는
경우에만 `--key-types-json '{"row_key":"string"}'`을 추가한다.

현재 remote Parquet 공개 API는 Rust S3 reader로 row-group·projection을 IPC 파일로 materialize할 수
있지만 Pipeline upstream 직접 연결은 아직 제공하지 않는다. SBDF mirror 게시와 key sidecar는 지원하며
remote SBDF slice decode capability는 아직 `false`다.

0201과 0301의 `physical_layout`은 `generation_fixed`로 고정한다. planner는 generation 시작 전에
row-group 크기를 한 번 선택하고 모든 partition/task에 같은 값을 적용한다. 동일 profile의 후속 실행은
이전 generation 값을 재사용하며, profile을 변경하면 전체 dataset을 새 generation으로 atomic
cutover한다. 0401만 `task_adaptive`를 허용하며 기본 profile은
`analysis_snapshot_adaptive_v1`이다. worker 수, input batch, prefetch와 메모리 admission은 이 물리
레이아웃 제약과 별개로 실행마다 조정할 수 있다.

0201~0401의 마지막 `save_dataset`은 데이터 흐름과 partition만 선언한다. 게시 경로·압축·쓰기
정책은 중복하지 않고 루트 `output.artifact`만 진실 원천으로 사용한다.
성공 metadata와 definition·plan은 게시된 dataset의 `_smoking_data/` 아래에 함께 저장한다.
최종 dataset은 기본적으로 Zstd를 사용하고, `.temp` 중간 산출물과 probe·selector·index sidecar는
쓰기 지연을 줄이기 위해 비압축 Parquet으로 기록한다.

0101·0201·0301 Definition은 작성 초기 검증을 위해 다음 테스트 실행 계약을 사용할 수 있다.

```yaml
execution:
  test_run:
    final_task_limit: 1
```

0101은 전체 window를 계획한 뒤 첫 source task만 실행한다. 0201·0301은 probe, candidate,
active-row-selection, left/right index와 physical plan을 전체 입력 기준으로 완성한 뒤 최종
materialize/join task만 제한한다. 테스트 실행은 변경 감지 skip과 기존 part 재사용을 우회한다.
게시 결과는 부분 dataset이며 같은 `root_dir`의 기존 전체 dataset을 대체하므로 테스트 전용 출력
경로를 사용하는 것이 안전하다. 실행 metadata의 `details.test_run`에서 전체 계획 task 수와 선택된
task ID를 확인할 수 있다.

## VS Code 자동완성 사용법

YAML 파일에서 prefix를 입력하고 `Ctrl+Space`로 후보를 연 뒤 `Enter` 또는 `Tab`으로 삽입한다.
snippet을 삽입한 뒤 `Tab`을 누르면 다음 편집 위치로 이동한다.

### 기본 골격

| prefix | 생성 내용 |
| --- | --- |
| `sd-source` | 0101 Source 기본 골격 |
| `sd-csv-source` | 0103 CSV Source 기본 골격 |
| `sd-pipeline` | 0201~0401 Pipeline 기본 골격 |
| `sd-chain` | Asset Chain 기본 골격 |
| `sd-schedule` | 예약 실행 기본 골격 |

Schedule의 `targets`는 하나 이상의 Asset/Chain Definition을 목록 순서대로 실행한다. 실행은 프로젝트 전체에서
항상 순차 처리되며, 앞 target이 실패하면 뒤 target은 실행하지 않고 `blocked`로 기록한다. 병렬 실행 여부는
YAML 옵션으로 노출하지 않는다.

### Pipeline 입력과 0201 phase

| prefix | 생성 내용 |
| --- | --- |
| `sd-op-define-asset` | 다른 Asset Definition의 게시 dataset을 관리형 upstream으로 연결 |
| `sd-op-define-dataset` | Asset 밖에서 관리되는 Parquet 경로를 외부 upstream으로 연결 |
| `sd-op-define-source-choice` | 두 입력 방식 중 하나를 주석 해제해 선택하는 골격 |
| `sd-execution-test-run` | 전역 sidecar·plan 생성 후 최종 task만 제한하는 테스트 실행 골격 |

`define_asset.definition`의 상대경로는 현재 Pipeline Definition 파일을 기준으로 해석한다.
`define_dataset.paths`는 기존처럼 물리 dataset 경로를 직접 지정한다. Full template에는 동일 alias의
두 입력 방식이 함께 들어가며, 하나만 활성화하고 다른 블록은 주석 상태로 유지한다.
모든 operation은 `id` 대신 사람이 편집할 수 있는 `alias`를 선언하고 `inputs`에서도 alias를 참조한다.
검증 시 생성되는 캐노니컬 key는 alias 변경과 무관하다.

0201의 `build_sidecar.operations[].group_keys`는 논리적인 active-row 선택 그룹을 선언한다.
`materialize.part_boundary.preserve_groups`는 물리적인 payload 경계 힌트이므로 두 필드가
항상 같을 필요가 없으며, 선택 그룹의 일부만 지정할 수 있다. 좌표로 선택된 행은 sidecar
결과를 기준으로 유지된다.

Pivot의 `value_keys`와 `value_keys_without_column`에서 `output_dtype`는 선택 사항이다.
생략하면 명시적 cast를 수행하지 않고 집계 결과의 Arrow 타입을 사용하며, `first`, `min`,
`max` 집계는 원천 칼럼 타입을 그대로 상속한다. 타입을 강제로 바꿔야 하는 경우에만
`output_dtype`를 지정한다.

0201은 `smoking-data.pipeline.v7` phase 계약만 허용한다. 기존 v6 `operations` 배열은 호환하지 않는다.
`define_upstream`은 입력을 정의하고, `build_sidecar.operations`는 전역 후보와 active 좌표를 만든다.
후보 프로세스는 기본적으로 파일 수와 selector 투영 크기를 함께 제한하는 adaptive recycle을
사용한다. 고급 조정이 필요한 경우에만 `build_sidecar.execution.worker_recycle`의
`max_source_files`와 `max_projected_bytes_mb`를 지정한다. 이 물리 설정은 논리 operation
해시와 후보 결과 fingerprint에는 포함되지 않는다.
`materialize.operations`는 좌표 단위 자식 프로세스에서 선택 읽기·payload 변환·staging Parquet 쓰기로
융합되며, `save_dataset`은 부모 프로세스의 assertion과 atomic commit을 담당한다. 0301·0401은
`smoking-data.pipeline.v6`의 평면 operation DAG를 유지한다.

### 예시 template

`sd-template-{asset-code}-{description}`은 Asset config가 제공하는 `output`과 실행 기본값을 생략한
Slim 예시다. 같은
prefix 끝에 `-full`을 붙이면 모든 선택 필드가 포함된 전체 예시를 삽입한다.

| Slim prefix | Full prefix | 생성 내용 |
| --- | --- | --- |
| `sd-template-0101-source` | `sd-template-0101-source-full` | 0101 Source |
| `sd-template-0102-calculated-list-facts` | `sd-template-0102-calculated-list-facts-full` | 0102 Calculated Fact |
| `sd-template-0201-pipeline-curated-pivot` | `sd-template-0201-pipeline-curated-pivot-full` | 0201 Curated Pivot |
| `sd-template-0201-pipeline-pivot-parity` | `sd-template-0201-pipeline-pivot-parity-full` | 0201 Pivot parity |
| `sd-template-0301-pipeline-basic` | `sd-template-0301-pipeline-basic-full` | 0301 기본 Join |
| `sd-template-0301-pipeline-multi-right-full-parity` | `sd-template-0301-pipeline-multi-right-full-parity-full` | 0301 Multi-right full parity |
| `sd-template-0301-pipeline-multi-right-join` | `sd-template-0301-pipeline-multi-right-join-full` | 0301 Multi-right Join |
| `sd-template-0301-pipeline-multi-right-post-ops` | `sd-template-0301-pipeline-multi-right-post-ops-full` | 0301 Join 후속 operation |
| `sd-template-0401-pipeline-analysis-snapshot` | `sd-template-0401-pipeline-analysis-snapshot-full` | 0401 Snapshot |
| `sd-template-chain-0101-to-0401-asset-chain` | `sd-template-chain-0101-to-0401-asset-chain-full` | 0101~0401 Chain |

## 검증과 실행

```bash
# Asset 또는 Chain 계약 검증 (YAML을 자동 판별)
smoking-data validate templates/0201.pipeline_curated_pivot.0201.yaml --project-root . --json
smoking-data validate templates/chain.0101_to_0401_asset_chain.chain.yaml --project-root . --json

# Asset 또는 Chain 실행 (YAML을 자동 판별)
smoking-data run templates/0101.source.0101.yaml --project-root . --json
smoking-data run templates/0102.calculated_list_facts.0102.yaml --project-root . --json
smoking-data run templates/0103.csv_unpivot_source.0103.yaml --project-root . --json
smoking-data run templates/0201.pipeline_curated_pivot.0201.yaml --project-root . --json
smoking-data run templates/chain.0101_to_0401_asset_chain.chain.yaml --project-root . --json

# 기존 명령도 호환된다.
smoking-data source templates/0101.source.0101.yaml --json
smoking-data templates/0201.pipeline_curated_pivot.0201.yaml --project-root . --json
smoking-data chain validate templates/chain.0101_to_0401_asset_chain.chain.yaml --project-root . --json
smoking-data chain run templates/chain.0101_to_0401_asset_chain.chain.yaml --project-root . --json

# template smoke 실행은 항상 1 task로 제한된다.
smoking-data smoke run templates/0201.pipeline_curated_pivot.0201.yaml --tasks 9 --json

# 재사용 가능한 filter operation과 자동완성 선택 기록
smoking-data registry list --op filter --project-root . --json
smoking-data registry record-insert filter_<hash> --alias valid_rows --project-root . --json

# 예약 계약 검증과 Windows Task Scheduler가 주기적으로 호출할 tick
smoking-data schedule validate --project-root . --json
smoking-data schedule tick --project-root . --json
```

Windows Task Scheduler에는 개별 Asset 대신 `schedule tick` 명령 하나를 1분 간격으로 등록한다.
인자를 생략하면 공통 config의 `paths.schedule_root`(기본값 `schedules`)를 사용한다.
예약 YAML은 실행 시각과 정책을 소유하고, 중복 claim·실행 결과·마지막 확인 시각은 Git에서 제외되는
`.smoking-data/scheduler/state.sqlite`에 기록한다. `catch_up_once`는 기록된 마지막 tick 이후 놓친 실행 중
가장 최근 한 건만 수행하며, 최초 tick에서는 현재 분이 cron과 일치할 때만 실행한다. 여러 target의 실행
순서와 결과도 같은 상태 DB에 남고 전용 SQLite lock으로 중복 러너와 스케줄 간 동시 실행을 막는다.

## LLM 읽기 전용 진단과 sandbox

`init`은 루트 `AGENTS.md`와 `.agent/`에 LLM용 CLI·metadata·분석 지침을 생성한다. 기존
`AGENTS.md`가 있으면 본문을 보존하고 Smoking Data 관리 블록만 추가·갱신한다. 이미 `.agent`
링크가 있는 문서는 변경하지 않는다. `.agent/local/CONTEXT.md`는 보존하고
`.agent/smoking-data/`의 패키지 관리 지침은 최신 버전으로 갱신한다.

```bash
smoking-data inspect dataset DATA/0103/job --project-root . --json
smoking-data inspect failure DATA/0201/job --project-root . --json
smoking-data inspect missing DATA/0102/job --project-root . --json
smoking-data inspect profile DATA/0201/job/_smoking_data/metadata.json --project-root . --json
```

네 명령은 JSON과 Parquet footer만 읽고 dataset·metadata·registry를 수정하지 않는다. LLM이 별도
Python 탐색을 작성해야 하면 `for_agents/scripts/`만 사용하고 결과는 `for_agents/output/`에만 저장한다.
`for_agents/.gitignore`는 sandbox 전체를 Git에서 제외한다.

## 자동완성이 보이지 않을 때

1. 파일명이 지원 접미사로 끝나는지 확인한다.
2. VS Code에서 작업공간 루트를 열었는지 확인한다.
3. `redhat.vscode-yaml` 확장이 활성화됐는지 확인한다.
4. `smoking-data init .`을 다시 실행하고 VS Code에서 `Developer: Reload Window`를 실행한다.

`init`은 이 도움말과 사용자 소유 Asset config가 이미 있으면 덮어쓰지 않는다. SQL Source adapter
설정과 private SPI 호출 계약은 별도 설치 패키지에서 관리하며 작업공간에 생성하지 않는다.
