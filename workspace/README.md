# 작업공간 원본

이 디렉터리는 `smoking-data init <작업공간>`이 사용하는 제품 원본이다. 초기화 결과를 수정하려면
생성된 임시 작업공간이 아니라 여기의 파일을 편집한다. Python 초기화 구현은
`src/smoking_data/workspace_init/`에 있으며, 이 디렉터리에는 사용자가 관리할 리소스만 둔다.

개발 환경의 `init`은 이 디렉터리를 직접 읽는다. wheel 빌드는 같은 내용을
`smoking_data/_workspace/` 리소스로 포함하며, 설치된 CLI는 해당 복사본을 읽는다.

| 저장소 원본 | init 생성 위치 | 역할 |
| --- | --- | --- |
| `vscode/` | `.vscode/` | YAML Schema, snippet, task, 편집기 설정 |
| `smoking_data/` | `.smoking-data/` | 공통·Asset config, adapter, object-store target, 도움말 |
| `examples/` | `examples/` | 사용자용 reference Definition, migration, 최소 입력 리소스 |
| `schedules/` | `schedules/` | 비활성 예약 실행 예시 |
| `AGENTS.md` | `AGENTS.md` | LLM 자동 발견용 읽기 전용 진단 진입점 |
| `agent/` | `.agent/` | CLI·metadata·profile·실패·missing 분석 지침 |

`smoking_data/gitignore`는 init 시 `.smoking-data/.gitignore` 내용으로 사용한다. 숨김 파일을 저장소
원본으로 직접 두지 않기 위한 이름 변환이다.

`smoking_data/object-stores.yaml`은 bucket·region·AWS profile 이름만 포함하는 비밀 없는 예시다.
실제 AWS credential 파일은 작업공간 리소스에 포함하지 않는다.

0101 SPI 인증 준비가 필요한 경우 Definition YAML의 `source.api_request.spi`에 프로젝트 루트
기준 `pre_query_script`를 지정한다. 해당 `.py`는 실행당 한 번 query 전에 별도 프로세스로
실행되며, 토큰 파일은 외부 SPI가 직접 관리한다.

```yaml
source:
  api_request:
    spi:
      pre_query_script: auth/generate_token.py
      execution: once_per_run
      timeout_sec: 60
      lock_timeout_sec: 60
```

기존 사용자 파일은 보존한다. VS Code 설정과 task는 관리 항목만 병합하고, 나머지 템플릿 파일은
대상 경로가 없을 때만 생성한다. 전체 init 결과의 회귀 스냅샷은 제품 저장소가 아닌 별도
`smoking-data-testkit` 저장소에서 관리한다.

Agent 지침은 예외적으로 `.agent/smoking-data/`의 패키지 관리 파일을 재실행 때 갱신한다. 루트
`AGENTS.md`가 이미 있으면 기존 본문을 보존하고 `smoking-data:agent-guidance` 관리 블록만 추가·갱신한다.
`.agent` 링크가 이미 있는 문서는 그대로 두며 `.agent/local/CONTEXT.md`는 사용자 파일로 보존한다. `for_agents/scripts/`와
`for_agents/output/`은 Python 초기화 코드가 빈 디렉터리로 만들며 workspace 원본에 두지 않는다.

`examples/`는 운영 설정과 회귀 테스트 fixture를 혼합하는 폴더가 아니다. 제품 계약을 설명하는
reference Definition과 이를 실행하기 위한 최소 `resources/`만 포함하며, 통합 회귀 테스트 fixture와
benchmark 산출물은 별도 testkit·benchmark 영역에서 관리한다.
