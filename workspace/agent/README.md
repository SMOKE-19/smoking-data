# LLM용 Smoking Data 지침

이 디렉터리는 `smoking-data init`이 생성하는 LLM 진단 지침이다.

- `smoking-data/`: 패키지가 관리하며 init 재실행 시 최신 내용으로 갱신한다.
- `local/CONTEXT.md`: 사용자 소유 파일이며 init이 덮어쓰지 않는다.
- 루트 `AGENTS.md`: 자동 발견용 진입점이다. 기존 파일이 있으면 init이 본문을 보존하고 Smoking Data 관리 블록을 추가·갱신한다. 자동 연결을 건너뛴 때만 수동으로 이 문서 링크를 추가한다.
- `for_agents/scripts/`: LLM이 작성하는 일회성 Python 탐색 코드 전용이다.
- `for_agents/output/`: 탐색 결과와 분석 보고서 전용이다.

진단 순서는 다음과 같다.

1. `smoking-data --help` 및 필요한 leaf command의 `--help`만 조회한다.
2. `--json` 결과를 먼저 확보한다.
3. 결과의 문제 유형에 해당하는 runbook 하나만 [CLI](smoking-data/CLI.md)의 routing
   표에서 선택해 읽는다.
4. runbook이 지시한 metadata·dataset만 추가로 확인한다.

모든 `.md`를 초기 컨텍스트에 넣지 않는다. CLI 문법은 Markdown보다 실행 파일의
`--help`를 canonical source로 취급한다.
