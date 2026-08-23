# LLM용 Smoking Data 지침

이 디렉터리는 `smoking-data init`이 생성하는 LLM 진단 지침이다.

- `smoking-data/`: 패키지가 관리하며 init 재실행 시 최신 내용으로 갱신한다.
- `local/CONTEXT.md`: 사용자 소유 파일이며 init이 덮어쓰지 않는다.
- 루트 `AGENTS.md`: 자동 발견용 진입점이다. 기존 파일이 있으면 init이 본문을 보존하고 Smoking Data 관리 블록을 추가·갱신한다. 자동 연결을 건너뛴 때만 수동으로 이 문서 링크를 추가한다.
- `for_agents/scripts/`: LLM이 작성하는 일회성 Python 탐색 코드 전용이다.
- `for_agents/output/`: 탐색 결과와 분석 보고서 전용이다.

진단 순서는 [CLI](smoking-data/CLI.md), [metadata map](smoking-data/METADATA_MAP.md), 해당 runbook 순서로 따른다.
