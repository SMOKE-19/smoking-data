# 실패 원인 분석 절차

1. `smoking-data inspect failure <metadata-or-directory> --project-root . --json`을 실행한다.
2. 가장 구체적인 `error_code`와 같은 객체의 `error_context`를 확인한다.
3. 실패 Asset의 dataset-local metadata와 직전 upstream manifest generation을 연결한다.
4. Chain 실행이면 최초 실패 Asset과 `blocked` downstream을 구분한다.
5. transaction 실패이면 기존 generation이 유지됐는지 manifest와 change receipt로 확인한다.
6. 로그는 구조화된 metadata로 설명되지 않는 stack trace와 운영 환경 정보에만 사용한다.

원인을 재현하려고 `run`을 호출하지 않는다. 재실행이 필요하면 예상 output 교체 범위와 비용을 먼저
사용자에게 알리고 승인을 받는다.
