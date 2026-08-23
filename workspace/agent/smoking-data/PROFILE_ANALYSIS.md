# 프로파일 분석 절차

1. 비교 실행의 Asset code, job, definition SHA-256, graph/logical-plan hash를 확인한다.
2. input generation과 source-file fingerprint가 같은지 확인한다.
3. `inspect profile`로 total·phase elapsed, peak RSS, CPU와 I/O metric을 수집한다.
4. worker 수, task 수, row 수와 입력 byte가 다르면 절대 시간만 비교하지 않는다.
5. warm cache, unchanged 재사용, test run 여부를 분리한다.
6. 결론은 병목 phase, 처리량 변화, peak memory 변화와 비교 불가 조건으로 나눈다.

일회성 집계가 필요하면 [`SANDBOX.md`](SANDBOX.md)에 따라 Python 코드를 작성한다. 제품의 `scripts/`
폴더에 실험 파일을 추가하지 않는다. 재현 명령과 입력 metadata 목록은 결과 보고서에 남긴다.
