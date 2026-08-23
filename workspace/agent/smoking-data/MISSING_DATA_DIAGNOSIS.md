# 데이터 미싱 원인 분석 절차

1. 기대한 row·column·계산값과 최초로 보이지 않는 Asset 단계를 정의한다.
2. 각 단계에 `inspect dataset`과 `inspect missing`을 실행해 generation을 연결한다.
3. 다음 원인을 서로 구분한다.

   - 0101 upstream 조회 결과에 원본 행이 없음
   - 0103 CSV 삭제·미처리 또는 route의 unmatched drop
   - 0201 active-row selection에서 비활성 행 제외
   - 0102 schema 변경으로 계산이 `blocked_missing_dependency` 또는 `skipped_missing_dependency`
   - 0301 join key·dtype·null 조건 불일치
   - `define_upstream.select.labels`가 다른 route/sub-job을 선택
   - 이전 generation과 현재 generation을 혼합해 비교

4. 0102는 `calculation-status.json`의 missing dependency와 최초 blocked 시점을 우선 근거로 사용한다.
5. 0103은 source-file manifest의 relative path·SHA-256과 dataset catalog의 route label을 연결한다.
6. 실제 payload row 검색이 필요할 때만 sandbox Python script를 만들고, 원본은 lazy scan/read만 한다.

“데이터가 없다”와 “계산 대상에서 제외됐다”를 같은 원인으로 보고하지 않는다.
