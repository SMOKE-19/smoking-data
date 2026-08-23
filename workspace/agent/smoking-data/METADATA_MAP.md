# Metadata Map

| 경로 | 확인 내용 |
| --- | --- |
| `<dataset>/_dataset.manifest.json` | generation, parent generation, part·row 수, 논리 계획 context, provenance |
| `<dataset>/_smoking_data/metadata.json` | Asset·job·definition, counter, warning, 실행 phase와 실패 정보 |
| `<dataset>/_smoking_data/change-receipt.json` | 이전 generation 대비 추가·변경·삭제 part |
| `<0103-root>/_smoking_data/source-file-manifest.json` | CSV 상대경로, size·mtime·SHA-256, 처리 상태와 출력 part |
| `<0103-root>/_smoking_data/dataset-catalog.json` | route·source_file label과 실제 child dataset 경로 |
| `<0102-root>/_smoking_data/calculation-status.json` | 계산별 active·blocked 상태와 missing dependency 기간 |
| `.temp/logs/` | 사람이 읽는 실행 로그. 구조화 metadata 확인 후 보조 근거로 사용 |
| `.temp/metadata/asset_chain/` | Chain orchestration 결과와 downstream 차단 원인 |

`_smoking_data`와 manifest는 게시 dataset의 일부다. 진단 중 수정·정리·재포맷하지 않는다.
경로가 없다는 사실도 근거이므로 새 빈 파일을 만들지 않는다.
