# LLM 진단 보고 형식

```markdown
## 결론

한두 문장으로 관측된 원인과 영향 범위를 설명한다.

## 근거

- 파일과 JSON path
- error_code 또는 missing state
- definition hash와 input/output generation
- 관련 counter·profile metric

## 시간 범위

- 정상 확인 시점
- 최초 이상 시점
- 현재도 지속되는지 여부

## 불확실성

확인하지 못한 원본, 부분 footer scan, 재현하지 않은 조건을 기록한다.

## 다음 조치

읽기 전용 추가 확인과 실행·재처리가 필요한 조치를 분리한다.
```

분석 산출물은 `for_agents/output/`에 저장하고 제품 metadata 경로에는 쓰지 않는다.
