# Reference Definition 예시

이 디렉터리는 제품 동작을 설명하고 로컬에서 검증할 수 있는 reference Definition 원본이다.
`smoking-data init`이 사용자 작업공간의 `templates/`에 복사한다. 기본 init은 기존 파일을
보존하며, `--force` 갱신 시 기존 init 관리 생성물 전체를 `.history/YYMMDD_HHMMSS/`로
압축 없이 백업한다.
운영 데이터나 제품 내부 테스트 fixture를 저장하는 위치가 아니다.

## 예시 분류

- 루트 `*.yaml`: Asset Definition과 Asset Chain의 공개 계약 예시
- `0101.*`, `0102.*`, `0103.*`: source·계산·CSV/HTTP 입력 adapter 조합 예시
- `0201.*`, `0301.*`, `0401.*`: curated·join·snapshot 처리 예시
- `chain.*`: 여러 Asset을 연결하는 end-to-end reference
- `migrations/`: 기존 dataset layout 마이그레이션 계약 예시
- `resources/`: 위 Definition이 참조하는 작고 재현 가능한 입력 데이터

GDELT 예시는 특정 업무 도메인 구현이 아니라 HTTP·bulk download·headerless TSV·고카디널리티
입력의 reference adapter 사용법을 보여주는 통합 예시다. 범용 adapter 구현은 제품 코드에 있고,
예시는 adapter를 조합하는 사용자 계약만 담는다.

## 실행 기준

예시는 작업공간 루트에서 실행한다.

```bash
smoking-data validate templates/0201.pipeline_curated_pivot.0201.yaml --json
smoking-data validate templates/chain.0101_to_0401_asset_chain.chain.yaml --json
```

Schedule 예시는 이 디렉터리에 두지 않고 작업공간의 `schedules/`에 별도로 생성된다.
