# LLM 탐색 Sandbox

LLM이 CLI만으로 답할 수 없어 Python 탐색 코드를 만들 때 다음 경계를 지킨다.

```text
for_agents/
  scripts/   일회성 Python 탐색 코드
  output/    JSON·CSV·Markdown·profile 결과
```

- 작업공간 루트, `src/`, `scripts/`, `templates/`, `DATA/`, `.temp/`에 탐색 코드를 만들지 않는다.
- script는 입력 파일을 읽기 모드로만 열고 모든 출력 경로를 `for_agents/output/` 아래로 제한한다.
- Parquet은 가능하면 footer 또는 필요한 column만 읽고 전체 materialize를 피한다.
- script 파일명은 `YYYYMMDD_<purpose>.py`, 결과는 `YYYYMMDD_<purpose>.<ext>` 형식을 권장한다.
- script 상단에 질문, 입력 경로, 읽은 column, 생성 output을 주석으로 기록한다.
- 절대 홈 디렉터리 경로를 코드에 고정하지 말고 script 위치에서 작업공간 루트를 계산한다.
- 외부 패키지 설치, 네트워크 전송, 원본 수정, dataset commit은 별도 사용자 승인 없이는 하지 않는다.
- 분석 종료 후 파일을 임의 삭제하지 않는다. 사용자가 검토한 뒤 정리한다.

권장 시작 코드:

```python
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_ROOT = PROJECT_ROOT / "for_agents" / "output"
```

`for_agents/.gitignore`는 sandbox 전체를 Git에서 제외한다.
