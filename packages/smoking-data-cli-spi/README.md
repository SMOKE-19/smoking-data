# smoking-data-cli-spi

SPI 환경에서 사용할 작업공간 초기화 전용 CLI다.

```bash
smoking-data-cli-spi init WORKSPACE
```

`init`은 일반 `smoking-data init`을 먼저 실행한 뒤 이 패키지의 `_workspace/` 리소스를
작업공간의 대응 경로에 overlay한다. `vscode/`는 `.vscode/`, `smoking_data/`는
`.smoking-data/`, `agent/`는 `.agent/`로 매핑된다. SPI 환경의 자동완성 JSON, YAML template, 안내 문서는
이 패키지의 `_workspace/vscode/`, `_workspace/templates/`, `_workspace/agent/`에서 별도로
관리할 수 있다.

데이터 실행과 검증은 일반 CLI를 사용한다.

```bash
smoking-data validate templates/job.0201.yaml --json
smoking-data run templates/job.0201.yaml --json
smoking-data inspect dataset DATA/0201 --json
```

SPI 전용 override 파일은 일반 init 생성물 전체를 대체하지 않는다. 파일이 없으면 일반
`smoking-data` 리소스가 사용된다. 기존 작업공간 파일은 일반 init의 보존 정책을 따르며,
`--force`를 지정한 경우에만 일반 init 관리 파일을 갱신한다. SPI 패키지에 포함된
override 파일은 SPI init의 package-owned 파일이므로 매번 적용된다.
